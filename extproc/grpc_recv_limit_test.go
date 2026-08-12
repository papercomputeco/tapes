package extproc

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"time"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// dialExtprocServer serves a real Processor through the production wiring —
// grpc.NewServer(GRPCServerOptions(cfg)...) + RegisterServer — over bufconn.
func dialExtprocServer(cfg Config) extprocv3.ExternalProcessorClient {
	GinkgoHelper()

	proc, err := NewProcessor(cfg)
	Expect(err).NotTo(HaveOccurred())

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(GRPCServerOptions(cfg)...)
	RegisterServer(srv, proc)
	go func() {
		defer GinkgoRecover()
		_ = srv.Serve(lis)
	}()
	DeferCleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(conn.Close)
	return extprocv3.NewExternalProcessorClient(conn)
}

var _ = Describe("gRPC receive limit", func() {
	It("accepts a single >4 MiB ProcessingRequest at the default recv limit and rejects it when configured to 1 MiB", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		DeferCleanup(cancel)

		headers := headerReq(map[string]string{
			":method":      http.MethodPost,
			":path":        "/v1/messages",
			"x-request-id": "recv-limit-test",
		})
		// 8 MiB clears grpc-go's own 4 MiB default only if the configured
		// option actually reached the server.
		big := reqBodyReq(bytes.Repeat([]byte("a"), 8<<20), true)

		client := dialExtprocServer(Config{GRPCMaxRecvBytes: defaultGRPCMaxRecvBytes, MaxInflight: 1})
		stream, err := client.Process(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(stream.Send(headers)).To(Succeed())
		resp, err := stream.Recv()
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.GetRequestHeaders()).NotTo(BeNil())
		Expect(stream.Send(big)).To(Succeed())
		resp, err = stream.Recv()
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.GetRequestBody()).NotTo(BeNil())
		Expect(stream.CloseSend()).To(Succeed())

		client = dialExtprocServer(Config{GRPCMaxRecvBytes: 1 << 20, MaxInflight: 1})
		stream, err = client.Process(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(stream.Send(headers)).To(Succeed())
		resp, err = stream.Recv()
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.GetRequestHeaders()).NotTo(BeNil())
		// Send may hit io.EOF if the server tears down mid-write; the RPC
		// status is authoritative on Recv either way.
		if err := stream.Send(big); err != nil {
			Expect(err).To(MatchError(io.EOF))
		}
		_, err = stream.Recv()
		Expect(status.Code(err)).To(Equal(codes.ResourceExhausted))
	})
})
