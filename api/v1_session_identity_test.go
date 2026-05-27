package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/gofiber/fiber/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// identityDriver wraps a real storage.Driver and adds a configurable
// SessionIdentityByHash so it satisfies the unexported sessionIdentityLookup
// interface. It lets the API tests exercise identity surfacing without a DB.
type identityDriver struct {
	storage.Driver

	identity  *storage.SessionIdentity
	err       error
	calls     int
	lastOrgID string
}

func (d *identityDriver) SessionIdentityByHash(_ context.Context, orgID, _ string) (*storage.SessionIdentity, error) {
	d.calls++
	d.lastOrgID = orgID
	return d.identity, d.err
}

func decodeSessionResp(server *Server, path string) (SessionResponse, int) {
	return decodeSessionRespWithOrg(server, path, "")
}

// decodeSessionRespWithOrg issues the request with an optional org header so
// tests can assert the tenant is threaded from the request down to the
// identity lookup. An empty org sends no header (exercising the default).
func decodeSessionRespWithOrg(server *Server, path, org string) (SessionResponse, int) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	Expect(err).NotTo(HaveOccurred())
	if org != "" {
		req.Header.Set(orgIDHeader, org)
	}
	resp, err := server.app.Test(req)
	Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
	var body SessionResponse
	if resp.StatusCode == fiber.StatusOK {
		Expect(json.Unmarshal(raw, &body)).To(Succeed())
	}
	return body, resp.StatusCode
}

var _ = Describe("session identity surfacing on GET /v1/sessions/:hash", func() {
	var (
		ctx  context.Context
		base storage.Driver
		leaf *merkle.Node
	)

	BeforeEach(func() {
		ctx = context.Background()
		base = inmemory.NewDriver()

		root := merkle.NewNode(v1TestBucket("user", "q", "m", "p", "claude"), nil)
		leaf = merkle.NewNode(v1TestBucket("assistant", "a", "m", "p", "claude"), root)
		Expect(putNode(ctx, base, root)).To(Succeed())
		Expect(putNode(ctx, base, leaf)).To(Succeed())
	})

	newServer := func(driver storage.Driver) *Server {
		server, err := NewServer(Config{ListenAddr: ":0"}, driver, tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())
		return server
	}

	It("carries harness_id/harness_session_id when the driver supplies an identity", func() {
		drv := &identityDriver{
			Driver:   base,
			identity: &storage.SessionIdentity{HarnessID: "claude", HarnessSessionID: "sess-xyz"},
		}
		server := newServer(drv)

		body, status := decodeSessionResp(server, "/v1/sessions/"+leaf.Hash)
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(body.HarnessID).To(Equal("claude"))
		Expect(body.HarnessSessionID).To(Equal("sess-xyz"))
		Expect(drv.calls).To(Equal(1), "the handler should consult the lookup")
		// With no org header the middleware defaults to the nil-org sentinel
		// and threads it all the way to the lookup.
		Expect(drv.lastOrgID).To(Equal(nilOrgID))
	})

	It("threads the client-asserted org header through to the identity lookup", func() {
		drv := &identityDriver{
			Driver:   base,
			identity: &storage.SessionIdentity{HarnessID: "claude", HarnessSessionID: "sess-xyz"},
		}
		server := newServer(drv)

		org := "11111111-1111-1111-1111-111111111111"
		_, status := decodeSessionRespWithOrg(server, "/v1/sessions/"+leaf.Hash, org)
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(drv.lastOrgID).To(Equal(org), "the lookup must be scoped to the requested tenant")
	})

	It("omits the fields when the driver returns (nil, nil) but still succeeds", func() {
		drv := &identityDriver{Driver: base, identity: nil, err: nil}
		server := newServer(drv)

		body, status := decodeSessionResp(server, "/v1/sessions/"+leaf.Hash)
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(body.HarnessID).To(BeEmpty())
		Expect(body.HarnessSessionID).To(BeEmpty())
		// The session payload itself must still be present.
		Expect(body.Hash).To(Equal(leaf.Hash))
		Expect(body.Turns).To(HaveLen(2))
	})

	It("omits the fields and succeeds when the driver does not implement the lookup interface", func() {
		// The inmemory driver has no SessionIdentityByHash method, so the
		// type assertion in (*Server).sessionIdentity fails and the handler
		// must fall through to an empty identity.
		_, hasLookup := base.(sessionIdentityLookup)
		Expect(hasLookup).To(BeFalse(), "precondition: base driver must not implement the lookup")

		server := newServer(base)

		body, status := decodeSessionResp(server, "/v1/sessions/"+leaf.Hash)
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(body.HarnessID).To(BeEmpty())
		Expect(body.HarnessSessionID).To(BeEmpty())
		Expect(body.Hash).To(Equal(leaf.Hash))
		Expect(body.Turns).To(HaveLen(2))
	})
})
