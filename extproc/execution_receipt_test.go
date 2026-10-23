package extproc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"strings"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/google/jsonschema-go/jsonschema"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/structpb"
)

var _ = Describe("ExecutionReceiptV1", func() {
	selectionHeader := func() string {
		raw, err := json.Marshal(SelectionMetadataV1{Version: 1, ConfigHash: strings.Repeat("a", 64), Decision: "dragon", DestinationID: "dst-a", Model: "predicted-model", Backend: "predicted-backend", Protocol: "openai.chat.v1"})
		Expect(err).NotTo(HaveOccurred())
		return base64.RawURLEncoding.EncodeToString(raw)
	}
	DescribeTable("observes captured bytes and actual backend without waiting for ingest", func(metadataMessage int) {
		proc, err := NewProcessor(Config{FinalDispatchReceipts: true, IngestURL: "http://127.0.0.1:1", MaxInflight: 1})
		Expect(err).NotTo(HaveOccurred())
		attempts := make([]string, 0, 2)
		for range 2 {
			h := respHeaderReq("200", "text/event-stream")
			forged := headerReq(map[string]string{ExecutionReceiptHeader: "forged", SelectionMetadataHeader: "forged"})
			h.GetResponseHeaders().Headers.Headers = append(h.GetResponseHeaders().Headers.Headers, forged.GetRequestHeaders().Headers.Headers...)
			stream := &fakeStream{ctx: context.Background(), toSend: []*extprocv3.ProcessingRequest{
				headerReq(map[string]string{SelectionMetadataHeader: selectionHeader(), "x-paper-gateway-id": "gateway-a", "x-ai-eg-selected-backend": "forged-header-backend", ":path": "/v1/chat/completions", ":method": "POST", "x-request-id": "request-a"}),
				reqBodyReq([]byte(`{"model":"final-model","temperature":0.25,"max_completion_tokens":100,"stream":true,"messages":[{"content":"PRIVATE_PROMPT"}],"reasoning":{"effort":"high","encrypted_content":"PRIVATE_REASONING"},"tools":[{"secret":"PRIVATE_TOOL"}]}`), true), h,
			}}
			stream.toSend[metadataMessage].MetadataContext = &corev3.Metadata{FilterMetadata: map[string]*structpb.Struct{"io.envoy.ai_gateway": {Fields: map[string]*structpb.Value{"backend_name": structpb.NewStringValue("tenant/actual-host-b/route/route-a/rule/0/ref/0")}}}}
			Expect(proc.Process(stream)).To(Succeed())
			responses := stream.Responses()
			Expect(responses[0].GetRequestHeaders().GetResponse().GetHeaderMutation().RemoveHeaders).To(ContainElements(SelectionMetadataHeader, "x-paper-gateway-id"))
			mutation := responses[2].GetResponseHeaders().GetResponse().GetHeaderMutation()
			Expect(mutation.RemoveHeaders).To(ContainElements(ExecutionReceiptHeader, SelectionMetadataHeader))
			Expect(mutation.SetHeaders).To(HaveLen(1))
			raw, err := base64.RawURLEncoding.DecodeString(string(mutation.SetHeaders[0].Header.RawValue))
			Expect(err).NotTo(HaveOccurred())
			Expect(string(raw)).NotTo(ContainSubstring("PRIVATE_"))
			var receipt ExecutionReceiptV1
			Expect(json.Unmarshal(raw, &receipt)).To(Succeed())
			Expect(receipt.Model).To(Equal("final-model"))
			Expect(receipt.BackendID).To(Equal("actual-host-b"))
			Expect(receipt.Selection.Backend).To(Equal("predicted-backend"))
			Expect(receipt.Outcome).To(Equal("dispatched"))
			Expect(receipt.TerminalOwner).To(Equal("downstream-stream-adapter"))
			Expect(receipt.Settings["reasoning.effort"].Value).To(Equal("high"))
			Expect(receipt.Settings["top_p"].State).To(Equal("omitted-provider-default"))
			attempts = append(attempts, receipt.AttemptID)
		}
		Expect(attempts[0]).NotTo(Equal(attempts[1]))
	}, Entry("backend available with request body", 1), Entry("backend first available at response headers", 2))
	DescribeTable("leaves unsupported dispatch facts unavailable at response headers", func(body, encoding string, eos, backendAvailable bool) {
		proc, err := NewProcessor(Config{FinalDispatchReceipts: true, IngestURL: "http://127.0.0.1:1", MaxInflight: 1})
		Expect(err).NotTo(HaveOccurred())
		response := respHeaderReq("200", "text/event-stream")
		if backendAvailable {
			response.MetadataContext = &corev3.Metadata{FilterMetadata: map[string]*structpb.Struct{"io.envoy.ai_gateway": {Fields: map[string]*structpb.Value{"backend_name": structpb.NewStringValue("tenant/actual-host-b/route/route-a/rule/0/ref/0")}}}}
		}
		stream := &fakeStream{ctx: context.Background(), toSend: []*extprocv3.ProcessingRequest{
			headerReq(map[string]string{SelectionMetadataHeader: selectionHeader(), "x-paper-gateway-id": "gateway-a", "x-ai-eg-selected-backend": "forged-header-backend", ":method": "POST", ":path": "/v1/chat/completions", "content-encoding": encoding}),
			reqBodyReq([]byte(body), eos), response,
		}}
		Expect(proc.Process(stream)).To(Succeed())
		mutation := stream.Responses()[2].GetResponseHeaders().GetResponse().GetHeaderMutation()
		Expect(mutation.GetSetHeaders()).To(HaveLen(1))
		raw, err := base64.RawURLEncoding.DecodeString(string(mutation.GetSetHeaders()[0].Header.RawValue))
		Expect(err).NotTo(HaveOccurred())
		var receipt ExecutionReceiptV1
		Expect(json.Unmarshal(raw, &receipt)).To(Succeed())
		Expect(receipt.Outcome).To(Equal("selected"))
		Expect(receipt.Model).To(BeEmpty())
		Expect(receipt.Settings).To(BeEmpty())
		if backendAvailable {
			Expect(receipt.BackendID).To(Equal("actual-host-b"))
		} else {
			Expect(receipt.BackendID).To(BeEmpty())
		}
	}, Entry("missing backend", `{"model":"captured-model"}`, "", true, false),
		Entry("incomplete request", `{"model":"captured-model"}`, "", false, true),
		Entry("malformed JSON", `{"model":`, "", true, true),
		Entry("undecodable request", `not gzip`, "gzip", true, true))
	DescribeTable("suppresses a receipt when response metadata conflicts with the request", func(native bool) {
		proc, err := NewProcessor(Config{FinalDispatchReceipts: true, IngestURL: "http://127.0.0.1:1", MaxInflight: 1})
		Expect(err).NotTo(HaveOccurred())
		request := headerReq(map[string]string{SelectionMetadataHeader: selectionHeader(), "x-paper-gateway-id": "gateway-a", ":method": "POST", ":path": "/v1/chat/completions"})
		request.MetadataContext = &corev3.Metadata{FilterMetadata: map[string]*structpb.Struct{"io.envoy.ai_gateway": {Fields: map[string]*structpb.Value{"backend_name": structpb.NewStringValue("tenant/backend-a")}}}}
		if native {
			request.MetadataContext = &corev3.Metadata{FilterMetadata: map[string]*structpb.Struct{"io.paper.native_dispatch": {Fields: map[string]*structpb.Value{
				"backend_name": structpb.NewStringValue("backend-a"), "destination_id": structpb.NewStringValue("dst-a"), "service": structpb.NewStringValue("chatgpt"),
			}}}}
		}
		response := respHeaderReq("200", "text/event-stream")
		response.MetadataContext = &corev3.Metadata{FilterMetadata: map[string]*structpb.Struct{"io.envoy.ai_gateway": {Fields: map[string]*structpb.Value{"backend_name": structpb.NewStringValue("tenant/backend-b")}}}}
		stream := &fakeStream{ctx: context.Background(), toSend: []*extprocv3.ProcessingRequest{request, reqBodyReq([]byte(`{"model":"captured-model"}`), true), response}}
		Expect(proc.Process(stream)).To(Succeed())
		Expect(stream.Responses()[2].GetResponseHeaders().GetResponse().GetHeaderMutation().GetSetHeaders()).To(BeEmpty())
	}, Entry("managed backend changes", false), Entry("native authority becomes managed", true))
	DescribeTable("binds native final-hop metadata to the selected destination", func(service, backend, destination string, conflict, expected bool) {
		proc, err := NewProcessor(Config{FinalDispatchReceipts: true, IngestURL: "http://127.0.0.1:1", MaxInflight: 1})
		Expect(err).NotTo(HaveOccurred())
		namespaces := map[string]*structpb.Struct{"io.paper.native_dispatch": {Fields: map[string]*structpb.Value{
			"backend_name": structpb.NewStringValue(backend), "destination_id": structpb.NewStringValue(destination), "service": structpb.NewStringValue(service),
		}}}
		if conflict {
			namespaces["io.envoy.ai_gateway"] = &structpb.Struct{Fields: map[string]*structpb.Value{"backend_name": structpb.NewStringValue("tenant/other")}}
		}
		request := headerReq(map[string]string{SelectionMetadataHeader: selectionHeader(), "x-paper-gateway-id": "gateway-a", ":method": "POST", ":path": "/backend-api/codex/responses", "x-ai-eg-selected-backend": "caller-forged"})
		request.MetadataContext = &corev3.Metadata{FilterMetadata: namespaces}
		stream := &fakeStream{ctx: context.Background(), toSend: []*extprocv3.ProcessingRequest{
			request,
			reqBodyReq([]byte(`{"model":"native-final","previous_response_id":"opaque-provider-id","store":true,"input":[{"encrypted_content":"PRIVATE_OPAQUE"}],"reasoning":{"effort":"high"}}`), true), respHeaderReq("200", "text/event-stream"),
		}}
		Expect(proc.Process(stream)).To(Succeed())
		mutation := stream.Responses()[2].GetResponseHeaders().GetResponse().GetHeaderMutation()
		if !expected {
			Expect(mutation.GetSetHeaders()).To(BeEmpty())
			return
		}
		Expect(mutation.GetSetHeaders()).To(HaveLen(1))
		raw, err := base64.RawURLEncoding.DecodeString(string(mutation.GetSetHeaders()[0].Header.RawValue))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(raw)).NotTo(ContainSubstring("PRIVATE_OPAQUE"))
		Expect(string(raw)).NotTo(ContainSubstring("opaque-provider-id"))
		var receipt ExecutionReceiptV1
		Expect(json.Unmarshal(raw, &receipt)).To(Succeed())
		Expect(receipt.BackendID).To(Equal(backend))
		Expect(receipt.Model).To(Equal("native-final"))
		Expect(receipt.Outcome).To(Equal("dispatched"))
	}, Entry("ChatGPT", "chatgpt", "actual-native", "dst-a", false, true),
		Entry("Claude", "anthropic", "actual-native", "dst-a", false, true),
		Entry("wrong destination", "chatgpt", "actual-native", "dst-b", false, false),
		Entry("missing backend", "chatgpt", "", "dst-a", false, false),
		Entry("managed composite is not native identity", "chatgpt", "tenant/backend", "dst-a", false, false),
		Entry("wrong service", "openai", "actual-native", "dst-a", false, false),
		Entry("conflicting sources", "chatgpt", "actual-native", "dst-a", true, false))
	It("invalidates a native receipt if a later message changes metadata authority", func() {
		st := &streamState{receipt: &ExecutionReceiptV1{Selection: SelectionMetadataV1{DestinationID: "dst-a"}}}
		native := &corev3.Metadata{FilterMetadata: map[string]*structpb.Struct{"io.paper.native_dispatch": {Fields: map[string]*structpb.Value{
			"backend_name": structpb.NewStringValue("actual-native"), "destination_id": structpb.NewStringValue("dst-a"), "service": structpb.NewStringValue("chatgpt"),
		}}}}
		st.observeDispatchMetadata(native)
		Expect(st.receipt.BackendID).To(Equal("actual-native"))
		st.observeDispatchMetadata(&corev3.Metadata{FilterMetadata: map[string]*structpb.Struct{"io.envoy.ai_gateway": {Fields: map[string]*structpb.Value{"backend_name": structpb.NewStringValue("tenant/other")}}}})
		Expect(st.receipt).To(BeNil())
		st.observeDispatchMetadata(native)
		Expect(st.receipt).To(BeNil())
	})
	It("reports a safe failure without reflecting provider errors", func() {
		proc := &Processor{finalDispatchReceipts: true}
		st := &streamState{statusCode: 429, receipt: &ExecutionReceiptV1{Version: 1, Outcome: "dispatched", Settings: map[string]ReceiptSetting{}}}
		response := proc.executionReceiptResponse(st, respHeaderReq("429", "application/json").GetResponseHeaders())
		raw, err := base64.RawURLEncoding.DecodeString(string(response.GetResponse().GetHeaderMutation().SetHeaders[0].Header.RawValue))
		Expect(err).NotTo(HaveOccurred())
		var receipt ExecutionReceiptV1
		Expect(json.Unmarshal(raw, &receipt)).To(Succeed())
		Expect(receipt.Outcome).To(Equal("failed"))
		Expect(receipt.FailureStage).To(Equal("upstream-response"))
		Expect(st.receipt.Outcome).To(Equal("dispatched"))
	})
	It("does not claim settings when the final body or backend is unavailable", func() {
		st := &streamState{receipt: &ExecutionReceiptV1{Outcome: "selected"}, decodedReq: []byte(`{"model":"real"}`)}
		st.observeFinalDispatch()
		Expect(st.receipt.Outcome).To(Equal("selected"))
		Expect(st.receipt.Model).To(BeEmpty())
	})
	DescribeTable("does not promote a rejected capture when backend evidence arrives later", func(complete, overBudget, decodeFailed bool) {
		st := &streamState{reqEOS: complete, reqOverBudget: overBudget, receipt: &ExecutionReceiptV1{Outcome: "selected"}, decodedReq: []byte(`{"model":"captured-model"}`)}
		if decodeFailed {
			st.requestDecodeErr = errors.New("invalid encoding")
		}
		st.observeDispatchMetadata(&corev3.Metadata{FilterMetadata: map[string]*structpb.Struct{"io.envoy.ai_gateway": {Fields: map[string]*structpb.Value{"backend_name": structpb.NewStringValue("tenant/backend-a")}}}})
		st.observeFinalDispatch()
		Expect(st.receipt.BackendID).To(Equal("backend-a"))
		Expect(st.receipt.Outcome).To(Equal("selected"))
		Expect(st.receipt.Model).To(BeEmpty())
		Expect(st.receipt.Settings).To(BeEmpty())
	}, Entry("incomplete", false, false, false), Entry("over budget", true, true, false), Entry("decode failed", true, false, true))
	It("redacts invalid scalars, bounds headers, and rejects malformed selection", func() {
		model, settings := forwardedSettings([]byte(`{"model":"real","temperature":{"secret":"PRIVATE"},"reasoning_effort":"PRIVATE","thinking":{"type":"enabled","budget_tokens":3}}`))
		Expect(model).To(Equal("real"))
		Expect(settings["temperature"].State).To(Equal("unknown"))
		Expect(settings["reasoning_effort"].Value).To(BeNil())
		_, err := EncodeExecutionReceipt(&ExecutionReceiptV1{RequestID: strings.Repeat("x", MaxExecutionReceiptBytes)})
		Expect(err).To(HaveOccurred())
		Expect(parseSelection(strings.Repeat("x", 6000))).To(BeNil())
		Expect(parseSelection(base64.RawURLEncoding.EncodeToString([]byte(`{"version":1,"configHash":"spoof"}`)))).To(BeNil())
	})
	DescribeTable("distinguishes omitted nested settings from unreadable parents", func(body, state string) {
		model, settings := forwardedSettings([]byte(body))
		Expect(model).To(Equal("real"))
		for _, key := range []string{"reasoning.effort", "thinking.type", "thinking.budget_tokens"} {
			Expect(settings[key].State).To(Equal(state), key)
			Expect(settings[key].Value).To(BeNil(), key)
			Expect(settings[key].Provenance).To(Equal("final-forwarded-body"), key)
		}
	},
		Entry("absent parents", `{"model":"real"}`, "omitted-provider-default"),
		Entry("empty objects", `{"model":"real","reasoning":{},"thinking":{}}`, "omitted-provider-default"),
		Entry("strings", `{"model":"real","reasoning":"PRIVATE","thinking":"enabled"}`, "unknown"),
		Entry("arrays", `{"model":"real","reasoning":[],"thinking":[]}`, "unknown"),
		Entry("numbers", `{"model":"real","reasoning":1,"thinking":2}`, "unknown"),
		Entry("booleans", `{"model":"real","reasoning":true,"thinking":false}`, "unknown"),
		Entry("null parents", `{"model":"real","reasoning":null,"thinking":null}`, "unknown"),
	)
	DescribeTable("keeps independent settings when one nested parent is unreadable", func(body, unknownKey, forwardedKey string, value any) {
		_, settings := forwardedSettings([]byte(body))
		Expect(settings[unknownKey].State).To(Equal("unknown"))
		Expect(settings[unknownKey].Value).To(BeNil())
		Expect(settings[forwardedKey].State).To(Equal("forwarded"))
		Expect(settings[forwardedKey].Value).To(Equal(value))
		Expect(settings["temperature"].State).To(Equal("omitted-provider-default"))
	},
		Entry("invalid reasoning with valid thinking", `{"model":"real","reasoning":"PRIVATE","thinking":{"type":"enabled","budget_tokens":3}}`, "reasoning.effort", "thinking.budget_tokens", float64(3)),
		Entry("invalid thinking with valid reasoning", `{"model":"real","reasoning":{"effort":"high"},"thinking":"PRIVATE"}`, "thinking.type", "reasoning.effort", "high"),
	)
	It("keeps the owner wire example parseable and within the header budget", func() {
		raw, err := os.ReadFile("testdata/execution-receipt-v1.json")
		Expect(err).NotTo(HaveOccurred())
		schema, err := jsonschema.For[ExecutionReceiptV1](nil)
		Expect(err).NotTo(HaveOccurred())
		schemaRaw, err := json.Marshal(schema)
		Expect(err).NotTo(HaveOccurred())
		schemaGolden, err := os.ReadFile("testdata/execution-receipt-v1.schema.json")
		Expect(err).NotTo(HaveOccurred())
		Expect(string(schemaRaw)).To(MatchJSON(string(schemaGolden)))
		resolved, err := schema.Resolve(nil)
		Expect(err).NotTo(HaveOccurred())
		var instance any
		Expect(json.Unmarshal(raw, &instance)).To(Succeed())
		Expect(resolved.Validate(instance)).To(Succeed())
		var receipt ExecutionReceiptV1
		Expect(json.Unmarshal(raw, &receipt)).To(Succeed())
		wire, err := EncodeExecutionReceipt(&receipt)
		Expect(err).NotTo(HaveOccurred())
		Expect(len(wire)).To(BeNumerically("<=", MaxExecutionReceiptBytes))
		Expect(receipt.Version).To(Equal(1))
		Expect(receipt.Outcome).To(Equal("dispatched"))
		Expect(parseSelection(selectionHeader())).NotTo(BeNil())
	})
})
