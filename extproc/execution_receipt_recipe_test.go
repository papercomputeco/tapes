package extproc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/structpb"
)

func recipeSelectionFixture() map[string]any {
	raw, err := os.ReadFile("testdata/selection-recipe-v1.json")
	Expect(err).NotTo(HaveOccurred())
	var selection map[string]any
	Expect(json.Unmarshal(raw, &selection)).To(Succeed())
	return selection
}

func encodeSelectionFixture(selection map[string]any) string {
	raw, err := json.Marshal(selection)
	Expect(err).NotTo(HaveOccurred())
	return base64.RawURLEncoding.EncodeToString(raw)
}

func captureRecipeFixture(selection map[string]any, backend string) (*ExecutionReceiptV1, string) {
	proc, err := NewProcessor(Config{FinalDispatchReceipts: true, IngestURL: "http://127.0.0.1:1", MaxInflight: 1})
	Expect(err).NotTo(HaveOccurred())
	response := respHeaderReq("200", "text/event-stream")
	forged := headerReq(map[string]string{ExecutionReceiptHeader: "provider-forged", SelectionMetadataHeader: "provider-forged"})
	response.GetResponseHeaders().Headers.Headers = append(response.GetResponseHeaders().Headers.Headers, forged.GetRequestHeaders().Headers.Headers...)
	if backend != "" {
		response.MetadataContext = &corev3.Metadata{FilterMetadata: map[string]*structpb.Struct{"io.envoy.ai_gateway": {Fields: map[string]*structpb.Value{"backend_name": structpb.NewStringValue("tenant/" + backend + "/route/recipe")}}}}
	}
	body, err := json.Marshal(map[string]any{
		"model": "final-model", "temperature": .25, "max_completion_tokens": 100, "stream": true,
		"messages":  []map[string]string{{"role": "user", "content": "PRIVATE_PROMPT"}},
		"reasoning": map[string]string{"effort": "high", "encrypted_content": "PRIVATE_REASONING"},
	})
	Expect(err).NotTo(HaveOccurred())
	stream := &fakeStream{ctx: context.Background(), toSend: []*extprocv3.ProcessingRequest{
		headerReq(map[string]string{SelectionMetadataHeader: encodeSelectionFixture(selection), "x-paper-gateway-id": "gateway-a", "x-request-id": "request-recipe", ":method": "POST", ":path": "/v1/chat/completions"}),
		reqBodyReq(body, true), response,
	}}
	Expect(proc.Process(stream)).To(Succeed())
	mutation := stream.Responses()[2].GetResponseHeaders().GetResponse().GetHeaderMutation()
	Expect(mutation.RemoveHeaders).To(ContainElements(ExecutionReceiptHeader, SelectionMetadataHeader))
	if len(mutation.SetHeaders) == 0 {
		return nil, ""
	}
	Expect(mutation.SetHeaders).To(HaveLen(1))
	wire := string(mutation.SetHeaders[0].Header.RawValue)
	Expect(len(wire)).To(BeNumerically("<=", MaxExecutionReceiptBytes))
	raw, err := base64.RawURLEncoding.DecodeString(wire)
	Expect(err).NotTo(HaveOccurred())
	Expect(string(raw)).NotTo(ContainSubstring("PRIVATE_"))
	var receipt ExecutionReceiptV1
	Expect(json.Unmarshal(raw, &receipt)).To(Succeed())
	return &receipt, string(raw)
}

var _ = Describe("Recipe execution receipt context", func() {
	It("keeps the request generation and explicit shape in the producer golden", func() {
		receipt, _ := captureRecipeFixture(recipeSelectionFixture(), "provider-b")
		Expect(receipt).NotTo(BeNil())
		Expect(receipt.Selection.Decision).To(Equal("sensitive"))
		Expect(receipt.Selection.Shape).To(Equal("dragon"))
		Expect(receipt.Model).To(Equal("final-model"))
		Expect(receipt.Selection.Model).To(Equal("model-b"))
		Expect(receipt.BackendID).To(Equal("provider-b"))
		Expect(receipt.Selection.BackendName).To(Equal("private"))
		receipt.ReceiptID, receipt.AttemptID = "receipt-normalized", "attempt-normalized"
		actual, err := json.Marshal(receipt)
		Expect(err).NotTo(HaveOccurred())
		golden, err := os.ReadFile("testdata/execution-receipt-recipe-v1.json")
		Expect(err).NotTo(HaveOccurred())
		Expect(string(actual)).To(MatchJSON(string(golden)))
	})

	It("keeps independent backend generations for the same saved recipe and name", func() {
		oldSelection := recipeSelectionFixture()
		newerSelection := recipeSelectionFixture()
		newerSelection["backendGeneration"] = "bg_new"
		newerSelection["configHash"] = strings.Repeat("b", 64)
		oldReceipt, _ := captureRecipeFixture(oldSelection, "provider-b")
		newReceipt, _ := captureRecipeFixture(newerSelection, "provider-b")
		Expect(oldReceipt.Selection.ConfigurationRevision).To(Equal(newReceipt.Selection.ConfigurationRevision))
		Expect(oldReceipt.Selection.BackendName).To(Equal(newReceipt.Selection.BackendName))
		Expect(oldReceipt.Selection.BackendGeneration).To(Equal("bg_b"))
		Expect(newReceipt.Selection.BackendGeneration).To(Equal("bg_new"))
		Expect(oldReceipt.Selection.ConfigHash).NotTo(Equal(newReceipt.Selection.ConfigHash))
		Expect(oldReceipt.AttemptID).NotTo(Equal(newReceipt.AttemptID))
	})

	It("uses the explicit target when the decision is the no-match default", func() {
		selection := recipeSelectionFixture()
		selection["decision"], selection["shape"] = "", "frog"
		receipt, _ := captureRecipeFixture(selection, "provider-b")
		Expect(receipt).NotTo(BeNil())
		Expect(receipt.Selection.Decision).To(BeEmpty())
		Expect(receipt.Selection.Shape).To(Equal("frog"))
		Expect(receipt.Outcome).To(Equal("dispatched"))
	})

	It("keeps local rejections selected without inventing provider execution", func() {
		raw, err := os.ReadFile("testdata/selection-recipe-rejected-v1.json")
		Expect(err).NotTo(HaveOccurred())
		selected := parseSelection(base64.RawURLEncoding.EncodeToString(raw))
		Expect(selected).NotTo(BeNil())
		Expect(selected.Shape).To(Equal("dragon"))
		Expect(selected.BackendName).To(Equal("private"))
		Expect(selected.Model).To(BeEmpty())
		receipt, _ := captureRecipeFixture(recipeSelectionFixture(), "")
		Expect(receipt).NotTo(BeNil())
		Expect(receipt.Outcome).To(Equal("selected"))
		Expect(receipt.Model).To(BeEmpty())
		Expect(receipt.Settings).To(BeEmpty())
	})

	It("suppresses conflicting final backend identity instead of attaching the wrong generation", func() {
		receipt, _ := captureRecipeFixture(recipeSelectionFixture(), "different-backend")
		Expect(receipt).To(BeNil())
	})

	It("rejects every partial, empty or null recipe identity fragment", func() {
		for _, key := range []string{"shape", "recipe", "configurationId", "configurationRevision", "backendName", "backendGeneration"} {
			for _, invalid := range []any{nil, "", "contains PRIVATE whitespace"} {
				selection := recipeSelectionFixture()
				selection[key] = invalid
				Expect(parseSelection(encodeSelectionFixture(selection))).To(BeNil(), "%s=%v", key, invalid)
			}
			selection := recipeSelectionFixture()
			delete(selection, key)
			Expect(parseSelection(encodeSelectionFixture(selection))).To(BeNil(), "missing %s", key)
		}
	})

	It("rejects ambiguous identities, unrecognized shapes and telemetry payloads", func() {
		for _, mutate := range []func(map[string]any){
			func(s map[string]any) { s["shape"] = "invented" },
			func(s map[string]any) { s["signals"] = []string{"PRIVATE_PROMPT"} },
			func(s map[string]any) { s["ConfigurationID"] = s["configurationId"]; delete(s, "configurationId") },
			func(s map[string]any) { s["decision"] = nil },
			func(s map[string]any) { delete(s, "decision") },
			func(s map[string]any) { s["recipe"] = strings.Repeat("r", 257) },
		} {
			selection := recipeSelectionFixture()
			mutate(selection)
			Expect(parseSelection(encodeSelectionFixture(selection))).To(BeNil())
		}
		wire := encodeSelectionFixture(recipeSelectionFixture())
		raw, err := base64.RawURLEncoding.DecodeString(wire)
		Expect(err).NotTo(HaveOccurred())
		duplicate := append([]byte("{\"shape\":\"rabbit\","), raw[1:]...)
		Expect(parseSelection(base64.RawURLEncoding.EncodeToString(duplicate))).To(BeNil())
	})

	It("does not reinterpret legacy selections with no recipe context", func() {
		raw, err := os.ReadFile("testdata/execution-receipt-v1.json")
		Expect(err).NotTo(HaveOccurred())
		var old ExecutionReceiptV1
		Expect(json.Unmarshal(raw, &old)).To(Succeed())
		selectionRaw, err := json.Marshal(old.Selection)
		Expect(err).NotTo(HaveOccurred())
		parsed := parseSelection(base64.RawURLEncoding.EncodeToString(selectionRaw))
		Expect(parsed).NotTo(BeNil())
		Expect(parsed.Decision).To(Equal("dragon"))
		Expect(parsed.Shape).To(BeEmpty())
		Expect(parsed.Recipe).To(BeEmpty())
	})
})
