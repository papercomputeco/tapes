package extproc

import (
	"strings"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"

	"github.com/papercomputeco/tapes/extproc/headers"
)

// startExecutionReceipt is enabled only on a deployment whose capture point is
// after the final body mutation. Header names alone do not establish trust:
// the gateway must strip caller selectors and overwrite its backend identity.
func (p *Processor) startExecutionReceipt(st *streamState, hdrs *extprocv3.HttpHeaders) {
	if !p.finalDispatchReceipts {
		return
	}
	selection := parseSelection(headers.Get(hdrs, SelectionMetadataHeader))
	gateway := headers.Get(hdrs, "x-paper-gateway-id")
	if selection == nil || !validReceiptID(gateway) {
		return
	}
	backend := "" // Populated only from trusted Envoy dynamic metadata.
	requestID := st.requestID
	if !validReceiptID(requestID) {
		requestID = "extproc-" + randHex(16)
	}
	st.receipt = &ExecutionReceiptV1{
		Version: 1, ReceiptID: "receipt-" + randHex(16), AttemptID: "attempt-" + randHex(16),
		RequestID: requestID, GatewayID: gateway, Selection: *selection, BackendID: backend,
		Settings: map[string]ReceiptSetting{}, Outcome: "selected", TerminalOwner: "downstream-stream-adapter",
	}
}

func (st *streamState) observeFinalDispatch() {
	if st.receipt == nil || !st.reqEOS || st.reqOverBudget || st.requestDecodeErr != nil {
		return
	}
	model, settings := forwardedSettings(st.decodedReq)
	if model == "" || st.receipt.BackendID == "" {
		return
	}
	if st.receipt.Selection.hasRecipeContext() {
		// Recipe generations name an exact physical dispatch backend. The compiler
		// supplies the final AIServiceBackend name in selection.Backend; the live
		// org binding name lives separately in selection.BackendName.
		if st.receipt.Selection.Backend == "" {
			return
		}
		if st.receipt.Selection.Backend != st.receipt.BackendID {
			st.receipt = nil
			return
		}
	}
	st.receipt.Model, st.receipt.Settings = model, settings
	// This is a forwarding observation; a provider's completion is still unknown.
	st.receipt.Outcome = "dispatched"
}

func receiptRequestStripKeys(p *Processor, hdrs *extprocv3.HttpHeaders) []string {
	if !p.finalDispatchReceipts {
		return nil
	}
	return append(receiptHeaderKeys(hdrs), "x-paper-gateway-id")
}

// observeDispatchMetadata accepts only authoritative metadata from the final
// route. Native dispatch uses its own namespace, never a selection/header copy.
func (st *streamState) observeDispatchMetadata(metadata *corev3.Metadata) {
	if st.receipt == nil {
		return
	}
	namespaces := metadata.GetFilterMetadata()
	native, managed := namespaces["io.paper.native_dispatch"], namespaces["io.envoy.ai_gateway"]
	if native != nil {
		fields := native.GetFields()
		backend := fields["backend_name"].GetStringValue()
		destination := fields["destination_id"].GetStringValue()
		service := fields["service"].GetStringValue()
		if managed != nil || st.dispatchMetadataSource == "managed" || !validReceiptID(backend) || strings.Contains(backend, "/") || !validReceiptID(destination) || destination != st.receipt.Selection.DestinationID || (service != "anthropic" && service != "chatgpt") || (st.receipt.BackendID != "" && st.receipt.BackendID != backend) {
			// Conflicting or malformed trusted evidence invalidates this attempt;
			// later messages must not resurrect a partially supported receipt.
			st.receipt = nil
			return
		}
		st.dispatchMetadataSource = "native"
		if st.receipt.Selection.hasRecipeContext() && st.receipt.Selection.Backend != "" && st.receipt.Selection.Backend != backend {
			st.receipt = nil
			return
		}
		st.receipt.BackendID = backend
		return
	}
	if managed == nil {
		return
	}
	if st.dispatchMetadataSource == "native" {
		st.receipt = nil
		return
	}
	st.dispatchMetadataSource = "managed"
	value := managed.GetFields()["backend_name"].GetStringValue()
	parts := strings.Split(value, "/")
	if len(parts) >= 2 {
		value = parts[1]
	}
	if validReceiptID(value) {
		if (st.receipt.BackendID != "" && st.receipt.BackendID != value) || (st.receipt.Selection.hasRecipeContext() && st.receipt.Selection.Backend != "" && st.receipt.Selection.Backend != value) {
			st.receipt = nil
			return
		}
		st.receipt.BackendID = value
	}
}
