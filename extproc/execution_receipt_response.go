package extproc

import (
	"strings"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
)

func receiptHeaderKeys(hdrs *extprocv3.HttpHeaders) []string {
	var keys []string
	for _, header := range hdrs.GetHeaders().GetHeaders() {
		name := strings.ToLower(header.GetKey())
		if strings.HasPrefix(name, "x-paper-execution-") || strings.HasPrefix(name, "x-vsr-") {
			keys = append(keys, name)
		}
	}
	return keys
}

func (p *Processor) executionReceiptResponse(st *streamState, hdrs *extprocv3.HttpHeaders) *extprocv3.HeadersResponse {
	response := &extprocv3.HeadersResponse{}
	if !p.finalDispatchReceipts {
		return response
	}
	mutation := &extprocv3.HeaderMutation{RemoveHeaders: receiptHeaderKeys(hdrs)}
	response.Response = &extprocv3.CommonResponse{HeaderMutation: mutation}
	if st.receipt == nil {
		return response
	}
	receipt := *st.receipt
	if st.statusCode >= 400 {
		receipt.Outcome, receipt.FailureStage = "failed", "upstream-response"
	}
	value, err := EncodeExecutionReceipt(&receipt)
	if err != nil {
		return response
	}
	mutation.SetHeaders = []*corev3.HeaderValueOption{{Header: &corev3.HeaderValue{Key: ExecutionReceiptHeader, RawValue: []byte(value)}, AppendAction: corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD}}
	return response
}
