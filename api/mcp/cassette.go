package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/api/cassetterunner"
)

const (
	cassetteToolTimeout   = 30 * time.Second
	maxCassetteToolBytes  = 8 << 20
	maxCassetteErrorBytes = 4 << 10
)

func (s *Server) callCassette(
	ctx context.Context,
	inbound *http.Request,
	instance *cassetterunner.Instance,
	tool cassetterunner.MCPTool,
	input map[string]any,
) (map[string]any, error) {
	if input == nil {
		input = map[string]any{}
	}
	body, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("encode cassette tool arguments: %w", err)
	}
	target, err := url.Parse(instance.URL)
	if err != nil {
		return nil, fmt.Errorf("cassette %q has an invalid target: %w", instance.Name, err)
	}
	callCtx, cancel := context.WithTimeout(ctx, cassetteToolTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(callCtx, tool.Method, target.String(), bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build cassette tool request: %w", err)
	}
	cassetterunner.CopyRequestHeaders(request.Header, inbound.Header)
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	cassetterunner.RewriteProxyRequest(
		&httputil.ProxyRequest{In: inbound, Out: request}, target, instance.Local(tool.Path), instance.Name)

	response, err := s.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("cassette %q tool %q failed: %w", instance.Name, tool.Name, err)
	}
	defer response.Body.Close()

	encoded, err := io.ReadAll(io.LimitReader(response.Body, maxCassetteToolBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read cassette %q tool %q response: %w", instance.Name, tool.Name, err)
	}
	if len(encoded) > maxCassetteToolBytes {
		return nil, fmt.Errorf("cassette %q tool %q response exceeds %d bytes", instance.Name, tool.Name, maxCassetteToolBytes)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		if len(encoded) > maxCassetteErrorBytes {
			encoded = append(encoded[:maxCassetteErrorBytes], []byte("… (truncated)")...)
		}
		message := strings.TrimSpace(string(encoded))
		if message == "" {
			message = response.Status
		}
		return nil, fmt.Errorf("cassette %q tool %q returned %s: %s", instance.Name, tool.Name, response.Status, message)
	}

	var output map[string]any
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&output); err != nil {
		return nil, fmt.Errorf("cassette %q tool %q did not return a JSON object: %w", instance.Name, tool.Name, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("cassette %q tool %q returned trailing JSON", instance.Name, tool.Name)
	}
	if output == nil {
		return nil, fmt.Errorf("cassette %q tool %q returned null instead of a JSON object", instance.Name, tool.Name)
	}
	return output, nil
}
