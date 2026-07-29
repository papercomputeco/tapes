package config

import (
	"fmt"
	"net/url"

	"github.com/papercomputeco/tapes/pkg/cassette"
)

// ValidateCassettes checks configured full OpenAPI source URLs. Resolution and
// metadata admission are runtime concerns so unreachable sources remain retryable.
func (config *Config) ValidateCassettes() error {
	problems := make([]cassette.Problem, 0)
	seen := make(map[string]int, len(config.Cassettes))
	for index, source := range config.Cassettes {
		field := fmt.Sprintf("cassettes[%d]", index)
		if message := validateCassetteURL(source); message != "" {
			problems = append(problems, cassette.Problem{Field: field, Message: message})
		}
		if previous, ok := seen[source]; ok {
			problems = append(problems, cassette.Problem{
				Field:   field,
				Message: fmt.Sprintf("duplicates cassettes[%d]", previous),
			})
		} else {
			seen[source] = index
		}
	}
	if len(problems) > 0 {
		return &cassette.ValidationError{Subject: "tapes config", Problems: problems}
	}
	return nil
}

func validateCassetteURL(source string) string {
	parsed, err := url.Parse(source)
	if err != nil {
		return "must be a valid URL"
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "must use the http or https scheme"
	}
	if parsed.Host == "" {
		return "must include a host"
	}
	if parsed.User != nil {
		return "must not include URL userinfo"
	}
	if parsed.Fragment != "" {
		return "must not carry a fragment"
	}
	return ""
}
