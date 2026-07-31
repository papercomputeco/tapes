package config

import (
	"fmt"
	"net/url"

	"github.com/papercomputeco/tapes/pkg/cassette"
)

// ValidateCassettes checks this config's cassette OpenAPI source URLs.
func (config *Config) ValidateCassettes() error {
	return ValidateCassetteSources(config.Cassettes)
}

// ValidateCassetteSources checks configured full OpenAPI source URLs. Resolution
// and metadata admission are runtime concerns so unreachable sources remain
// retryable.
func ValidateCassetteSources(sources []string) error {
	problems := make([]cassette.Problem, 0)
	seen := make(map[string]int, len(sources))
	for index, source := range sources {
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
	if parsed.Hostname() == "" {
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
