package cassette

import "strings"

// defaultSubject is what a ValidationError describes when Subject is unset.
const defaultSubject = "cassette manifest"

// Problem identifies one invalid field without stopping validation of the rest
// of a manifest.
type Problem struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// ValidationError contains every problem found during one validation pass.
type ValidationError struct {
	// Subject names what failed validation. Empty means the cassette manifest,
	// so manifest validators need not set it; tapes config validation sets it
	// so one error shape can serve every document
	// without every message claiming to be about a manifest.
	Subject  string    `json:"subject,omitempty"`
	Problems []Problem `json:"problems"`
}

func (validationError *ValidationError) Error() string {
	if validationError == nil || len(validationError.Problems) == 0 {
		return defaultSubject + " validation failed"
	}

	subject := validationError.Subject
	if subject == "" {
		subject = defaultSubject
	}

	var message strings.Builder
	message.WriteString(subject)
	message.WriteString(" validation failed")
	for _, problem := range validationError.Problems {
		message.WriteString("\n- ")
		message.WriteString(problem.Field)
		message.WriteString(": ")
		message.WriteString(problem.Message)
	}

	return message.String()
}
