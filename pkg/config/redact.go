package config

import (
	"net/url"
	"regexp"
	"strings"
)

const redactedPassword = "xxxxx"

var (
	urlPasswordPattern     = regexp.MustCompile(`(://[^:/?#@]+:)[^@]+@`)
	keywordPasswordPattern = regexp.MustCompile(`(\bpassword=)[^\s&]+`)
)

// RedactDSN masks the password in a database connection string so the string
// can be logged safely. URL DSNs (postgres://user:pass@host/db, including the
// libpq-style ?password=... query parameter) and keyword/value DSNs
// (host=... password=...) are supported. Strings without a password are
// returned unchanged.
func RedactDSN(dsn string) string {
	if u, err := url.Parse(dsn); err == nil && u.Scheme != "" {
		if u.User != nil {
			if _, ok := u.User.Password(); ok {
				u.User = url.UserPassword(u.User.Username(), redactedPassword)
			}
		}
		if q := u.Query(); q.Has("password") {
			q.Set("password", redactedPassword)
			u.RawQuery = q.Encode()
		}
		return u.String()
	}
	redacted := dsn
	if strings.Contains(dsn, "://") {
		redacted = urlPasswordPattern.ReplaceAllString(redacted, "${1}"+redactedPassword+"@")
	}
	return keywordPasswordPattern.ReplaceAllString(redacted, "${1}"+redactedPassword)
}
