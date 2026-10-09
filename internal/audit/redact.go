// Package audit — secret redaction (F-15).
//
// The audit log must never store or display a secret's value: a secret is
// represented by its name only. The API builds the old/new value documents for
// an audited action and, before recording the event, runs them through a
// Redactor that replaces any secret plaintext with a marker. This is
// defense-in-depth on top of the API omitting secret values from the documents
// it builds: even if a secret's value appears in another field (a description
// or a parameter that happens to equal a secret), it is redacted before it
// reaches the audit log or the database.
package audit

import (
	"sort"
	"strings"
)

// RedactionMarker replaces a secret's value in an audited value document.
const RedactionMarker = "[REDACTED]"

// Redactor replaces a set of secret plaintext values in a string with the
// redaction marker. A nil Redactor is a no-op.
type Redactor struct {
	// values are the secret plaintexts to redact, sorted longest-first so a
	// value that is a substring of a longer one is not partially redacted
	// before the longer one is.
	values []string
}

// NewRedactor returns a Redactor for the given secret plaintext values. Empty
// values are dropped. A nil or empty result redacts nothing.
func NewRedactor(values []string) *Redactor {
	out := make([]string, 0, len(values))
	for _, v := range values {
		if v != "" {
			out = append(out, v)
		}
	}
	if len(out) == 0 {
		return nil
	}
	sort.Slice(out, func(i, j int) bool { return len(out[i]) > len(out[j]) })
	return &Redactor{values: out}
}

// Redact replaces every occurrence of a secret plaintext in s with the
// redaction marker. A nil receiver is a no-op.
func (r *Redactor) Redact(s string) string {
	if r == nil || len(r.values) == 0 {
		return s
	}
	for _, v := range r.values {
		s = strings.ReplaceAll(s, v, RedactionMarker)
	}
	return s
}
