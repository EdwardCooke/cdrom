package pipelinesource

import (
	"cdrom/internal/sourceref"
)

// The reference kinds (F-21.2.2). A reference is a "<type>:<spec>" string; the
// type selects the kind and the spec carries the kind-specific locator. These
// mirror the shared sourceref kinds (the parser lives in internal/sourceref,
// which the pipeline compiler also uses to resolve relative import locators,
// F-21.4).
const (
	KindGit  = sourceref.KindGit
	KindHTTP = sourceref.KindHTTP
	KindFile = sourceref.KindFile
)

// Reference is a parsed source reference (the shared sourceref.Reference).
type Reference = sourceref.Reference

// ParseReference parses a "<type>:<spec>" reference into a Reference. It
// returns a clear error for an unknown type or a malformed reference.
func ParseReference(ref string) (*Reference, error) {
	return sourceref.ParseReference(ref)
}

// unescape resolves backslash escapes in s (alias for sourceref.Unescape),
// exposed privately so the reference parser tests keep compiling.
func unescape(s string) (string, error) { return sourceref.Unescape(s) }
