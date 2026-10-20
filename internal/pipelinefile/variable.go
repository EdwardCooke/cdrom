package pipelinefile

import (
	"fmt"
	"strings"
)

// VariableShape is the shape of a variable's value (F-21.1.3): a plain value,
// an encrypted (builtin F-12 ciphertext) value, or a sourced (a value from a
// named secretSources entry) value. A variable is masked when its shape is not
// VariablePlain — that is, when its value is not a plain string.
type VariableShape int

// The shapes a variable's value can be.
const (
	// VariablePlain is a plain value (a scalar string); it is not masked.
	VariablePlain VariableShape = iota
	// VariableEncrypted is a builtin F-12 ciphertext (an {encrypted:
	// <ciphertext>} value); it is masked.
	VariableEncrypted
	// VariableSecretSource is a value from a named secretSources entry (a
	// {secretSource: <name>, key: <key>} value); it is masked.
	VariableSecretSource
)

// String renders the shape's name.
func (s VariableShape) String() string {
	switch s {
	case VariablePlain:
		return "plain"
	case VariableEncrypted:
		return "encrypted"
	case VariableSecretSource:
		return "secretSource"
	default:
		return fmt.Sprintf("variableShape(%d)", int(s))
	}
}

// VariableValue is the compiled form of a single variable's value (F-21.1.3).
// There is no separate concept of a secret: every value is a variable, and a
// variable is masked when its value is not a plain string. Exactly one of the
// shape-specific fields is set, selected by Shape:
//   - VariablePlain: Plain (the value);
//   - VariableEncrypted: Encrypted (the builtin F-12 ciphertext);
//   - VariableSecretSource: SecretSource (the named source) + Key (the value's
//     key within the source).
type VariableValue struct {
	// Shape is the value's shape.
	Shape VariableShape
	// Plain is the value when Shape is VariablePlain.
	Plain string
	// Encrypted is the builtin F-12 ciphertext when Shape is
	// VariableEncrypted. It is self-describing (it carries the nonce) and safe
	// to store opaquely; the API decrypts it at dispatch and hands the
	// plaintext to the execution target. The plaintext is never persisted.
	Encrypted string
	// SecretSource is the name of the secretSources entry the value is sourced
	// from when Shape is VariableSecretSource.
	SecretSource string
	// Key is the value's key within the named source when Shape is
	// VariableSecretSource.
	Key string
}

// IsMasked reports whether the value is masked (its shape is not
// VariablePlain). A masked variable's value is redacted in logs and hidden in
// the UI, and its plaintext is never persisted or returned to the UI.
func (v VariableValue) IsMasked() bool {
	return v.Shape != VariablePlain
}

// Variable is a single variable in a `variables` map (F-21.1.3): a name and a
// value (a plain value, an encrypted ciphertext, or a secret-source
// reference). A variable is masked when its value is not a plain string. All
// variables — masked or not — are referenced the same way, as
// `{{ .params.<name> }}`.
type Variable struct {
	// Name is the variable's name (its key in the `variables` map).
	Name string
	// Value is the variable's compiled value (its shape and content).
	Value VariableValue
}

// VariableSet is a named variable set (F-21.1.3): an inline `variables` map
// declared under `spec.variableSets`. A job references a set by name in its
// `variableSets` list. (External-file / vault imports are F-21.4.)
type VariableSet struct {
	// Name is the set's name (its key under spec.variableSets).
	Name string
	// Variables are the set's variables, in file order.
	Variables []Variable
}

// SecretSource is a named secret source (F-21.1.3 / F-12): a `kind` (a
// secret-store kind — `Vault` / `OpenBao` are reserved kinds; the built-in
// AES store is the default when no source is named), a `path` (the source
// path), and an `audience` (the token audience used to fetch from the
// source). The source's settings are stored opaquely on the pipeline (like a
// secret declaration, F-12); the API resolves the named source to a concrete
// value at dispatch and hands it to the execution target.
type SecretSource struct {
	// Name is the source's name (its key under spec.secretSources).
	Name string
	// Kind is the secret-store kind (e.g. "Vault", "OpenBao"); empty means
	// the built-in AES store (the default when no source is named).
	Kind string
	// Path is the source path.
	Path string
	// Audience is the token audience used to fetch from the source.
	Audience string
}

// CompileVariables compiles a `variables` map (a name → value map, in file
// order) into an ordered list of Variables, returning a validation error for
// each variable whose value is malformed. A variable is masked when its value
// is not a plain string (it is an {encrypted: <ciphertext>} map or a
// {secretSource: <name>, key: <key>} map); the compiler records the shape so
// the platform can redact a masked variable's value in logs and hide it in the
// UI. A nil or non-map value yields an empty list (the set declares no
// variables). file is the name of the offending file (carried into any
// validation errors), and base is the dotted path of the variables map (e.g.
// "spec.variableSets.global.variables"), so a validation error names the
// offending key (e.g. "spec.variableSets.global.variables.region").
func CompileVariables(file string, base Path, value Value) ([]Variable, Errors) {
	if value.Type != Map {
		return nil, nil
	}
	vars := make([]Variable, 0, len(value.Entries))
	var errs Errors
	for _, e := range value.Entries {
		vv, reason := compileVariableValue(e.Value)
		if reason != "" {
			errs = append(errs, Error{File: file, Key: base.Extend(e.Key).String(), Reason: reason})
			continue
		}
		vars = append(vars, Variable{Name: e.Key, Value: vv})
	}
	return vars, errs
}

// compileVariableValue converts a single variable's value (from the file) into
// a VariableValue, returning a non-empty reason when the value is malformed.
// A variable's value is one of three shapes:
//   - a plain value (a scalar string) — not masked;
//   - an encrypted value (a map with an `encrypted` key, a builtin F-12
//     ciphertext) — masked;
//   - a sourced value (a map with a `secretSource` key and a `key`) — a value
//     from a named secretSources entry — masked.
func compileVariableValue(v Value) (VariableValue, string) {
	switch v.Type {
	case Scalar:
		return VariableValue{Shape: VariablePlain, Plain: v.String}, ""
	case Map:
		encrypted, hasEncrypted := v.Get("encrypted")
		source, hasSource := v.Get("secretSource")
		switch {
		case hasEncrypted && hasSource:
			return VariableValue{}, "a variable's value may be either an {encrypted: <ciphertext>} map or a {secretSource: <name>, key: <key>} map, not both"
		case hasEncrypted:
			if encrypted.Type != Scalar {
				return VariableValue{}, "a variable's encrypted value must be a string (the builtin F-12 ciphertext)"
			}
			if extra := unexpectedVariableKeys(v, "encrypted"); extra != "" {
				return VariableValue{}, extra
			}
			return VariableValue{Shape: VariableEncrypted, Encrypted: encrypted.String}, ""
		case hasSource:
			if source.Type != Scalar {
				return VariableValue{}, "a variable's secretSource must be a string (the name of a secretSources entry)"
			}
			key, hasKey := v.Get("key")
			if !hasKey {
				return VariableValue{}, "a variable that sources a value from a secretSource must also set a key (the value's key within the source)"
			}
			if key.Type != Scalar {
				return VariableValue{}, "a variable's key must be a string (the value's key within the secretSource)"
			}
			if extra := unexpectedVariableKeys(v, "secretSource", "key"); extra != "" {
				return VariableValue{}, extra
			}
			return VariableValue{Shape: VariableSecretSource, SecretSource: source.String, Key: key.String}, ""
		default:
			return VariableValue{}, "a variable's value must be a plain string, an {encrypted: <ciphertext>} map, or a {secretSource: <name>, key: <key>} map"
		}
	default:
		return VariableValue{}, "a variable's value must be a plain string, an {encrypted: <ciphertext>} map, or a {secretSource: <name>, key: <key>} map"
	}
}

// unexpectedVariableKeys returns a non-empty reason when the variable value
// map (value) has any key other than the allowed keys, naming the first
// unexpected key. It is how the compiler enforces that an {encrypted: ...}
// map has only the encrypted key and a {secretSource: ..., key: ...} map has
// only the secretSource and key keys.
func unexpectedVariableKeys(value Value, allowed ...string) string {
	allowedSet := make(map[string]bool, len(allowed))
	for _, a := range allowed {
		allowedSet[a] = true
	}
	for _, e := range value.Entries {
		if !allowedSet[e.Key] {
			return fmt.Sprintf("an unrecognized key %q (a variable's value has only %s keys)", e.Key, strings.Join(allowed, " and "))
		}
	}
	return ""
}

// CompileVariableSets compiles the pipeline's `spec.variableSets` map (a name
// → variable-set map, in file order) into a name → VariableSet map. Each set
// is an inline `variables` map (external-file / vault imports are F-21.4). A
// nil or non-map value yields an empty map (the pipeline declares no variable
// sets). A set that is not a mapping, or that carries an unrecognized key, is
// a validation error. file is the name of the offending file (carried into any
// validation errors).
func CompileVariableSets(file string, value Value) (map[string]*VariableSet, Errors) {
	out := make(map[string]*VariableSet)
	if value.Type != Map {
		return out, nil
	}
	var errs Errors
	for _, e := range value.Entries {
		set, setErrs := compileVariableSet(file, NewPath("spec", "variableSets", e.Key), e.Key, e.Value)
		if len(setErrs) > 0 {
			errs = append(errs, setErrs...)
			continue
		}
		out[e.Key] = set
	}
	return out, errs
}

// compileVariableSet compiles a single variable set (a name → set-spec map
// entry) into a VariableSet. A set is a mapping whose only recognized key is
// `variables` (an inline variables map; external-file / vault imports are
// F-21.4); a set with no `variables` key is an empty set. A set that is not a
// mapping, or that carries an unrecognized key, is a validation error.
func compileVariableSet(file string, base Path, name string, value Value) (*VariableSet, Errors) {
	if value.Type != Map {
		return nil, Errors{Error{File: file, Key: base.String(),
			Reason: "a variable set must be a mapping (its variables map)"}}
	}
	var errs Errors
	var vars []Variable
	for _, e := range value.Entries {
		switch e.Key {
		case "variables":
			v, vErrs := CompileVariables(file, base.Extend("variables"), e.Value)
			errs = append(errs, vErrs...)
			vars = v
		default:
			errs = append(errs, Error{File: file, Key: base.Extend(e.Key).String(),
				Reason: fmt.Sprintf("a variable set has only a variables key (external-file / vault imports are F-21.4); %q is not recognized", e.Key)})
		}
	}
	if len(errs) > 0 {
		return nil, errs
	}
	return &VariableSet{Name: name, Variables: vars}, nil
}

// CompileSecretSources compiles the pipeline's `spec.secretSources` map (a
// name → secret-source map, in file order) into a name → SecretSource map.
// Each source has a `kind` (a secret-store kind — `Vault` / `OpenBao` are
// reserved kinds; the built-in AES store is the default when no source is
// named), a `path` (the source path), and an `audience` (the token audience
// used to fetch from the source). The source's settings are stored opaquely on
// the pipeline (like a secret declaration, F-12); the API resolves the named
// source to a concrete value at dispatch. A nil or non-map value yields an
// empty map (the pipeline declares no secret sources). file is the name of the
// offending file (carried into any validation errors).
func CompileSecretSources(file string, value Value) (map[string]*SecretSource, Errors) {
	out := make(map[string]*SecretSource)
	if value.Type != Map {
		return out, nil
	}
	var errs Errors
	for _, e := range value.Entries {
		src, srcErrs := compileSecretSource(file, NewPath("spec", "secretSources", e.Key), e.Key, e.Value)
		if len(srcErrs) > 0 {
			errs = append(errs, srcErrs...)
			continue
		}
		out[e.Key] = src
	}
	return out, errs
}

// compileSecretSource compiles a single secret source (a name → source-spec
// map entry) into a SecretSource. A source is a mapping whose recognized keys
// are `kind`, `path`, and `audience` (all optional; an empty kind means the
// built-in AES store). A source that is not a mapping, or that carries an
// unrecognized key or a non-scalar recognized key, is a validation error.
func compileSecretSource(file string, base Path, name string, value Value) (*SecretSource, Errors) {
	if value.Type != Map {
		return nil, Errors{Error{File: file, Key: base.String(),
			Reason: "a secret source must be a mapping (its kind, path, and audience)"}}
	}
	src := &SecretSource{Name: name}
	var errs Errors
	for _, e := range value.Entries {
		switch e.Key {
		case "kind":
			if e.Value.Type != Scalar {
				errs = append(errs, Error{File: file, Key: base.Extend("kind").String(), Reason: "a secret source's kind must be a string (e.g. Vault, OpenBao)"})
			} else {
				src.Kind = e.Value.String
			}
		case "path":
			if e.Value.Type != Scalar {
				errs = append(errs, Error{File: file, Key: base.Extend("path").String(), Reason: "a secret source's path must be a string"})
			} else {
				src.Path = e.Value.String
			}
		case "audience":
			if e.Value.Type != Scalar {
				errs = append(errs, Error{File: file, Key: base.Extend("audience").String(), Reason: "a secret source's audience must be a string"})
			} else {
				src.Audience = e.Value.String
			}
		default:
			errs = append(errs, Error{File: file, Key: base.Extend(e.Key).String(),
				Reason: fmt.Sprintf("a secret source has only kind, path, and audience keys; %q is not recognized", e.Key)})
		}
	}
	if len(errs) > 0 {
		return nil, errs
	}
	return src, nil
}

// ResolveVariableSets resolves a job's `variableSets` list (a list of named
// set references, in file order) against the pipeline's declared variable sets
// (from CompileVariableSets), returning the referenced sets in the order the
// job lists them. A reference to a set that is not declared under
// spec.variableSets is a validation error naming the offending key, and a
// list entry that is not a string is a validation error. A nil or non-list
// value yields an empty list (the job references no sets). file is the name of
// the offending file (carried into any validation errors), and base is the
// dotted path of the job (e.g. "spec.jobs.deploy-dev"), so a validation error
// names the offending key (e.g. "spec.jobs.deploy-dev.variableSets[1]").
func ResolveVariableSets(file string, base Path, list Value, declared map[string]*VariableSet) ([]*VariableSet, Errors) {
	if list.Type != List {
		return nil, nil
	}
	out := make([]*VariableSet, 0, len(list.Items))
	var errs Errors
	for i, item := range list.Items {
		// The offending key is the job's variableSets list element (e.g.
		// "spec.jobs.deploy-dev.variableSets[0]"); the index is appended
		// directly (not as a dotted segment) so it reads as a list element.
		key := base.Extend("variableSets").String() + fmt.Sprintf("[%d]", i)
		if item.Type != Scalar {
			errs = append(errs, Error{File: file, Key: key,
				Reason: "a job's variableSets list entries must be strings (the names of variable sets)"})
			continue
		}
		name := item.String
		set, ok := declared[name]
		if !ok {
			errs = append(errs, Error{File: file, Key: key,
				Reason: fmt.Sprintf("no variable set named %q (a job's variableSets list may only reference a set declared under spec.variableSets)", name)})
			continue
		}
		out = append(out, set)
	}
	return out, errs
}

// MergeJobVariables merges a job's inline `variables` (inline, the highest
// priority) with the variables of each named set in the job's `variableSets`
// list (in the order the sets are listed; an earlier-listed set has higher
// priority than a later-listed one), producing the job's effective variable
// set for spec interpolation (F-10). On a key collision the job's inline
// variables win over a set's, and an earlier-listed set wins over a
// later-listed set. The result is an ordered list of Variables: the inline
// variables first (in file order), then each set's variables that are not
// already present (in file order), in the order the sets are listed.
func MergeJobVariables(inline []Variable, sets []*VariableSet) []Variable {
	merged := make([]Variable, 0, len(inline))
	seen := make(map[string]bool)
	// The job's inline variables have the highest priority: they are added
	// first and win any key collision.
	for _, v := range inline {
		if seen[v.Name] {
			continue
		}
		seen[v.Name] = true
		merged = append(merged, v)
	}
	// The sets follow, in listed order (an earlier-listed set has higher
	// priority than a later-listed one): a set's variable is added only when
	// its name is not already present (from the inline variables or an
	// earlier-listed set).
	for _, set := range sets {
		for _, v := range set.Variables {
			if seen[v.Name] {
				continue
			}
			seen[v.Name] = true
			merged = append(merged, v)
		}
	}
	return merged
}

// ValidateSecretSourceRefs validates that every sourced variable in variables
// (a variable whose value is a {secretSource: <name>, key: <key>} map) names a
// secret source declared in sources (from CompileSecretSources). A variable
// that sources a value from a source that is not declared is a validation error
// naming the offending key. A variable that is not sourced (a plain or
// encrypted value) yields no error. file is the name of the offending file
// (carried into any validation errors), and base is the dotted path of the
// variables map (e.g. "spec.jobs.deploy-dev.variables"), so a validation error
// names the offending key (e.g. "spec.jobs.deploy-dev.variables.deploy_token").
func ValidateSecretSourceRefs(file string, base Path, variables []Variable, sources map[string]*SecretSource) Errors {
	var errs Errors
	for _, v := range variables {
		if v.Value.Shape != VariableSecretSource {
			continue
		}
		if _, ok := sources[v.Value.SecretSource]; !ok {
			errs = append(errs, Error{File: file, Key: base.Extend(v.Name).String(),
				Reason: fmt.Sprintf("variable %q sources a value from secret source %q, which is not declared under spec.secretSources", v.Name, v.Value.SecretSource)})
		}
	}
	return errs
}

// PlainParams returns the plain (unmasked) variables' values as a name → value
// map: the run's parameters for spec interpolation (F-10). Masked variables
// (encrypted or sourced) are omitted — their values are resolved at dispatch
// (F-12) and are never persisted. An empty list yields an empty map.
func PlainParams(vars []Variable) map[string]string {
	out := make(map[string]string)
	for _, v := range vars {
		if v.Value.Shape == VariablePlain {
			out[v.Name] = v.Value.Plain
		}
	}
	return out
}

// MaskedVariables returns the masked variables (those whose value is not a
// plain string — an encrypted or sourced value), in order. The platform
// redacts a masked variable's value in logs and hides it in the UI, and its
// plaintext is never persisted or returned to the UI. An empty list yields an
// empty (non-nil) slice.
func MaskedVariables(vars []Variable) []Variable {
	out := make([]Variable, 0)
	for _, v := range vars {
		if v.Value.IsMasked() {
			out = append(out, v)
		}
	}
	return out
}
