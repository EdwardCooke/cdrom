package pipelinefile

import (
	"fmt"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Value is a node of a parsed pipeline definition: a scalar (a string,
// integer, or boolean), a list, or a map. A map preserves the order its keys
// appear in the file (Keys), so a YAML map still yields a deterministic
// ordered list — the property the compiler relies on to turn a name → spec
// map (steps, jobs, variable sets) into an ordered, name-addressable list.
//
// A Value is produced by Parse (or by the yaml.v3 decode it performs) and is
// the unit the compiler walks: it descends into a map's entries in file order
// and reports validation errors by the key path it walked.
type Value struct {
	// Type is the node's kind: scalar, list, or map.
	Type ValueType
	// String is the node's value when Type is Scalar.
	String string
	// Entries are the map's entries in file order when Type is Map.
	Entries []Entry
	// Items are the list's items in order when Type is List.
	Items []Value
}

// ValueType is the kind of a Value node.
type ValueType int

// The kinds of a Value node.
const (
	// Scalar is a string, integer, or boolean.
	Scalar ValueType = iota
	// List is an ordered list of values.
	List
	// Map is an ordered map of key → value.
	Map
)

// String renders the kind's name.
func (k ValueType) String() string {
	switch k {
	case Scalar:
		return "scalar"
	case List:
		return "list"
	case Map:
		return "map"
	default:
		return fmt.Sprintf("valueType(%d)", int(k))
	}
}

// Entry is one key/value pair of a map Value, in file order.
type Entry struct {
	// Key is the entry's key.
	Key string
	// Value is the entry's value.
	Value Value
}

// Get looks up key in a map Value and reports whether it is present. It is a
// convenience for the compiler's descent; a non-map Value reports false.
func (v Value) Get(key string) (Value, bool) {
	if v.Type != Map {
		return Value{}, false
	}
	for _, e := range v.Entries {
		if e.Key == key {
			return e.Value, true
		}
	}
	return Value{}, false
}

// Has reports whether a map Value contains key.
func (v Value) Has(key string) bool {
	_, ok := v.Get(key)
	return ok
}

// StringValue returns the entry's value as a string when the entry is present
// and is a scalar; it reports false otherwise.
func (v Value) StringValue(key string) (string, bool) {
	val, ok := v.Get(key)
	if !ok {
		return "", false
	}
	if val.Type != Scalar {
		return "", false
	}
	return val.String, true
}

// StringSlice returns the entry's value as a list of strings when the entry
// is present and is a list of scalars; it reports false otherwise.
func (v Value) StringSlice(key string) ([]string, bool) {
	val, ok := v.Get(key)
	if !ok || val.Type != List {
		return nil, false
	}
	out := make([]string, 0, len(val.Items))
	for _, item := range val.Items {
		if item.Type != Scalar {
			return nil, false
		}
		out = append(out, item.String)
	}
	return out, true
}

// MapValue returns the entry's value as a map when the entry is present and is
// a map; it reports false otherwise.
func (v Value) MapValue(key string) (Value, bool) {
	val, ok := v.Get(key)
	if !ok || val.Type != Map {
		return Value{}, false
	}
	return val, true
}

// StringMap returns the entry's value as a key → string map when the entry is
// present and is a map of scalar values (in file order); it reports false
// otherwise.
func (v Value) StringMap(key string) (map[string]string, bool) {
	val, ok := v.Get(key)
	if !ok || val.Type != Map {
		return nil, false
	}
	out := make(map[string]string, len(val.Entries))
	for _, e := range val.Entries {
		if e.Value.Type != Scalar {
			return nil, false
		}
		out[e.Key] = e.Value.String
	}
	return out, true
}

// AsString returns the Value's string when it is a scalar, or the empty string
// otherwise. It is a convenience for the compiler's descent into a scalar
// field (a step's workdir, timeout, or an empty entry).
func (v Value) AsString() string {
	if v.Type != Scalar {
		return ""
	}
	return v.String
}

// IsNull reports whether the Value is an empty entry: a scalar with an empty
// string. yaml.v3 decodes a key with no value (e.g. `step1:`) as a null
// scalar, which is how the file format's empty step entry (shorthand for
// `ref: <same name>`) is represented. A non-scalar, or a scalar with a
// non-empty string, is not null.
func (v Value) IsNull() bool {
	return v.Type == Scalar && v.String == ""
}

// AsBool parses the Value's string as a boolean (strconv.ParseBool) when it is
// a scalar, reporting whether it parsed. A non-scalar Value reports false. It
// is how a step's boolean common fields (ignoreFailed, once) are lifted.
func (v Value) AsBool() (bool, bool) {
	if v.Type != Scalar {
		return false, false
	}
	b, err := strconv.ParseBool(v.String)
	if err != nil {
		return false, false
	}
	return b, true
}

// AsBoolField returns the entry's value as a boolean (strconv.ParseBool) when
// the entry is present and is a scalar that parses as a boolean; it reports
// false otherwise. It is how a job's boolean deployment options (stepBarrier,
// ignoreFailed) are lifted from the file (F-21.1.4).
func (v Value) AsBoolField(key string) (bool, bool) {
	val, ok := v.Get(key)
	if !ok {
		return false, false
	}
	return val.AsBool()
}

// AsStringMap returns the Value's entries as a key → string map when it is a
// map of scalar values (in file order), reporting whether it is. A non-map, or
// a map with a non-scalar value, reports false. It is how a step's env map is
// lifted onto the JobStep.
func (v Value) AsStringMap() (map[string]string, bool) {
	if v.Type != Map {
		return nil, false
	}
	out := make(map[string]string, len(v.Entries))
	for _, e := range v.Entries {
		if e.Value.Type != Scalar {
			return nil, false
		}
		out[e.Key] = e.Value.String
	}
	return out, true
}

// decodeValue converts a yaml.v3 node into a Value, preserving the order of a
// mapping's keys (yaml.v3 keeps a mapping's keys in document order in
// node.Content, alternating key/value nodes).
func decodeValue(node *yaml.Node) (Value, error) {
	switch node.Kind {
	case yaml.DocumentNode:
		if len(node.Content) == 0 {
			return Value{}, nil
		}
		return decodeValue(node.Content[0])
	case yaml.ScalarNode:
		return Value{Type: Scalar, String: node.Value}, nil
	case yaml.SequenceNode:
		items := make([]Value, 0, len(node.Content))
		for _, child := range node.Content {
			item, err := decodeValue(child)
			if err != nil {
				return Value{}, err
			}
			items = append(items, item)
		}
		return Value{Type: List, Items: items}, nil
	case yaml.MappingNode:
		entries := make([]Entry, 0, len(node.Content)/2)
		for i := 0; i+1 < len(node.Content); i += 2 {
			keyNode, valNode := node.Content[i], node.Content[i+1]
			if keyNode.Kind != yaml.ScalarNode {
				return Value{}, fmt.Errorf("a mapping key must be a scalar (got %s)", kindName(keyNode.Kind))
			}
			val, err := decodeValue(valNode)
			if err != nil {
				return Value{}, err
			}
			entries = append(entries, Entry{Key: keyNode.Value, Value: val})
		}
		return Value{Type: Map, Entries: entries}, nil
	default:
		return Value{}, fmt.Errorf("unsupported YAML node kind %s", kindName(node.Kind))
	}
}

// MarshalYAML renders the Value as a yaml.v3 node (the inverse of decodeValue):
// a scalar is a scalar node, a list is a sequence node, and a map is a mapping
// node (its entries in order). It is what lets the export endpoint (F-21.1.4)
// marshal a compiled definition back into the file format with yaml.Marshal.
func (v Value) MarshalYAML() (interface{}, error) {
	switch v.Type {
	case Scalar:
		return &yaml.Node{Kind: yaml.ScalarNode, Value: v.String}, nil
	case List:
		node := &yaml.Node{Kind: yaml.SequenceNode}
		for _, item := range v.Items {
			child, err := item.MarshalYAML()
			if err != nil {
				return nil, err
			}
			node.Content = append(node.Content, child.(*yaml.Node))
		}
		return node, nil
	case Map:
		node := &yaml.Node{Kind: yaml.MappingNode}
		for _, e := range v.Entries {
			key := &yaml.Node{Kind: yaml.ScalarNode, Value: e.Key}
			val, err := e.Value.MarshalYAML()
			if err != nil {
				return nil, err
			}
			node.Content = append(node.Content, key, val.(*yaml.Node))
		}
		return node, nil
	default:
		return nil, fmt.Errorf("cannot marshal value of type %s", v.Type)
	}
}

// kindName renders a yaml.v3 node kind as a short name for error messages.
func kindName(kind yaml.Kind) string {
	switch kind {
	case yaml.DocumentNode:
		return "document"
	case yaml.ScalarNode:
		return "scalar"
	case yaml.SequenceNode:
		return "sequence"
	case yaml.MappingNode:
		return "mapping"
	default:
		return fmt.Sprintf("kind %d", int(kind))
	}
}

// Path is the location of a key within a document, rendered as a dotted path
// (e.g. "spec.jobs.deploy-dev.steps.build.shell"). It is the "key" an
// actionable validation error names, so a reader can find the offending line
// in the file.
type Path []string

// NewPath builds a Path from its segments.
func NewPath(segments ...string) Path {
	out := make(Path, len(segments))
	copy(out, segments)
	return out
}

// Extend returns a new Path with the given segments appended.
func (p Path) Extend(segments ...string) Path {
	out := make(Path, 0, len(p)+len(segments))
	out = append(out, p...)
	out = append(out, segments...)
	return out
}

// String renders the path as a dotted string. An empty path renders as the
// empty string (the document root).
func (p Path) String() string {
	if len(p) == 0 {
		return ""
	}
	return strings.Join(p, ".")
}
