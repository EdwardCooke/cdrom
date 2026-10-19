package pipelinefile

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// APIVersion is the schema version of the file format (F-21.1.1). It is the
// only accepted value of a document's apiVersion field; the format evolves by
// bumping it through the project's lifecycle, and a document carrying any
// other apiVersion is a validation error.
const APIVersion = "cdrom/alphav1"

// Kind is what a document is. The compiler dispatches on kind to know how to
// interpret a document: a Pipeline document's spec holds the jobs and the
// pipeline-level config; a VariableSet document's spec is a single variable
// set (its variables); a Step document's spec is a single step definition;
// and a Job document's spec is a single job spec. Each file holds exactly one
// unit, so a file contributes a reusable unit (kind Step / VariableSet / Job)
// that many pipelines pull in by import (F-21.4).
type Kind string

// The kinds a document can be.
const (
	// KindPipeline is a pipeline definition: its spec holds the jobs and the
	// pipeline-level config (workerGroup, failureMode, concurrencyKeys,
	// params, triggers, artifacts, steps, variableSets, secretSources).
	KindPipeline Kind = "Pipeline"
	// KindVariableSet is a single named variable set: its spec is the set's
	// variables (and, in F-21.4, an import list).
	KindVariableSet Kind = "VariableSet"
	// KindStep is a single step definition: its spec is the step's spec.
	KindStep Kind = "Step"
	// KindJob is a single job spec: its spec is the job's steps, needs,
	// variables, environment, and deployment options.
	KindJob Kind = "Job"
)

// KnownKinds are the kinds the compiler recognizes, in the order they are
// listed in the format reference.
var KnownKinds = []Kind{KindPipeline, KindVariableSet, KindStep, KindJob}

// IsKnown reports whether k is one of the known kinds.
func (k Kind) IsKnown() bool {
	for _, known := range KnownKinds {
		if k == known {
			return true
		}
	}
	return false
}

// String renders the kind's name.
func (k Kind) String() string {
	return string(k)
}

// Metadata is a document's identity block: its name (required), an optional
// description, the F-29 key/value labels, and free-form annotations. The
// annotations are used only for UI searchability — they never affect
// behavior. Everything else in a document lives under its spec.
type Metadata struct {
	// Name is the document's name; it is required.
	Name string
	// Description is a human-readable description; optional.
	Description string
	// Labels are the document's F-29 key/value labels (the file's `labels`
	// map, renamed from `tags` in F-21.7).
	Labels map[string]string
	// Annotations are free-form key/value pairs used only for UI
	// searchability; they never affect behavior.
	Annotations map[string]string
}

// Document is a parsed, envelope-validated pipeline definition: the
// Kubernetes-style manifest (apiVersion, kind, metadata) plus the spec, which
// holds everything else. The spec is kept as an ordered Value (a map whose
// keys appear in file order) so the F-21.1.x sub-features can walk it
// deterministically; the envelope is the only part F-21.1.1 interprets.
type Document struct {
	// APIVersion is the document's apiVersion (always APIVersion after
	// validation).
	APIVersion string
	// Kind is what the document is; the compiler dispatches on it.
	Kind Kind
	// Metadata is the document's identity block.
	Metadata Metadata
	// Spec is the definition proper: everything under the document's spec
	// key, as an ordered map (empty when the document has no spec).
	Spec Value
}

// Error is an actionable validation error: it names the offending file, the
// key (a dotted path into the document), and the reason, so a reader can find
// and fix the problem. A definition that fails validation is rejected with
// one or more Errors; Parse returns them joined (all of them, not just the
// first, so a single pass surfaces every problem).
type Error struct {
	// File is the name of the offending file (empty when the definition was
	// parsed from a string with no name).
	File string
	// Key is the dotted path of the offending key (e.g.
	// "spec.jobs.deploy-dev.steps.build.shell"); empty for a document-level
	// problem (a bad apiVersion, a missing metadata.name).
	Key string
	// Reason is a human-readable description of the problem.
	Reason string
}

// Error renders the actionable message: "<file>:<key>: <reason>" (the file
// and key are omitted when empty).
func (e Error) Error() string {
	var b strings.Builder
	if e.File != "" {
		b.WriteString(e.File)
	}
	if e.Key != "" {
		if b.Len() > 0 {
			b.WriteString(":")
		}
		b.WriteString(e.Key)
	}
	if b.Len() > 0 {
		b.WriteString(": ")
	}
	b.WriteString(e.Reason)
	return b.String()
}

// Errors is a set of validation errors; it is returned by Parse when the
// definition is invalid.
type Errors []Error

// Error joins the set into a single message (one line per error).
func (e Errors) Error() string {
	parts := make([]string, len(e))
	for i, err := range e {
		parts[i] = err.Error()
	}
	return strings.Join(parts, "; ")
}

// Parse parses a pipeline definition (YAML or its JSON equivalent) and
// validates its manifest envelope (F-21.1.1): the apiVersion is the known
// schema version, the kind is one of the known kinds, the metadata carries a
// non-empty name, and the document is a mapping with no unexpected top-level
// keys. The YAML is decoded preserving key order, so a map (steps, jobs)
// yields a deterministic ordered list. On success the returned Document's
// Spec holds everything under the document's spec key, in file order.
//
// name is the file's name, carried into any validation errors so they name
// the offending file; it may be empty.
func Parse(name string, data []byte) (*Document, error) {
	var root yaml.Node
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil, Errors{Error{File: name, Reason: "invalid YAML: " + err.Error()}}
	}
	if len(root.Content) == 0 {
		return nil, Errors{Error{File: name, Reason: "the document is empty: a definition needs an apiVersion, a kind, a metadata block, and a spec"}}
	}
	docNode := root.Content[0]
	if docNode.Kind != yaml.MappingNode {
		return nil, Errors{Error{File: name, Reason: "the document must be a mapping with an apiVersion, a kind, a metadata block, and a spec"}}
	}

	var errs Errors
	rootVal, err := decodeValue(docNode)
	if err != nil {
		return nil, Errors{Error{File: name, Reason: err.Error()}}
	}

	// apiVersion: required, and must be the known schema version.
	apiVersion, ok := rootVal.StringValue("apiVersion")
	if !ok {
		errs = append(errs, Error{File: name, Key: "apiVersion", Reason: "apiVersion is required"})
	} else if apiVersion != APIVersion {
		errs = append(errs, Error{File: name, Key: "apiVersion",
			Reason: fmt.Sprintf("unknown apiVersion %q (the only supported version is %s)", apiVersion, APIVersion)})
	}

	// kind: required, and must be a known kind.
	kindStr, ok := rootVal.StringValue("kind")
	if !ok {
		errs = append(errs, Error{File: name, Key: "kind", Reason: "kind is required"})
	} else {
		kind := Kind(kindStr)
		if !kind.IsKnown() {
			errs = append(errs, Error{File: name, Key: "kind",
				Reason: fmt.Sprintf("unknown kind %q (a document is one of: %s)", kindStr, strings.Join(kindNames(KnownKinds), ", "))})
		}
	}

	// metadata: required, a mapping, with a non-empty name.
	metaVal, metaOK := rootVal.Get("metadata")
	if !metaOK {
		errs = append(errs, Error{File: name, Key: "metadata", Reason: "metadata is required"})
	} else if metaVal.Type != Map {
		errs = append(errs, Error{File: name, Key: "metadata", Reason: "metadata must be a mapping (name, description, labels, annotations)"})
	} else {
		name, nameOK := metaVal.StringValue("name")
		if !nameOK {
			errs = append(errs, Error{File: name, Key: "metadata.name", Reason: "metadata.name is required"})
		} else if strings.TrimSpace(name) == "" {
			errs = append(errs, Error{File: name, Key: "metadata.name", Reason: "metadata.name must not be empty"})
		}
	}

	// spec: optional (a VariableSet / Step / Job document may omit it); when
	// present it must be a mapping. A document with no spec gets an empty map,
	// so the sub-features can always treat Spec as an ordered map.
	spec := Value{Type: Map}
	if specVal, ok := rootVal.Get("spec"); ok {
		if specVal.Type != Map {
			errs = append(errs, Error{File: name, Key: "spec", Reason: "spec must be a mapping"})
		} else {
			spec = specVal
		}
	}

	// No unexpected top-level keys: apiVersion, kind, metadata, and spec are
	// the only keys a document has.
	for _, e := range rootVal.Entries {
		switch e.Key {
		case "apiVersion", "kind", "metadata", "spec":
		default:
			errs = append(errs, Error{File: name, Key: e.Key,
				Reason: fmt.Sprintf("unknown top-level key %q (a document has only apiVersion, kind, metadata, and spec)", e.Key)})
		}
	}

	if len(errs) > 0 {
		return nil, errs
	}

	doc := &Document{
		APIVersion: apiVersion,
		Kind:       Kind(kindStr),
		Spec:       spec,
	}
	if metaVal.Type == Map {
		doc.Metadata.Name, _ = metaVal.StringValue("name")
		doc.Metadata.Description, _ = metaVal.StringValue("description")
		if labels, ok := metaVal.StringMap("labels"); ok {
			doc.Metadata.Labels = labels
		}
		if annotations, ok := metaVal.StringMap("annotations"); ok {
			doc.Metadata.Annotations = annotations
		}
	}
	return doc, nil
}

// ParseFile is Parse for a named file: it reads the file and parses it,
// carrying the file's name into any validation errors.
func ParseFile(path string) (*Document, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, Errors{Error{File: path, Reason: "cannot read file: " + err.Error()}}
	}
	return Parse(path, data)
}

// kindNames renders the given kinds as a list of their string names.
func kindNames(kinds []Kind) []string {
	out := make([]string, len(kinds))
	for i, k := range kinds {
		out[i] = string(k)
	}
	return out
}
