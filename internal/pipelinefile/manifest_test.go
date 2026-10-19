package pipelinefile

import (
	"os"
	"strings"
	"testing"
)

// A minimal, valid Pipeline document: the manifest envelope with an empty
// spec. It is the baseline the negative tests mutate.
const minimalPipeline = `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: webapp
  description: Build, test, and deploy the web app
  labels:
    team: platform
    tier: "1"
  annotations:
    owner: platform-team
    review: required
spec:
  workerGroup: webapp-workers
`

func TestParseValidManifest(t *testing.T) {
	doc, err := Parse("webapp.yaml", []byte(minimalPipeline))
	if err != nil {
		t.Fatalf("Parse returned an error for a valid manifest: %v", err)
	}
	if doc.APIVersion != APIVersion {
		t.Errorf("APIVersion = %q, want %q", doc.APIVersion, APIVersion)
	}
	if doc.Kind != KindPipeline {
		t.Errorf("Kind = %q, want %q", doc.Kind, KindPipeline)
	}
	if doc.Metadata.Name != "webapp" {
		t.Errorf("Metadata.Name = %q, want %q", doc.Metadata.Name, "webapp")
	}
	if doc.Metadata.Description != "Build, test, and deploy the web app" {
		t.Errorf("Metadata.Description = %q, want the description", doc.Metadata.Description)
	}
	if got := doc.Metadata.Labels["team"]; got != "platform" {
		t.Errorf("Metadata.Labels[team] = %q, want %q", got, "platform")
	}
	if got := doc.Metadata.Labels["tier"]; got != "1" {
		t.Errorf("Metadata.Labels[tier] = %q, want %q", got, "1")
	}
	if got := doc.Metadata.Annotations["owner"]; got != "platform-team" {
		t.Errorf("Metadata.Annotations[owner] = %q, want %q", got, "platform-team")
	}
	if got := doc.Metadata.Annotations["review"]; got != "required" {
		t.Errorf("Metadata.Annotations[review] = %q, want %q", got, "required")
	}
	// Everything else lives under spec.
	if wg, ok := doc.Spec.StringValue("workerGroup"); !ok || wg != "webapp-workers" {
		t.Errorf("Spec.workerGroup = %q (ok=%v), want webapp-workers", wg, ok)
	}
}

// TestParseDispatchesOnKind verifies the compiler recognizes each of the four
// kinds and dispatches on it (acceptance criterion: the compiler dispatches on
// kind to know how to interpret a document).
func TestParseDispatchesOnKind(t *testing.T) {
	cases := []struct {
		kind Kind
		doc  string
	}{
		{KindPipeline, "apiVersion: cdrom/alphav1\nkind: Pipeline\nmetadata:\n  name: p\n"},
		{KindVariableSet, "apiVersion: cdrom/alphav1\nkind: VariableSet\nmetadata:\n  name: vs\nspec:\n  variables:\n    a: b\n"},
		{KindStep, "apiVersion: cdrom/alphav1\nkind: Step\nmetadata:\n  name: s\nspec:\n  shell: echo hi\n"},
		{KindJob, "apiVersion: cdrom/alphav1\nkind: Job\nmetadata:\n  name: j\nspec:\n  steps:\n    a:\n      shell: echo hi\n"},
	}
	for _, c := range cases {
		t.Run(string(c.kind), func(t *testing.T) {
			doc, err := Parse("doc.yaml", []byte(c.doc))
			if err != nil {
				t.Fatalf("Parse(%s) returned an error: %v", c.kind, err)
			}
			if doc.Kind != c.kind {
				t.Errorf("Kind = %q, want %q", doc.Kind, c.kind)
			}
		})
	}
}

// TestParseUnknownKind verifies an unknown kind is a validation error.
func TestParseUnknownKind(t *testing.T) {
	_, err := Parse("doc.yaml", []byte("apiVersion: cdrom/alphav1\nkind: Widget\nmetadata:\n  name: w\n"))
	if err == nil {
		t.Fatal("Parse accepted an unknown kind; want a validation error")
	}
	if !strings.Contains(err.Error(), "kind") {
		t.Errorf("error %q does not name the offending key (kind)", err)
	}
	if !strings.Contains(err.Error(), "Widget") {
		t.Errorf("error %q does not name the offending value (Widget)", err)
	}
}

// TestParseUnknownAPIVersion verifies an unknown apiVersion is a validation
// error (acceptance criterion: an unknown apiVersion is a validation error).
func TestParseUnknownAPIVersion(t *testing.T) {
	_, err := Parse("doc.yaml", []byte("apiVersion: cdrom/betav9\nkind: Pipeline\nmetadata:\n  name: p\n"))
	if err == nil {
		t.Fatal("Parse accepted an unknown apiVersion; want a validation error")
	}
	if !strings.Contains(err.Error(), "apiVersion") {
		t.Errorf("error %q does not name the offending key (apiVersion)", err)
	}
	if !strings.Contains(err.Error(), "cdrom/betav9") {
		t.Errorf("error %q does not name the offending value (cdrom/betav9)", err)
	}
}

// TestParseMissingAPIVersion verifies a document without an apiVersion is
// rejected.
func TestParseMissingAPIVersion(t *testing.T) {
	_, err := Parse("doc.yaml", []byte("kind: Pipeline\nmetadata:\n  name: p\n"))
	if err == nil {
		t.Fatal("Parse accepted a document with no apiVersion; want a validation error")
	}
	if !strings.Contains(err.Error(), "apiVersion") {
		t.Errorf("error %q does not name the offending key (apiVersion)", err)
	}
}

// TestParseMissingKind verifies a document without a kind is rejected.
func TestParseMissingKind(t *testing.T) {
	_, err := Parse("doc.yaml", []byte("apiVersion: cdrom/alphav1\nmetadata:\n  name: p\n"))
	if err == nil {
		t.Fatal("Parse accepted a document with no kind; want a validation error")
	}
	if !strings.Contains(err.Error(), "kind") {
		t.Errorf("error %q does not name the offending key (kind)", err)
	}
}

// TestParseMissingMetadata verifies a document without a metadata block is
// rejected.
func TestParseMissingMetadata(t *testing.T) {
	_, err := Parse("doc.yaml", []byte("apiVersion: cdrom/alphav1\nkind: Pipeline\n"))
	if err == nil {
		t.Fatal("Parse accepted a document with no metadata; want a validation error")
	}
	if !strings.Contains(err.Error(), "metadata") {
		t.Errorf("error %q does not name the offending key (metadata)", err)
	}
}

// TestParseMissingName verifies a metadata block without a name is rejected
// (acceptance criterion: metadata carries name, required).
func TestParseMissingName(t *testing.T) {
	_, err := Parse("doc.yaml", []byte("apiVersion: cdrom/alphav1\nkind: Pipeline\nmetadata:\n  description: no name here\n"))
	if err == nil {
		t.Fatal("Parse accepted a document whose metadata has no name; want a validation error")
	}
	if !strings.Contains(err.Error(), "metadata.name") {
		t.Errorf("error %q does not name the offending key (metadata.name)", err)
	}
}

// TestParseEmptyName verifies a metadata block with an empty name is rejected.
func TestParseEmptyName(t *testing.T) {
	_, err := Parse("doc.yaml", []byte("apiVersion: cdrom/alphav1\nkind: Pipeline\nmetadata:\n  name: \"\"\n"))
	if err == nil {
		t.Fatal("Parse accepted a document whose metadata.name is empty; want a validation error")
	}
	if !strings.Contains(err.Error(), "metadata.name") {
		t.Errorf("error %q does not name the offending key (metadata.name)", err)
	}
}

// TestParseUnknownTopLevelKey verifies a document with an unexpected
// top-level key is rejected (everything else lives under spec).
func TestParseUnknownTopLevelKey(t *testing.T) {
	_, err := Parse("doc.yaml", []byte("apiVersion: cdrom/alphav1\nkind: Pipeline\nmetadata:\n  name: p\nspec: {}\nfoo: bar\n"))
	if err == nil {
		t.Fatal("Parse accepted a document with an unknown top-level key; want a validation error")
	}
	if !strings.Contains(err.Error(), "foo") {
		t.Errorf("error %q does not name the offending key (foo)", err)
	}
}

// TestParseNonMapping verifies a document that is not a mapping is rejected.
func TestParseNonMapping(t *testing.T) {
	_, err := Parse("doc.yaml", []byte("- a\n- b\n"))
	if err == nil {
		t.Fatal("Parse accepted a document that is not a mapping; want a validation error")
	}
}

// TestParseEmptyDocument verifies an empty document is rejected.
func TestParseEmptyDocument(t *testing.T) {
	_, err := Parse("doc.yaml", []byte(""))
	if err == nil {
		t.Fatal("Parse accepted an empty document; want a validation error")
	}
}

// TestParseInvalidYAML verifies a document that is not valid YAML is rejected
// with an error naming the file.
func TestParseInvalidYAML(t *testing.T) {
	_, err := Parse("broken.yaml", []byte("apiVersion: [unclosed"))
	if err == nil {
		t.Fatal("Parse accepted invalid YAML; want a validation error")
	}
	if !strings.Contains(err.Error(), "broken.yaml") {
		t.Errorf("error %q does not name the offending file (broken.yaml)", err)
	}
}

// TestParseErrorNamesFileAndKey verifies an actionable error names the
// offending file, key, and reason (acceptance criterion).
func TestParseErrorNamesFileAndKey(t *testing.T) {
	_, err := Parse("pipelines/webapp.yaml", []byte("apiVersion: nope\nkind: Pipeline\nmetadata:\n  name: p\n"))
	if err == nil {
		t.Fatal("Parse accepted an unknown apiVersion; want a validation error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "pipelines/webapp.yaml") {
		t.Errorf("error %q does not name the offending file", msg)
	}
	if !strings.Contains(msg, "apiVersion") {
		t.Errorf("error %q does not name the offending key", msg)
	}
	if !strings.Contains(msg, "unknown apiVersion") {
		t.Errorf("error %q does not state the reason", msg)
	}
}

// TestParseMultipleErrors verifies Parse reports every envelope problem in a
// single pass, not just the first.
func TestParseMultipleErrors(t *testing.T) {
	// No apiVersion, unknown kind, and no metadata: three distinct problems.
	_, err := Parse("doc.yaml", []byte("kind: Widget\n"))
	if err == nil {
		t.Fatal("Parse accepted a document with several problems; want validation errors")
	}
	msg := err.Error()
	for _, want := range []string{"apiVersion", "kind", "metadata"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %q does not mention %q", msg, want)
		}
	}
}

// TestParsePreservesKeyOrder verifies the YAML is parsed preserving key order,
// so a map (steps, jobs) yields a deterministic ordered list (acceptance
// criterion). The spec's keys appear in the same order they appear in the
// file, and a nested map's keys do too.
func TestParsePreservesKeyOrder(t *testing.T) {
	doc, err := Parse("doc.yaml", []byte(`
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: p
spec:
  zebra: 1
  apple: 2
  mango: 3
  jobs:
    deploy-prod:
      environment: prod
    build-test:
      environment: dev
    deploy-dev:
      environment: dev
`))
	if err != nil {
		t.Fatalf("Parse returned an error: %v", err)
	}
	// The top-level spec keys are in file order: zebra, apple, mango, jobs.
	var specKeys []string
	for _, e := range doc.Spec.Entries {
		specKeys = append(specKeys, e.Key)
	}
	wantSpec := []string{"zebra", "apple", "mango", "jobs"}
	if strings.Join(specKeys, ",") != strings.Join(wantSpec, ",") {
		t.Errorf("spec keys = %v, want %v (key order must be preserved)", specKeys, wantSpec)
	}
	// The jobs map's keys are in file order: deploy-prod, build-test,
	// deploy-dev — not alphabetical.
	jobs, ok := doc.Spec.Get("jobs")
	if !ok {
		t.Fatal("spec has no jobs entry")
	}
	var jobKeys []string
	for _, e := range jobs.Entries {
		jobKeys = append(jobKeys, e.Key)
	}
	wantJobs := []string{"deploy-prod", "build-test", "deploy-dev"}
	if strings.Join(jobKeys, ",") != strings.Join(wantJobs, ",") {
		t.Errorf("job keys = %v, want %v (key order must be preserved)", jobKeys, wantJobs)
	}
}

// TestParseCamelCaseKeys verifies the file's mapping keys are camelCase and
// the compiler reads them as such (acceptance criterion). The complete
// example's camelCase keys (concurrencyKeys, variableSets, secretSources,
// workerGroup, failureMode) are recognized.
func TestParseCamelCaseKeys(t *testing.T) {
	doc, err := Parse("doc.yaml", []byte(`
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: p
spec:
  workerGroup: webapp-workers
  failureMode: all
  concurrencyKeys:
    - webapp-prod
  variableSets:
    global:
      variables:
        var1: value1
  secretSources:
    prod-secrets:
      kind: Vault
      path: /secret/prod
      audience: cdrom
`))
	if err != nil {
		t.Fatalf("Parse returned an error: %v", err)
	}
	if wg, ok := doc.Spec.StringValue("workerGroup"); !ok || wg != "webapp-workers" {
		t.Errorf("spec.workerGroup = %q (ok=%v), want webapp-workers", wg, ok)
	}
	if fm, ok := doc.Spec.StringValue("failureMode"); !ok || fm != "all" {
		t.Errorf("spec.failureMode = %q (ok=%v), want all", fm, ok)
	}
	if keys, ok := doc.Spec.StringSlice("concurrencyKeys"); !ok || len(keys) != 1 || keys[0] != "webapp-prod" {
		t.Errorf("spec.concurrencyKeys = %v (ok=%v), want [webapp-prod]", keys, ok)
	}
	vs, ok := doc.Spec.Get("variableSets")
	if !ok {
		t.Fatal("spec has no variableSets entry")
	}
	global, ok := vs.Get("global")
	if !ok {
		t.Fatal("variableSets has no global entry")
	}
	vars, ok := global.Get("variables")
	if !ok {
		t.Fatal("global has no variables entry")
	}
	if v, ok := vars.StringValue("var1"); !ok || v != "value1" {
		t.Errorf("variables.var1 = %q (ok=%v), want value1", v, ok)
	}
	ss, ok := doc.Spec.Get("secretSources")
	if !ok {
		t.Fatal("spec has no secretSources entry")
	}
	prod, ok := ss.Get("prod-secrets")
	if !ok {
		t.Fatal("secretSources has no prod-secrets entry")
	}
	if kind, ok := prod.StringValue("kind"); !ok || kind != "Vault" {
		t.Errorf("secretSources.prod-secrets.kind = %q (ok=%v), want Vault", kind, ok)
	}
}

// TestParseSpecOptional verifies a document may omit its spec (a VariableSet /
// Step / Job document's spec is interpreted by the sub-features; the envelope
// does not require it).
func TestParseSpecOptional(t *testing.T) {
	doc, err := Parse("doc.yaml", []byte("apiVersion: cdrom/alphav1\nkind: Step\nmetadata:\n  name: s\n"))
	if err != nil {
		t.Fatalf("Parse returned an error for a document with no spec: %v", err)
	}
	if doc.Spec.Type != Map || len(doc.Spec.Entries) != 0 {
		t.Errorf("Spec = %+v, want an empty map", doc.Spec)
	}
}

// TestParseSpecMustBeMapping verifies a spec that is not a mapping is
// rejected.
func TestParseSpecMustBeMapping(t *testing.T) {
	_, err := Parse("doc.yaml", []byte("apiVersion: cdrom/alphav1\nkind: Pipeline\nmetadata:\n  name: p\nspec: just-a-string\n"))
	if err == nil {
		t.Fatal("Parse accepted a spec that is not a mapping; want a validation error")
	}
	if !strings.Contains(err.Error(), "spec") {
		t.Errorf("error %q does not name the offending key (spec)", err)
	}
}

// TestParseMetadataMustBeMapping verifies a metadata block that is not a
// mapping is rejected.
func TestParseMetadataMustBeMapping(t *testing.T) {
	_, err := Parse("doc.yaml", []byte("apiVersion: cdrom/alphav1\nkind: Pipeline\nmetadata: just-a-string\n"))
	if err == nil {
		t.Fatal("Parse accepted a metadata block that is not a mapping; want a validation error")
	}
	if !strings.Contains(err.Error(), "metadata") {
		t.Errorf("error %q does not name the offending key (metadata)", err)
	}
}

// TestParseFile verifies ParseFile reads a named file and carries its name
// into validation errors.
func TestParseFile(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/webapp.yaml"
	if err := os.WriteFile(path, []byte(minimalPipeline), 0o644); err != nil {
		t.Fatal(err)
	}
	doc, err := ParseFile(path)
	if err != nil {
		t.Fatalf("ParseFile returned an error for a valid file: %v", err)
	}
	if doc.Metadata.Name != "webapp" {
		t.Errorf("Metadata.Name = %q, want webapp", doc.Metadata.Name)
	}

	// A file with an invalid envelope names the file in its error.
	badPath := dir + "/bad.yaml"
	if err := os.WriteFile(badPath, []byte("apiVersion: nope\nkind: Pipeline\nmetadata:\n  name: p\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = ParseFile(badPath)
	if err == nil {
		t.Fatal("ParseFile accepted an invalid file; want a validation error")
	}
	if !strings.Contains(err.Error(), "bad.yaml") {
		t.Errorf("error %q does not name the offending file (bad.yaml)", err)
	}
}

// TestParseFileMissing verifies ParseFile reports a readable error for a file
// that does not exist.
func TestParseFileMissing(t *testing.T) {
	_, err := ParseFile("/nonexistent/path/does-not-exist.yaml")
	if err == nil {
		t.Fatal("ParseFile accepted a missing file; want an error")
	}
}

// TestParseJSONEquivalent verifies the compiler accepts the JSON equivalent of
// the file format (a definition in YAML or its JSON equivalent).
func TestParseJSONEquivalent(t *testing.T) {
	doc, err := Parse("doc.json", []byte(`{
  "apiVersion": "cdrom/alphav1",
  "kind": "Pipeline",
  "metadata": {"name": "p", "labels": {"team": "platform"}},
  "spec": {"workerGroup": "webapp-workers"}
}`))
	if err != nil {
		t.Fatalf("Parse returned an error for a JSON definition: %v", err)
	}
	if doc.Kind != KindPipeline {
		t.Errorf("Kind = %q, want Pipeline", doc.Kind)
	}
	if doc.Metadata.Name != "p" {
		t.Errorf("Metadata.Name = %q, want p", doc.Metadata.Name)
	}
	if wg, ok := doc.Spec.StringValue("workerGroup"); !ok || wg != "webapp-workers" {
		t.Errorf("Spec.workerGroup = %q (ok=%v), want webapp-workers", wg, ok)
	}
}

// TestValueHelpers exercises the Value accessors the sub-features will use to
// walk a document's spec.
func TestValueHelpers(t *testing.T) {
	v := Value{Type: Map, Entries: []Entry{
		{Key: "name", Value: Value{Type: Scalar, String: "p"}},
		{Key: "count", Value: Value{Type: Scalar, String: "3"}},
		{Key: "items", Value: Value{Type: List, Items: []Value{
			{Type: Scalar, String: "a"},
			{Type: Scalar, String: "b"},
		}}},
		{Key: "env", Value: Value{Type: Map, Entries: []Entry{
			{Key: "CI", Value: Value{Type: Scalar, String: "true"}},
		}}},
	}}
	if s, ok := v.StringValue("name"); !ok || s != "p" {
		t.Errorf("StringValue(name) = %q (ok=%v), want p", s, ok)
	}
	if _, ok := v.StringValue("items"); ok {
		t.Error("StringValue(items) reported ok for a list; want false")
	}
	if items, ok := v.StringSlice("items"); !ok || len(items) != 2 || items[0] != "a" || items[1] != "b" {
		t.Errorf("StringSlice(items) = %v (ok=%v), want [a b]", items, ok)
	}
	if env, ok := v.StringMap("env"); !ok || env["CI"] != "true" {
		t.Errorf("StringMap(env) = %v (ok=%v), want map[CI:true]", env, ok)
	}
	if m, ok := v.MapValue("env"); !ok || m.Type != Map {
		t.Errorf("MapValue(env) = %+v (ok=%v), want a map", m, ok)
	}
	if !v.Has("name") {
		t.Error("Has(name) = false, want true")
	}
	if v.Has("missing") {
		t.Error("Has(missing) = true, want false")
	}
}

// TestPath verifies Path rendering (the "key" an actionable error names).
func TestPath(t *testing.T) {
	p := NewPath("spec", "jobs", "deploy-dev", "steps", "build", "shell")
	if got := p.String(); got != "spec.jobs.deploy-dev.steps.build.shell" {
		t.Errorf("Path.String() = %q, want spec.jobs.deploy-dev.steps.build.shell", got)
	}
	q := p.Extend("command")
	if got := q.String(); got != "spec.jobs.deploy-dev.steps.build.shell.command" {
		t.Errorf("Path.Extend(command).String() = %q, want ...shell.command", got)
	}
	if NewPath().String() != "" {
		t.Error("an empty Path should render as the empty string")
	}
}

// TestParseCompleteExample verifies the complete example from the format
// reference (docs/features/f-21.1-file-format-compiler.md) parses: the
// manifest envelope validates, the kind dispatches to Pipeline, and the spec's
// camelCase keys (workerGroup, steps, variableSets, secretSources, jobs,
// failureMode, concurrencyKeys, params, triggers) are all present and in file
// order.
func TestParseCompleteExample(t *testing.T) {
	const example = `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: webapp
  description: Build, test, and deploy the web app
  labels:
    team: platform
    tier: "1"
  annotations:
    owner: platform-team
    review: required
spec:
  workerGroup: webapp-workers
  steps:
    step1:
      shell: echo "Yes"
    build:
      shell:
        command: make build
      workdir: /src
      timeout: 10m
      env:
        GOFLAGS: -mod=vendor
    full-shell:
      shell:
        command: "make build --verbose"
        args: ["-c"]
        shell: sh
      workdir: /src
      env:
        GOFLAGS: -mod=vendor
        CI: "true"
      timeout: 10m
      condition: '{{ .params.environment == "prod" }}'
      ignoreFailed: true
      once: true
    approve-prod:
      approval:
        message: 'Deploy {{ .params.image_tag }} to {{ .params.environment }}?'
      timeout: 1h
      condition: '{{ .params.environment == "prod" }}'
      ignoreFailed: false
      once: true
    mint-registry-token:
      token_exchange:
        audience: container-registry
        expires_in: 15m
        output: registry_token
      timeout: 1m
      condition: '{{ .params.push == "true" }}'
      ignoreFailed: false
      once: true
    notify-team:
      email:
        to:
          - team@example.com
        cc:
          - leads@example.com
        bcc:
          - archive@example.com
        subject: 'Deployed {{ .params.image_tag }} to {{ .params.environment }}'
        body: |
          Deployed ` + "`{{ .params.image_tag }}`" + ` to ` + "`{{ .params.environment }}`" + `
          (build ` + "`{{ index .steps \"build\" .Outputs.version }}`" + `).
        content_type: markdown
        from: cdrom@example.com
        reply_to: oncall@example.com
      timeout: 30s
      condition: '{{ .params.environment == "prod" }}'
      ignoreFailed: true
      once: true
    notify-slack:
      webhook:
        url: https://hooks.example.com/services/T000/B000/XXXX
        method: POST
        headers:
          - "Content-Type: application/json"
          - "X-Run-Id: {{ .run.ID }}"
        body: |
          {"text": "Deployed {{ .params.image_tag }} to {{ .params.environment }}"}
        content_type: plain
        timeout: 10s
      timeout: 30s
      condition: '{{ .params.environment == "prod" }}'
      ignoreFailed: true
      once: true
  variableSets:
    global:
      variables:
        var1: value1
        var2: value2
        region:
          encrypted: <encrypted>
        deploy_token:
          secretSource: prod-secrets
          key: deploy_token
  secretSources:
    prod-secrets:
      kind: Vault
      path: /secret/prod
      audience: cdrom
  jobs:
    build-test:
      steps:
        a-build:
          ref: build
        lint:
          shell:
            command: make
            args: ["lint"]
          workdir: /src
          timeout: 5m
    deploy-dev:
      needs: [build-test]
      environment: dev
      workerGroup: webapp-workers
      variables:
        canary: "true"
        deploy_token:
          secretSource: prod-secrets
          key: deploy_token
        api_key:
          encrypted: <encrypted>
      variableSets:
        - global
    deploy-prod:
      needs: [deploy-dev]
      environment: prod
      workerGroup: prod-workers
      variables: {}
      variableSets:
        - global
      concurrencyKeys: ["webapp-prod"]
      steps:
        step1:
  failureMode: all
  concurrencyKeys: ["webapp-{{ .params.environment }}"]
  params:
    - name: image_tag
      description: The image tag to deploy
    - name: environment
      description: The target environment (dev / staging / prod)
    - name: push
      description: Whether to push the built image
      default: "false"
  triggers:
    - name: nightly
      type: cron
      cron: "0 2 * * *"
`
	doc, err := Parse("pipelines/webapp.yaml", []byte(example))
	if err != nil {
		t.Fatalf("Parse returned an error for the complete example: %v", err)
	}
	if doc.Kind != KindPipeline {
		t.Errorf("Kind = %q, want Pipeline", doc.Kind)
	}
	if doc.Metadata.Name != "webapp" {
		t.Errorf("Metadata.Name = %q, want webapp", doc.Metadata.Name)
	}
	// The spec's top-level keys are in file order.
	var specKeys []string
	for _, e := range doc.Spec.Entries {
		specKeys = append(specKeys, e.Key)
	}
	wantSpec := []string{"workerGroup", "steps", "variableSets", "secretSources", "jobs", "failureMode", "concurrencyKeys", "params", "triggers"}
	if strings.Join(specKeys, ",") != strings.Join(wantSpec, ",") {
		t.Errorf("spec keys = %v, want %v", specKeys, wantSpec)
	}
	// The steps map's keys are in file order (not alphabetical).
	steps, _ := doc.Spec.Get("steps")
	var stepKeys []string
	for _, e := range steps.Entries {
		stepKeys = append(stepKeys, e.Key)
	}
	wantSteps := []string{"step1", "build", "full-shell", "approve-prod", "mint-registry-token", "notify-team", "notify-slack"}
	if strings.Join(stepKeys, ",") != strings.Join(wantSteps, ",") {
		t.Errorf("step keys = %v, want %v", stepKeys, wantSteps)
	}
	// A step's handler-type key and common fields are present.
	build, _ := steps.Get("build")
	if shell, ok := build.Get("shell"); !ok || shell.Type != Map {
		t.Error("build step has no shell handler map")
	} else if cmd, ok := shell.StringValue("command"); !ok || cmd != "make build" {
		t.Errorf("build.shell.command = %q (ok=%v), want 'make build'", cmd, ok)
	}
	if wd, ok := build.StringValue("workdir"); !ok || wd != "/src" {
		t.Errorf("build.workdir = %q (ok=%v), want /src", wd, ok)
	}
	// A job's deployment options are present.
	jobs, _ := doc.Spec.Get("jobs")
	deployProd, _ := jobs.Get("deploy-prod")
	if env, ok := deployProd.StringValue("environment"); !ok || env != "prod" {
		t.Errorf("deploy-prod.environment = %q (ok=%v), want prod", env, ok)
	}
	if wg, ok := deployProd.StringValue("workerGroup"); !ok || wg != "prod-workers" {
		t.Errorf("deploy-prod.workerGroup = %q (ok=%v), want prod-workers", wg, ok)
	}
	if keys, ok := deployProd.StringSlice("concurrencyKeys"); !ok || len(keys) != 1 || keys[0] != "webapp-prod" {
		t.Errorf("deploy-prod.concurrencyKeys = %v (ok=%v), want [webapp-prod]", keys, ok)
	}
	// The pipeline-level concurrency keys are templatable.
	if keys, ok := doc.Spec.StringSlice("concurrencyKeys"); !ok || len(keys) != 1 || keys[0] != "webapp-{{ .params.environment }}" {
		t.Errorf("spec.concurrencyKeys = %v (ok=%v), want the templated key", keys, ok)
	}
}

// TestKindString verifies the document Kind and the ValueType renderers.
func TestKindString(t *testing.T) {
	if KindPipeline.String() != "Pipeline" {
		t.Errorf("KindPipeline.String() = %q, want Pipeline", KindPipeline.String())
	}
	if Scalar.String() != "scalar" {
		t.Errorf("Scalar.String() = %q, want scalar", Scalar.String())
	}
	if !KindPipeline.IsKnown() {
		t.Error("KindPipeline.IsKnown() = false, want true")
	}
	if Kind("Widget").IsKnown() {
		t.Error("Kind(Widget).IsKnown() = true, want false")
	}
}
