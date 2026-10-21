package pipelinefile

import (
	"strings"
	"testing"
)

// parseVariables parses a `variables` map (a name → value map) into a Value,
// so the tests can hand CompileVariables the same ordered Value the compiler
// produces. The yaml is the body of the variables map (indented under a
// `variables:` key in a Pipeline document).
func parseVariables(t *testing.T, yaml string) Value {
	t.Helper()
	doc, err := Parse("doc.yaml", []byte("apiVersion: cdrom/alphav1\nkind: Pipeline\nmetadata:\n  name: p\nspec:\n  variableSets:\n    s:\n      variables:\n"+indent(yaml, "        ")))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	sets, ok := doc.Spec.Get("variableSets")
	if !ok {
		t.Fatal("spec has no variableSets")
	}
	set, ok := sets.Get("s")
	if !ok {
		t.Fatal("variableSets has no s")
	}
	v, ok := set.Get("variables")
	if !ok {
		t.Fatal("set s has no variables")
	}
	return v
}

// TestCompileVariablesPlain verifies a plain variable (a scalar value) compiles
// to an unmasked VariablePlain value (acceptance criterion: a variable is
// `name: value` (a plain value)).
func TestCompileVariablesPlain(t *testing.T) {
	v := parseVariables(t, `
canary: "true"
region: us-east-1`)
	vars, errs := CompileVariables("doc.yaml", NewPath("spec", "variableSets", "s", "variables"), v)
	if len(errs) > 0 {
		t.Fatalf("CompileVariables returned errors: %v", errs)
	}
	if len(vars) != 2 {
		t.Fatalf("vars = %d, want 2", len(vars))
	}
	if vars[0].Name != "canary" || vars[0].Value.Shape != VariablePlain || vars[0].Value.Plain != "true" {
		t.Errorf("vars[0] = %+v, want canary/plain/true", vars[0])
	}
	if vars[0].Value.IsMasked() {
		t.Errorf("vars[0] (plain) is masked, want unmasked")
	}
	if vars[1].Name != "region" || vars[1].Value.Shape != VariablePlain || vars[1].Value.Plain != "us-east-1" {
		t.Errorf("vars[1] = %+v, want region/plain/us-east-1", vars[1])
	}
}

// TestCompileVariablesEncrypted verifies an encrypted variable (an
// {encrypted: <ciphertext>} map) compiles to a masked VariableEncrypted value
// (acceptance criterion: a variable is `name: {encrypted: <ciphertext>}` (a
// builtin F-12 ciphertext)).
func TestCompileVariablesEncrypted(t *testing.T) {
	v := parseVariables(t, `
api_key:
  encrypted: <encrypted>`)
	vars, errs := CompileVariables("doc.yaml", NewPath("spec", "variableSets", "s", "variables"), v)
	if len(errs) > 0 {
		t.Fatalf("CompileVariables returned errors: %v", errs)
	}
	if len(vars) != 1 {
		t.Fatalf("vars = %d, want 1", len(vars))
	}
	if vars[0].Name != "api_key" || vars[0].Value.Shape != VariableEncrypted || vars[0].Value.Encrypted != "<encrypted>" {
		t.Errorf("vars[0] = %+v, want api_key/encrypted/<encrypted>", vars[0])
	}
	if !vars[0].Value.IsMasked() {
		t.Errorf("vars[0] (encrypted) is not masked, want masked")
	}
}

// TestCompileVariablesSecretSource verifies a sourced variable (a
// {secretSource: <name>, key: <key>} map) compiles to a masked
// VariableSecretSource value (acceptance criterion: a variable is
// `name: {secretSource: <name>, key: <key>}` (a value from a named
// secretSources entry)).
func TestCompileVariablesSecretSource(t *testing.T) {
	v := parseVariables(t, `
deploy_token:
  secretSource: prod-secrets
  key: deploy_token`)
	vars, errs := CompileVariables("doc.yaml", NewPath("spec", "variableSets", "s", "variables"), v)
	if len(errs) > 0 {
		t.Fatalf("CompileVariables returned errors: %v", errs)
	}
	if len(vars) != 1 {
		t.Fatalf("vars = %d, want 1", len(vars))
	}
	if vars[0].Name != "deploy_token" || vars[0].Value.Shape != VariableSecretSource || vars[0].Value.SecretSource != "prod-secrets" || vars[0].Value.Key != "deploy_token" {
		t.Errorf("vars[0] = %+v, want deploy_token/secretSource/prod-secrets/deploy_token", vars[0])
	}
	if !vars[0].Value.IsMasked() {
		t.Errorf("vars[0] (sourced) is not masked, want masked")
	}
}

// TestCompileVariablesMixedShapes verifies a variables map mixing plain,
// encrypted, and sourced values compiles each to its shape, and that the
// masked ones are tracked (acceptance criterion: a variable is masked when its
// value is not a plain string, and the platform tracks which variables are
// masked).
func TestCompileVariablesMixedShapes(t *testing.T) {
	v := parseVariables(t, `
canary: "true"
region:
  encrypted: <encrypted>
deploy_token:
  secretSource: prod-secrets
  key: deploy_token`)
	vars, errs := CompileVariables("doc.yaml", NewPath("spec", "variableSets", "s", "variables"), v)
	if len(errs) > 0 {
		t.Fatalf("CompileVariables returned errors: %v", errs)
	}
	if len(vars) != 3 {
		t.Fatalf("vars = %d, want 3", len(vars))
	}
	// The platform tracks which variables are masked: exactly the two that are
	// not plain strings.
	masked := MaskedVariables(vars)
	if len(masked) != 2 {
		t.Fatalf("MaskedVariables = %d, want 2", len(masked))
	}
	if masked[0].Name != "region" || masked[1].Name != "deploy_token" {
		t.Errorf("MaskedVariables = %v, want [region deploy_token]", []string{masked[0].Name, masked[1].Name})
	}
	// The plain variable's value is the only one that lands in the run's
	// parameters (F-10); the masked ones are resolved at dispatch (F-12).
	params := PlainParams(vars)
	if len(params) != 1 || params["canary"] != "true" {
		t.Errorf("PlainParams = %v, want map[canary:true]", params)
	}
}

// TestCompileVariablesEncryptedNonScalar verifies an encrypted value that is
// not a scalar is a validation error.
func TestCompileVariablesEncryptedNonScalar(t *testing.T) {
	v := parseVariables(t, `
api_key:
  encrypted:
    nested: value`)
	_, errs := CompileVariables("doc.yaml", NewPath("spec", "variableSets", "s", "variables"), v)
	if len(errs) == 0 {
		t.Fatal("CompileVariables accepted a non-scalar encrypted value; want a validation error")
	}
	if !strings.Contains(errs[0].Error(), "encrypted") {
		t.Errorf("error %q does not name the offending key (encrypted)", errs[0].Error())
	}
}

// TestCompileVariablesBothShapes verifies a variable value that is both
// {encrypted: ...} and {secretSource: ...} is a validation error (a value has
// exactly one shape).
func TestCompileVariablesBothShapes(t *testing.T) {
	v := parseVariables(t, `
api_key:
  encrypted: <encrypted>
  secretSource: prod-secrets
  key: deploy_token`)
	_, errs := CompileVariables("doc.yaml", NewPath("spec", "variableSets", "s", "variables"), v)
	if len(errs) == 0 {
		t.Fatal("CompileVariables accepted a value with both encrypted and secretSource; want a validation error")
	}
}

// TestCompileVariablesMissingKey verifies a sourced variable without a key is a
// validation error (a sourced value selects the value within the source by
// key).
func TestCompileVariablesMissingKey(t *testing.T) {
	v := parseVariables(t, `
deploy_token:
  secretSource: prod-secrets`)
	_, errs := CompileVariables("doc.yaml", NewPath("spec", "variableSets", "s", "variables"), v)
	if len(errs) == 0 {
		t.Fatal("CompileVariables accepted a sourced variable without a key; want a validation error")
	}
	if !strings.Contains(errs[0].Error(), "key") {
		t.Errorf("error %q does not name the missing key", errs[0].Error())
	}
}

// TestCompileVariablesUnexpectedKey verifies a variable value map with an
// unrecognized key is a validation error.
func TestCompileVariablesUnexpectedKey(t *testing.T) {
	v := parseVariables(t, `
api_key:
  encrypted: <encrypted>
  bogus: x`)
	_, errs := CompileVariables("doc.yaml", NewPath("spec", "variableSets", "s", "variables"), v)
	if len(errs) == 0 {
		t.Fatal("CompileVariables accepted an unexpected key in a variable value; want a validation error")
	}
	if !strings.Contains(errs[0].Error(), "bogus") {
		t.Errorf("error %q does not name the offending key (bogus)", errs[0].Error())
	}
}

// TestCompileVariablesListValue verifies a variable value that is a list is a
// validation error (a variable's value is a scalar, an encrypted map, or a
// sourced map — not a list).
func TestCompileVariablesListValue(t *testing.T) {
	v := parseVariables(t, `
api_key:
  - a
  - b`)
	_, errs := CompileVariables("doc.yaml", NewPath("spec", "variableSets", "s", "variables"), v)
	if len(errs) == 0 {
		t.Fatal("CompileVariables accepted a list variable value; want a validation error")
	}
}

// TestCompileVariableSets verifies spec.variableSets (a name → set map) compiles
// to a name → VariableSet map, each set an inline variables map (acceptance
// criterion: variable sets are inline `variables` maps declared under
// spec.variableSets).
func TestCompileVariableSets(t *testing.T) {
	doc := parseDoc(t, `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: p
spec:
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
    other:
      variables:
        x: y`)
	setsVal, _ := doc.Spec.Get("variableSets")
	sets, errs := CompileVariableSets("doc.yaml", setsVal)
	if len(errs) > 0 {
		t.Fatalf("CompileVariableSets returned errors: %v", errs)
	}
	if len(sets) != 2 {
		t.Fatalf("sets = %d, want 2", len(sets))
	}
	global := sets["global"]
	if global == nil {
		t.Fatal("sets has no global")
	}
	if len(global.Variables) != 4 {
		t.Fatalf("global.Variables = %d, want 4", len(global.Variables))
	}
	// The set's variables carry their shapes (plain, encrypted, sourced).
	byName := map[string]Variable{}
	for _, v := range global.Variables {
		byName[v.Name] = v
	}
	if byName["var1"].Value.Shape != VariablePlain || byName["var1"].Value.Plain != "value1" {
		t.Errorf("global.var1 = %+v, want plain/value1", byName["var1"])
	}
	if byName["region"].Value.Shape != VariableEncrypted {
		t.Errorf("global.region = %+v, want encrypted", byName["region"])
	}
	if byName["deploy_token"].Value.Shape != VariableSecretSource || byName["deploy_token"].Value.SecretSource != "prod-secrets" {
		t.Errorf("global.deploy_token = %+v, want secretSource/prod-secrets", byName["deploy_token"])
	}
	if other := sets["other"]; other == nil || len(other.Variables) != 1 || other.Variables[0].Name != "x" {
		t.Errorf("other = %+v, want a set with one variable x", other)
	}
}

// TestCompileVariableSetsSecretSources verifies a variable set's own
// secretSources section (shaped like the pipeline's spec.secretSources,
// F-21.4) compiles into the set's SecretSources, so the set owns the sources
// its masked variables reference.
func TestCompileVariableSetsSecretSources(t *testing.T) {
	doc := parseDoc(t, `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: p
spec:
  variableSets:
    dev:
      variables:
        deploy_token:
          secretSource: prod-secrets
          key: deploy_token
      secretSources:
        prod-secrets:
          kind: Vault
          path: cdrom/dev
          audience: cdrom`)
	setsVal, _ := doc.Spec.Get("variableSets")
	sets, errs := CompileVariableSets("doc.yaml", setsVal)
	if len(errs) > 0 {
		t.Fatalf("CompileVariableSets returned errors: %v", errs)
	}
	dev := sets["dev"]
	if dev == nil {
		t.Fatal("sets has no dev")
	}
	if len(dev.SecretSources) != 1 {
		t.Fatalf("dev.SecretSources = %d, want 1", len(dev.SecretSources))
	}
	src := dev.SecretSources["prod-secrets"]
	if src == nil {
		t.Fatal("dev.SecretSources has no prod-secrets")
	}
	if src.Kind != "Vault" || src.Path != "cdrom/dev" || src.Audience != "cdrom" {
		t.Errorf("dev.SecretSources.prod-secrets = %+v, want Vault/cdrom-dev/cdrom", src)
	}
	// The set's masked variable references the set's own source.
	if len(dev.Variables) != 1 || dev.Variables[0].Value.SecretSource != "prod-secrets" {
		t.Errorf("dev.Variables = %+v, want one variable sourced from prod-secrets", dev.Variables)
	}
}

// TestCompileVariableSetsEmpty verifies a variable set with no variables key is
// an empty set (not an error).
func TestCompileVariableSetsEmpty(t *testing.T) {
	doc := parseDoc(t, `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: p
spec:
  variableSets:
    empty: {}`)
	setsVal, _ := doc.Spec.Get("variableSets")
	sets, errs := CompileVariableSets("doc.yaml", setsVal)
	if len(errs) > 0 {
		t.Fatalf("CompileVariableSets returned errors: %v", errs)
	}
	if s := sets["empty"]; s == nil || len(s.Variables) != 0 {
		t.Errorf("empty set = %+v, want an empty set", s)
	}
}

// TestCompileVariableSetsUnexpectedKey verifies a variable set with an
// unrecognized key is a validation error (a set has only a variables key;
// external-file / vault imports are F-21.4).
func TestCompileVariableSetsUnexpectedKey(t *testing.T) {
	doc := parseDoc(t, `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: p
spec:
  variableSets:
    s:
      variables:
        a: b
      import:
        - other.yaml`)
	setsVal, _ := doc.Spec.Get("variableSets")
	_, errs := CompileVariableSets("doc.yaml", setsVal)
	if len(errs) == 0 {
		t.Fatal("CompileVariableSets accepted an unexpected key in a variable set; want a validation error")
	}
	if !strings.Contains(errs[0].Error(), "import") {
		t.Errorf("error %q does not name the offending key (import)", errs[0].Error())
	}
}

// TestCompileVariableSetsNotMap verifies a variable set that is not a mapping
// is a validation error.
func TestCompileVariableSetsNotMap(t *testing.T) {
	doc := parseDoc(t, `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: p
spec:
  variableSets:
    s: just-a-string`)
	setsVal, _ := doc.Spec.Get("variableSets")
	_, errs := CompileVariableSets("doc.yaml", setsVal)
	if len(errs) == 0 {
		t.Fatal("CompileVariableSets accepted a non-mapping variable set; want a validation error")
	}
}

// TestCompileSecretSources verifies spec.secretSources (a name → source map)
// compiles to a name → SecretSource map, each with its kind, path, and
// audience (acceptance criterion: each entry has a kind, a path, and an
// audience).
func TestCompileSecretSources(t *testing.T) {
	doc := parseDoc(t, `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: p
spec:
  secretSources:
    prod-secrets:
      kind: Vault
      path: /secret/prod
      audience: cdrom
    bao:
      kind: OpenBao
      path: /secret/bao`)
	srcsVal, _ := doc.Spec.Get("secretSources")
	srcs, errs := CompileSecretSources("doc.yaml", srcsVal)
	if len(errs) > 0 {
		t.Fatalf("CompileSecretSources returned errors: %v", errs)
	}
	if len(srcs) != 2 {
		t.Fatalf("sources = %d, want 2", len(srcs))
	}
	prod := srcs["prod-secrets"]
	if prod == nil {
		t.Fatal("sources has no prod-secrets")
	}
	if prod.Kind != "Vault" || prod.Path != "/secret/prod" || prod.Audience != "cdrom" {
		t.Errorf("prod-secrets = %+v, want Vault//secret/prod/cdrom", prod)
	}
	bao := srcs["bao"]
	if bao == nil || bao.Kind != "OpenBao" || bao.Path != "/secret/bao" {
		t.Errorf("bao = %+v, want OpenBao//secret/bao", bao)
	}
}

// TestCompileSecretSourcesPartial verifies a secret source with only some of
// its keys (kind, path, audience are all optional) compiles, leaving the unset
// keys empty (an empty kind means the built-in AES store).
func TestCompileSecretSourcesPartial(t *testing.T) {
	doc := parseDoc(t, `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: p
spec:
  secretSources:
    builtin:
      path: /secret/builtin`)
	srcsVal, _ := doc.Spec.Get("secretSources")
	srcs, errs := CompileSecretSources("doc.yaml", srcsVal)
	if len(errs) > 0 {
		t.Fatalf("CompileSecretSources returned errors: %v", errs)
	}
	b := srcs["builtin"]
	if b == nil {
		t.Fatal("sources has no builtin")
	}
	if b.Kind != "" || b.Path != "/secret/builtin" || b.Audience != "" {
		t.Errorf("builtin = %+v, want empty kind, /secret/builtin path, empty audience", b)
	}
}

// TestCompileSecretSourcesUnexpectedKey verifies a secret source with an
// unrecognized key is a validation error.
func TestCompileSecretSourcesUnexpectedKey(t *testing.T) {
	doc := parseDoc(t, `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: p
spec:
  secretSources:
    s:
      kind: Vault
      bogus: x`)
	srcsVal, _ := doc.Spec.Get("secretSources")
	_, errs := CompileSecretSources("doc.yaml", srcsVal)
	if len(errs) == 0 {
		t.Fatal("CompileSecretSources accepted an unexpected key in a secret source; want a validation error")
	}
	if !strings.Contains(errs[0].Error(), "bogus") {
		t.Errorf("error %q does not name the offending key (bogus)", errs[0].Error())
	}
}

// TestCompileSecretSourcesNotMap verifies a secret source that is not a
// mapping is a validation error.
func TestCompileSecretSourcesNotMap(t *testing.T) {
	doc := parseDoc(t, `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: p
spec:
  secretSources:
    s: just-a-string`)
	srcsVal, _ := doc.Spec.Get("secretSources")
	_, errs := CompileSecretSources("doc.yaml", srcsVal)
	if len(errs) == 0 {
		t.Fatal("CompileSecretSources accepted a non-mapping secret source; want a validation error")
	}
}

// TestResolveVariableSets verifies a job's variableSets list resolves to the
// declared sets in listed order (acceptance criterion: a job references sets by
// name in its variableSets list).
func TestResolveVariableSets(t *testing.T) {
	declared := map[string]*VariableSet{
		"global": {Name: "global", Variables: []Variable{{Name: "var1", Value: VariableValue{Shape: VariablePlain, Plain: "value1"}}}},
		"other":  {Name: "other", Variables: []Variable{{Name: "x", Value: VariableValue{Shape: VariablePlain, Plain: "y"}}}},
	}
	list := Value{Type: List, Items: []Value{
		{Type: Scalar, String: "global"},
		{Type: Scalar, String: "other"},
	}}
	sets, errs := ResolveVariableSets("doc.yaml", NewPath("spec", "jobs", "deploy"), list, declared)
	if len(errs) > 0 {
		t.Fatalf("ResolveVariableSets returned errors: %v", errs)
	}
	if len(sets) != 2 {
		t.Fatalf("sets = %d, want 2", len(sets))
	}
	if sets[0].Name != "global" || sets[1].Name != "other" {
		t.Errorf("sets = [%s %s], want [global other]", sets[0].Name, sets[1].Name)
	}
}

// TestResolveVariableSetsUnknown verifies a reference to a set that is not
// declared is a validation error naming the offending key.
func TestResolveVariableSetsUnknown(t *testing.T) {
	declared := map[string]*VariableSet{
		"global": {Name: "global"},
	}
	list := Value{Type: List, Items: []Value{
		{Type: Scalar, String: "missing"},
	}}
	_, errs := ResolveVariableSets("doc.yaml", NewPath("spec", "jobs", "deploy"), list, declared)
	if len(errs) == 0 {
		t.Fatal("ResolveVariableSets accepted a reference to an undeclared set; want a validation error")
	}
	if !strings.Contains(errs[0].Error(), "missing") {
		t.Errorf("error %q does not name the offending set (missing)", errs[0].Error())
	}
	if !strings.Contains(errs[0].Error(), "spec.jobs.deploy.variableSets[0]") {
		t.Errorf("error %q does not name the offending key path", errs[0].Error())
	}
}

// TestResolveVariableSetsNonScalar verifies a variableSets list entry that is
// not a string is a validation error.
func TestResolveVariableSetsNonScalar(t *testing.T) {
	declared := map[string]*VariableSet{"global": {Name: "global"}}
	list := Value{Type: List, Items: []Value{
		{Type: Map, Entries: []Entry{{Key: "x", Value: Value{Type: Scalar, String: "y"}}}},
	}}
	_, errs := ResolveVariableSets("doc.yaml", NewPath("spec", "jobs", "deploy"), list, declared)
	if len(errs) == 0 {
		t.Fatal("ResolveVariableSets accepted a non-string list entry; want a validation error")
	}
}

// TestMergeJobVariablesInlineWins verifies the job's inline variables win over
// a set's on a key collision (acceptance criterion: the job's inline variables
// win over a set's on a key collision).
func TestMergeJobVariablesInlineWins(t *testing.T) {
	inline := []Variable{
		{Name: "canary", Value: VariableValue{Shape: VariablePlain, Plain: "true"}},
		{Name: "region", Value: VariableValue{Shape: VariablePlain, Plain: "us-west-2"}},
	}
	sets := []*VariableSet{
		{Name: "global", Variables: []Variable{
			{Name: "region", Value: VariableValue{Shape: VariablePlain, Plain: "us-east-1"}},
			{Name: "var1", Value: VariableValue{Shape: VariablePlain, Plain: "value1"}},
		}},
	}
	merged := MergeJobVariables(inline, sets)
	byName := map[string]Variable{}
	for _, v := range merged {
		byName[v.Name] = v
	}
	// The inline region (us-west-2) wins over the set's region (us-east-1).
	if byName["region"].Value.Plain != "us-west-2" {
		t.Errorf("merged region = %q, want the inline value us-west-2", byName["region"].Value.Plain)
	}
	// The set's var1 is merged in (not present inline).
	if byName["var1"].Value.Plain != "value1" {
		t.Errorf("merged var1 = %q, want value1", byName["var1"].Value.Plain)
	}
	// No duplicate names in the merged result.
	if len(merged) != len(byName) {
		t.Errorf("merged has %d entries but %d unique names (a duplicate)", len(merged), len(byName))
	}
}

// TestMergeJobVariablesSetOrder verifies an earlier-listed set wins over a
// later-listed set on a key collision.
func TestMergeJobVariablesSetOrder(t *testing.T) {
	sets := []*VariableSet{
		{Name: "first", Variables: []Variable{{Name: "x", Value: VariableValue{Shape: VariablePlain, Plain: "first"}}}},
		{Name: "second", Variables: []Variable{{Name: "x", Value: VariableValue{Shape: VariablePlain, Plain: "second"}}}},
	}
	merged := MergeJobVariables(nil, sets)
	byName := map[string]Variable{}
	for _, v := range merged {
		byName[v.Name] = v
	}
	if byName["x"].Value.Plain != "first" {
		t.Errorf("merged x = %q, want the earlier-listed set's value (first)", byName["x"].Value.Plain)
	}
}

// TestMergeJobVariablesMasked verifies a masked variable from a set is merged
// in (masked variables are referenced the same way as plain ones) and is
// tracked as masked in the merged result.
func TestMergeJobVariablesMasked(t *testing.T) {
	sets := []*VariableSet{
		{Name: "global", Variables: []Variable{
			{Name: "deploy_token", Value: VariableValue{Shape: VariableSecretSource, SecretSource: "prod-secrets", Key: "deploy_token"}},
		}},
	}
	merged := MergeJobVariables(nil, sets)
	if len(merged) != 1 {
		t.Fatalf("merged = %d, want 1", len(merged))
	}
	if !merged[0].Value.IsMasked() {
		t.Errorf("merged deploy_token is not masked, want masked")
	}
	// A masked variable is not in the run's plain parameters (it is resolved at
	// dispatch, F-12).
	if _, ok := PlainParams(merged)["deploy_token"]; ok {
		t.Errorf("PlainParams includes a masked variable, want it omitted")
	}
}

// TestValidateSecretSourceRefs verifies a sourced variable that names a
// declared source passes, and one that names an undeclared source is a
// validation error (acceptance criterion: a variable references a source by
// name).
func TestValidateSecretSourceRefs(t *testing.T) {
	sources := map[string]*SecretSource{
		"prod-secrets": {Name: "prod-secrets", Kind: "Vault"},
	}
	vars := []Variable{
		{Name: "deploy_token", Value: VariableValue{Shape: VariableSecretSource, SecretSource: "prod-secrets", Key: "deploy_token"}},
		{Name: "canary", Value: VariableValue{Shape: VariablePlain, Plain: "true"}},
	}
	if errs := ValidateSecretSourceRefs("doc.yaml", NewPath("spec", "jobs", "deploy", "variables"), vars, sources); len(errs) != 0 {
		t.Errorf("ValidateSecretSourceRefs returned errors for a declared source: %v", errs)
	}
	// A sourced variable that names an undeclared source is an error.
	bad := []Variable{
		{Name: "x", Value: VariableValue{Shape: VariableSecretSource, SecretSource: "missing", Key: "k"}},
	}
	errs := ValidateSecretSourceRefs("doc.yaml", NewPath("spec", "jobs", "deploy", "variables"), bad, sources)
	if len(errs) == 0 {
		t.Fatal("ValidateSecretSourceRefs accepted a reference to an undeclared source; want a validation error")
	}
	if !strings.Contains(errs[0].Error(), "missing") {
		t.Errorf("error %q does not name the offending source (missing)", errs[0].Error())
	}
}

// TestCompileVariablesEndToEnd verifies the full F-21.1.3 flow on a document
// that declares variableSets and secretSources and a job that references both:
// the sets resolve, the job's inline variables win on a collision, and the
// job's sourced variables reference declared sources.
func TestCompileVariablesEndToEnd(t *testing.T) {
	doc := parseDoc(t, `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: p
spec:
  variableSets:
    global:
      variables:
        var1: value1
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
    deploy:
      variables:
        canary: "true"
        deploy_token:
          secretSource: prod-secrets
          key: deploy_token
      variableSets:
        - global`)
	// Compile the pipeline's named resources.
	setsVal, _ := doc.Spec.Get("variableSets")
	sets, sErrs := CompileVariableSets("doc.yaml", setsVal)
	if len(sErrs) > 0 {
		t.Fatalf("CompileVariableSets: %v", sErrs)
	}
	srcsVal, _ := doc.Spec.Get("secretSources")
	srcs, srcErrs := CompileSecretSources("doc.yaml", srcsVal)
	if len(srcErrs) > 0 {
		t.Fatalf("CompileSecretSources: %v", srcErrs)
	}
	// Compile the job's inline variables and resolve its variableSets list.
	jobs, _ := doc.Spec.Get("jobs")
	job, ok := jobs.Get("deploy")
	if !ok {
		t.Fatal("no job deploy")
	}
	inlineVal, _ := job.Get("variables")
	inline, iErrs := CompileVariables("doc.yaml", NewPath("spec", "jobs", "deploy", "variables"), inlineVal)
	if len(iErrs) > 0 {
		t.Fatalf("CompileVariables (inline): %v", iErrs)
	}
	listVal, _ := job.Get("variableSets")
	resolved, rErrs := ResolveVariableSets("doc.yaml", NewPath("spec", "jobs", "deploy"), listVal, sets)
	if len(rErrs) > 0 {
		t.Fatalf("ResolveVariableSets: %v", rErrs)
	}
	// Merge the inline variables with the sets' variables (inline wins).
	merged := MergeJobVariables(inline, resolved)
	byName := map[string]Variable{}
	for _, v := range merged {
		byName[v.Name] = v
	}
	// The job's inline canary is present; the set's var1 is merged in.
	if byName["canary"].Value.Plain != "true" {
		t.Errorf("merged canary = %q, want true", byName["canary"].Value.Plain)
	}
	if byName["var1"].Value.Plain != "value1" {
		t.Errorf("merged var1 = %q, want value1", byName["var1"].Value.Plain)
	}
	// The job's inline deploy_token (sourced) wins over the set's; both are
	// sourced from the declared prod-secrets source.
	if byName["deploy_token"].Value.Shape != VariableSecretSource || byName["deploy_token"].Value.SecretSource != "prod-secrets" {
		t.Errorf("merged deploy_token = %+v, want secretSource/prod-secrets", byName["deploy_token"])
	}
	// The set's region (encrypted) is merged in and is masked.
	if byName["region"].Value.Shape != VariableEncrypted || !byName["region"].Value.IsMasked() {
		t.Errorf("merged region = %+v, want encrypted/masked", byName["region"])
	}
	// Every sourced variable references a declared source.
	if errs := ValidateSecretSourceRefs("doc.yaml", NewPath("spec", "jobs", "deploy", "variables"), merged, srcs); len(errs) != 0 {
		t.Errorf("ValidateSecretSourceRefs: %v", errs)
	}
	// The run's plain parameters carry only the unmasked values.
	params := PlainParams(merged)
	if params["canary"] != "true" || params["var1"] != "value1" {
		t.Errorf("PlainParams = %v, want canary and var1", params)
	}
	if _, ok := params["deploy_token"]; ok {
		t.Errorf("PlainParams includes the masked deploy_token, want it omitted")
	}
	if _, ok := params["region"]; ok {
		t.Errorf("PlainParams includes the masked region, want it omitted")
	}
}
