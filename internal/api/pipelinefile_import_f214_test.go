package api

import (
	"context"
	"fmt"
	"strings"
	"testing"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/pipelinefile"
	"cdrom/internal/sourceref"
)

// importFixtureFiles are the shared unit files a pipeline with imports pulls
// in (F-21.4): a step, a variable set (with its own secret source and an
// every-key vault import), and a job. Their references are relative to the
// main file's source (testImportRoot).
var importFixtureFiles = map[string]string{
	"git:https://github.com/org/repo.git@main[/shared/steps/unit-test.yaml]": `
apiVersion: cdrom/alphav1
kind: Step
metadata:
  name: unit-test
spec:
  shell:
    command: make test
  workdir: /src
  timeout: 5m
`,
	"git:https://github.com/org/repo.git@main[/variablesets/dev.yaml]": `
apiVersion: cdrom/alphav1
kind: VariableSet
metadata:
  name: dev
spec:
  variables:
    region: eu-west
  secretSources:
    dev-vault:
      kind: AES
      path: enc-json
  import:
    - vault: dev-vault
`,
	"git:https://github.com/org/repo.git@main[/jobs/deploy-dev.yaml]": `
apiVersion: cdrom/alphav1
kind: Job
metadata:
  name: deploy-dev
spec:
  steps:
    step1:
      shell: echo "imported step1"
  variables:
    canary: "true"
  variableSets:
    - dev
`,
}

// testImportRoot is the main file's source reference (a git source) that the
// pipeline's relative import locators resolve against.
const testImportRoot = "git:https://github.com/org/repo.git@main[/pipelines/webapp.yaml]"

// importFixturePipeline is a pipeline that exercises every F-21.4 import form:
// a shared step pulled in with importFrom (plus a local override), a variable
// set that imports a file and an every-key vault, and a job pulled in with
// importFrom (local fields override the imported job's).
const importFixturePipeline = `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: webapp
spec:
  steps:
    build:
      shell: make build
    unit-test:
      importFrom: ../shared/steps/unit-test.yaml
      workdir: /opt/src
  variableSets:
    dev:
      variables:
        overlap: from-inline
      import:
        - from: ../variablesets/dev.yaml
  jobs:
    build:
      steps:
        b:
          shell: make build
    deploy-dev:
      importFrom: ../jobs/deploy-dev.yaml
      environment: staging
`

// TestCompilePipelineFileResolvesImports runs a pipeline with imports through
// the full API compile path (F-21.4): the resolver splices each imported unit
// in place (verified as-if-inline by the downstream compilers), local fields
// override the imported ones, and the imported variable set's every-key vault
// import propagates onto the job that references it.
func TestCompilePipelineFileResolvesImports(t *testing.T) {
	fetch := pipelinefile.Fetcher(func(ctx context.Context, reference string) ([]byte, error) {
		b, ok := importFixtureFiles[reference]
		if !ok {
			return nil, fmt.Errorf("unexpected fetch: %s", reference)
		}
		return []byte(b), nil
	})
	root, err := sourceref.ParseReference(testImportRoot)
	if err != nil {
		t.Fatalf("parse root: %v", err)
	}
	req, variableSets, secretSources, err := requestFromFile(context.Background(), "webapp.yaml", []byte(importFixturePipeline), root, fetch)
	if err != nil {
		t.Fatalf("requestFromFile: %v", err)
	}

	// The imported variable set's variables are merged (inline wins on
	// collision) and its secret source bubbled up into the pipeline's
	// namespace.
	var devSet *dbpb.VariableSet
	for _, s := range variableSets {
		if s.GetName() == "dev" {
			devSet = s
		}
	}
	if devSet == nil {
		t.Fatal("no dev variable set in the compiled pipeline")
	}
	byVar := map[string]*dbpb.Variable{}
	for _, v := range devSet.GetVariables() {
		byVar[v.GetName()] = v
	}
	if ov := byVar["overlap"]; ov == nil || ov.GetPlain() != "from-inline" {
		t.Errorf("dev.overlap = %+v, want the inline value (inline wins)", ov)
	}
	if rg := byVar["region"]; rg == nil || rg.GetPlain() != "eu-west" {
		t.Errorf("dev.region = %+v, want the imported 'eu-west'", rg)
	}
	srcNames := map[string]bool{}
	for _, s := range secretSources {
		srcNames[s.GetName()] = true
	}
	if !srcNames["dev-vault"] {
		t.Errorf("pipeline secret sources lack the imported dev-vault: %v", srcNames)
	}

	// The imported job is spliced in: its environment is overridden locally,
	// its imported step survives, and its every-key vault import (from the
	// imported variable set) is carried on the job.
	var deployDev *pipelineJobRequest
	for i := range req.Jobs {
		if req.Jobs[i].Key == "deploy-dev" {
			deployDev = &req.Jobs[i]
		}
	}
	if deployDev == nil {
		t.Fatal("no deploy-dev job in the compiled pipeline")
	}
	if deployDev.Environment != "staging" {
		t.Errorf("deploy-dev.environment = %q, want the local override 'staging'", deployDev.Environment)
	}
	stepNames := map[string]bool{}
	for _, st := range deployDev.Spec.Steps {
		stepNames[st.Name] = true
	}
	if !stepNames["step1"] {
		t.Errorf("deploy-dev steps lack the imported step1: %v", stepNames)
	}
	vi := map[string]bool{}
	for _, im := range deployDev.VaultImports {
		vi[im.GetSource()] = true
	}
	if !vi["dev-vault"] {
		t.Errorf("deploy-dev vault imports lack dev-vault (propagated from the imported set): %v", vi)
	}
}

// TestCompilePipelineFileImportWithoutFetcher verifies a pipeline with an
// import but no source-fetcher fails with a clear, actionable error (F-21.4).
func TestCompilePipelineFileImportWithoutFetcher(t *testing.T) {
	root, err := sourceref.ParseReference(testImportRoot)
	if err != nil {
		t.Fatalf("parse root: %v", err)
	}
	_, _, _, err = requestFromFile(context.Background(), "webapp.yaml", []byte(importFixturePipeline), root, nil)
	if err == nil {
		t.Fatal("requestFromFile with no fetcher accepted imports; want an error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "source-fetcher") && !strings.Contains(msg, "fetch import") {
		t.Errorf("error does not mention the missing fetcher: %v", msg)
	}
}
