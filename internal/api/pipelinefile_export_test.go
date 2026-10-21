package api

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"gopkg.in/yaml.v3"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// TestExportPipelineRoundTrip verifies the F-21.1.4 round-trip property:
// exporting a pipeline's compiled definition to the file format and re-importing
// the exported file yields the same compiled definition. The test builds a
// pipeline (the source of truth), exports it through pipelineToDocument, runs
// the exported YAML back through the import compiler (pipelineFileToRequest),
// and asserts the re-imported definition matches the original on every field
// the file format can express.
func TestExportPipelineRoundTrip(t *testing.T) {
	original := &dbpb.Pipeline{
		Name:        "deploy",
		Description: "a round-trip pipeline",
		Tags:        map[string]string{"team": "platform"},
		FailureMode: dbpb.FailureMode_FAILURE_MODE_BEST_EFFORT,
		ConcurrencyKeys: []string{
			"global-lock",
		},
		Params: []*dbpb.Parameter{
			{Name: "version", Default: "1.0.0", Description: "the version"},
			{Name: "environment", Default: "staging"},
		},
		Triggers: []*dbpb.Trigger{
			{Name: "nightly", Type: dbpb.TriggerType_TRIGGER_TYPE_CRON, Cron: "0 0 * * *"},
			{Name: "hook", Type: dbpb.TriggerType_TRIGGER_TYPE_WEBHOOK, Secret: "s3cret", OidcIssuer: "https://idp", OidcClaims: map[string]string{"org": "*"}},
		},
		VariableSets: []*dbpb.VariableSet{
			{
				Name: "common",
				Variables: []*dbpb.Variable{
					{Name: "region", Shape: dbpb.VariableShape_VARIABLE_PLAIN, Plain: "us-east-1"},
					{Name: "db_password", Shape: dbpb.VariableShape_VARIABLE_ENCRYPTED, Encrypted: "enc-1"},
				},
			},
		},
		SecretSources: []*dbpb.SecretSource{
			{Name: "vault", Kind: "vault", Path: "secret/data/app", Audience: "cdrom"},
		},
		Jobs: []*dbpb.JobDefinition{
			{
				Key:         "build",
				Name:        "build",
				TargetGroup: "linux-pool",
				Spec: &dbpb.JobSpec{
					Steps: []*dbpb.JobStep{
						{Name: "compile", Type: "shell", Params: map[string]*dbpb.ParamValue{"command": {String_: "go build ./..."}}},
						{Name: "test", Type: "shell", Params: map[string]*dbpb.ParamValue{"command": {String_: "go test ./..."}, "args": {Strings: []string{"-race"}}}},
						{Name: "lint", Params: map[string]*dbpb.ParamValue{"command": {String_: "golangci-lint run"}}},
					},
				},
			},
			{
				Key:         "deploy",
				Name:        "deploy",
				TargetGroup: "linux-pool",
				Needs:       []string{"build"},
				Environment: "prod",
				Automation:  dbpb.Automation_AUTOMATION_MANUAL,
				Approval:    dbpb.ApprovalPolicy_APPROVAL_POLICY_REQUIRED,
				Tags:        map[string]string{"team": "platform"},
				Variables: []*dbpb.Variable{
					{Name: "region", Shape: dbpb.VariableShape_VARIABLE_PLAIN, Plain: "eu-west-1"},
					{Name: "db_password", Shape: dbpb.VariableShape_VARIABLE_ENCRYPTED, Encrypted: "enc-1"},
					{Name: "api_token", Shape: dbpb.VariableShape_VARIABLE_SECRET_SOURCE, SecretSource: "vault", Key: "token"},
				},
				Spec: &dbpb.JobSpec{
					Steps: []*dbpb.JobStep{
						{Name: "release", Type: "shell", Params: map[string]*dbpb.ParamValue{"command": {String_: "deploy.sh"}, "args": {Strings: []string{"--prod"}}}},
					},
					Timeout:         durationPtr("10m"),
					Retry:           &dbpb.RetryPolicy{MaxAttempts: 2, Backoff: durationPtr("30s")},
					FailureMode:     dbpb.FailureMode_FAILURE_MODE_BEST_EFFORT,
					ConcurrencyKeys: []string{"db"},
					StepBarrier:     true,
					IgnoreFailed:    true,
				},
			},
		},
	}

	// Export the pipeline to the file format.
	doc := pipelineToDocument(original)
	data, err := yaml.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal export: %v", err)
	}

	// Re-import the exported file through the compiler.
	req, variableSets, secretSources, err := pipelineFileToRequest("", data)
	if err != nil {
		t.Fatalf("re-import exported file: %v", err)
	}
	jobs, err := pipelineJobsToProto(req.Jobs)
	if err != nil {
		t.Fatalf("map re-imported jobs: %v", err)
	}
	reimported := &dbpb.Pipeline{
		Name:            req.Name,
		Description:     req.Description,
		Tags:            req.Tags,
		FailureMode:     failureModeFromName(req.FailureMode),
		ConcurrencyKeys: req.ConcurrencyKeys,
		Params:          pipelineParamsToProto(req.Params),
		Triggers:        triggersToProto(req.Triggers),
		VariableSets:    variableSets,
		SecretSources:   secretSources,
		Jobs:            jobs,
	}

	got := canonicalizePipeline(reimported)
	want := canonicalizePipeline(original)
	if !deepEqualCanonical(want, got) {
		t.Errorf("round-trip mismatch:\nwant: %s\ngot:  %s", canonicalString(want), canonicalString(got))
	}
}

// durationPtr builds a proto Duration from a Go duration string.
func durationPtr(s string) *durationpb.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		panic(err)
	}
	return durationpb.New(d)
}

// canonicalVariable is a comparable form of a variable (F-21.1.3).
type canonicalVariable struct {
	Shape        string
	Plain        string
	Encrypted    string
	SecretSource string
	Key          string
}

// canonicalParam is a comparable form of a step param.
type canonicalParam struct {
	String  string
	Strings []string
}

// canonicalStep is a comparable form of a step.
type canonicalStep struct {
	Type         string
	Workdir      string
	Env          map[string]string
	Timeout      string
	Condition    string
	IgnoreFailed bool
	Once         bool
	Params       map[string]canonicalParam
}

// canonicalSpec is a comparable form of a job's execution spec.
type canonicalSpec struct {
	Steps           map[string]canonicalStep
	Timeout         string
	RetryMax        int
	RetryBackoff    string
	FailureMode     string
	ConcurrencyKeys []string
	StepBarrier     bool
	IgnoreFailed    bool
}

// canonicalJob is a comparable form of a job definition.
type canonicalJob struct {
	Key         string
	Name        string
	TargetGroup string
	Needs       []string
	Environment string
	Automation  string
	Approval    string
	Tags        map[string]string
	Variables   map[string]canonicalVariable
	Spec        canonicalSpec
}

// canonicalParamDecl is a comparable form of a pipeline parameter.
type canonicalParamDecl struct {
	Default     string
	Description string
}

// canonicalTrigger is a comparable form of a trigger.
type canonicalTrigger struct {
	Type          string
	Cron          string
	Secret        string
	EventPipeline string
	EventStatus   string
	OidcIssuer    string
	OidcClaims    map[string]string
}

// canonicalSource is a comparable form of a secret source.
type canonicalSource struct {
	Kind     string
	Path     string
	Audience string
}

// canonicalPipeline is a comparable form of a pipeline's compiled definition,
// normalized so two definitions that are equivalent (ignoring ordering of
// sets/slices and non-round-trippable fields) compare equal.
type canonicalPipeline struct {
	Name            string
	Description     string
	Tags            map[string]string
	FailureMode     string
	ConcurrencyKeys []string
	Params          map[string]canonicalParamDecl
	Triggers        map[string]canonicalTrigger
	VariableSets    map[string]map[string]canonicalVariable
	SecretSources   map[string]canonicalSource
	Jobs            map[string]canonicalJob
}

// canonicalizePipeline reduces a pipeline's compiled definition to a
// comparable form: slices are sorted, and the fields the file format cannot
// express (id, version, timestamps, the pipeline-level worker group) are
// dropped.
func canonicalizePipeline(p *dbpb.Pipeline) canonicalPipeline {
	out := canonicalPipeline{
		Name:            p.GetName(),
		Description:     p.GetDescription(),
		Tags:            p.GetTags(),
		FailureMode:     failureModeExportName(p.GetFailureMode()),
		ConcurrencyKeys: sortedStrings(p.GetConcurrencyKeys()),
		Params:          map[string]canonicalParamDecl{},
		Triggers:        map[string]canonicalTrigger{},
		VariableSets:    map[string]map[string]canonicalVariable{},
		SecretSources:   map[string]canonicalSource{},
		Jobs:            map[string]canonicalJob{},
	}
	for _, param := range p.GetParams() {
		out.Params[param.GetName()] = canonicalParamDecl{Default: param.GetDefault(), Description: param.GetDescription()}
	}
	for _, trigger := range p.GetTriggers() {
		out.Triggers[trigger.GetName()] = canonicalTrigger{
			Type:          triggerTypeName(trigger.GetType()),
			Cron:          trigger.GetCron(),
			Secret:        trigger.GetSecret(),
			EventPipeline: trigger.GetEventPipeline(),
			EventStatus:   runStatusExportName(trigger.GetEventStatus()),
			OidcIssuer:    trigger.GetOidcIssuer(),
			OidcClaims:    trigger.GetOidcClaims(),
		}
	}
	for _, set := range p.GetVariableSets() {
		vars := map[string]canonicalVariable{}
		for _, v := range set.GetVariables() {
			vars[v.GetName()] = canonicalVariableFromProto(v)
		}
		out.VariableSets[set.GetName()] = vars
	}
	for _, src := range p.GetSecretSources() {
		out.SecretSources[src.GetName()] = canonicalSource{Kind: src.GetKind(), Path: src.GetPath(), Audience: src.GetAudience()}
	}
	for _, job := range p.GetJobs() {
		out.Jobs[job.GetKey()] = canonicalizeJob(job)
	}
	return out
}

// canonicalizeJob reduces a job definition to a comparable form.
func canonicalizeJob(job *dbpb.JobDefinition) canonicalJob {
	out := canonicalJob{
		Key:         job.GetKey(),
		Name:        job.GetName(),
		TargetGroup: job.GetTargetGroup(),
		Needs:       sortedStrings(job.GetNeeds()),
		Environment: job.GetEnvironment(),
		Automation:  automationName(job.GetAutomation()),
		Approval:    approvalPolicyExportName(job.GetApproval()),
		Tags:        job.GetTags(),
		Variables:   map[string]canonicalVariable{},
	}
	for _, v := range job.GetVariables() {
		out.Variables[v.GetName()] = canonicalVariableFromProto(v)
	}
	if spec := job.GetSpec(); spec != nil {
		out.Spec = canonicalSpec{
			Steps:           map[string]canonicalStep{},
			Timeout:         durationStringOrNil(spec.GetTimeout()),
			FailureMode:     failureModeExportName(spec.GetFailureMode()),
			ConcurrencyKeys: sortedStrings(spec.GetConcurrencyKeys()),
			StepBarrier:     spec.GetStepBarrier(),
			IgnoreFailed:    spec.GetIgnoreFailed(),
		}
		if spec.GetRetry() != nil {
			out.Spec.RetryMax = int(spec.GetRetry().GetMaxAttempts())
			out.Spec.RetryBackoff = durationStringOrNil(spec.GetRetry().GetBackoff())
		}
		for _, step := range spec.GetSteps() {
			out.Spec.Steps[stepNameOrIndex(step, len(out.Spec.Steps))] = canonicalizeStep(step)
		}
	}
	return out
}

// canonicalizeStep reduces a step to a comparable form.
func canonicalizeStep(step *dbpb.JobStep) canonicalStep {
	out := canonicalStep{
		Type:         step.GetType(),
		Workdir:      step.GetWorkdir(),
		Env:          step.GetEnv(),
		Timeout:      durationStringOrNil(step.GetTimeout()),
		Condition:    step.GetCondition(),
		IgnoreFailed: step.GetIgnoreFailed(),
		Once:         step.GetOnce(),
		Params:       map[string]canonicalParam{},
	}
	for name, pv := range step.GetParams() {
		out.Params[name] = canonicalParam{String: pv.GetString_(), Strings: pv.GetStrings()}
	}
	return out
}

// canonicalVariableFromProto reduces a variable to a comparable form.
func canonicalVariableFromProto(v *dbpb.Variable) canonicalVariable {
	return canonicalVariable{
		Shape:        variableShapeExportName(v.GetShape()),
		Plain:        v.GetPlain(),
		Encrypted:    v.GetEncrypted(),
		SecretSource: v.GetSecretSource(),
		Key:          v.GetKey(),
	}
}

// variableShapeExportName maps a proto VariableShape onto its file name.
func variableShapeExportName(s dbpb.VariableShape) string {
	switch s {
	case dbpb.VariableShape_VARIABLE_PLAIN:
		return "plain"
	case dbpb.VariableShape_VARIABLE_ENCRYPTED:
		return "encrypted"
	case dbpb.VariableShape_VARIABLE_SECRET_SOURCE:
		return "secretSource"
	default:
		return ""
	}
}

// stepNameOrIndex returns a step's name, or a positional name for an unnamed
// step (mirroring the exporter's fallback).
func stepNameOrIndex(step *dbpb.JobStep, index int) string {
	if step.GetName() != "" {
		return step.GetName()
	}
	return fmt.Sprintf("step%d", index)
}

// durationStringOrNil renders a proto Duration as a Go duration string; a nil
// duration yields "".
func durationStringOrNil(d *durationpb.Duration) string {
	if d == nil {
		return ""
	}
	return d.AsDuration().String()
}

// sortedStrings returns a sorted copy of items.
func sortedStrings(items []string) []string {
	out := append([]string(nil), items...)
	sort.Strings(out)
	return out
}

// deepEqualCanonical reports whether two canonical pipelines are equal.
func deepEqualCanonical(a, b canonicalPipeline) bool {
	return reflect.DeepEqual(a, b)
}

// canonicalString renders a canonical pipeline for a mismatch message.
func canonicalString(p canonicalPipeline) string {
	data, _ := json.MarshalIndent(p, "", "  ")
	return string(data)
}
