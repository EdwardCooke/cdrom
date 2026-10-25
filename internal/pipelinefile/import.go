package pipelinefile

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"cdrom/internal/sourceref"
)

// Fetcher retrieves a file's content given a full reference (a "<type>:<spec>"
// string, F-21.2). It is the seam between the compiler and the source-fetcher
// (F-21.2): the API implements it by dialing the fetcher gRPC service (selecting
// and decrypting the source's credential, and passing the runtime-managed SSH
// known hosts). A nil Fetcher means imports cannot be resolved: a document with
// no imports compiles fine, but a document with an import fails with a clear
// error.
type Fetcher func(ctx context.Context, reference string) ([]byte, error)

// VaultImport is a variable set's vault import (F-21.4): a reference to a named
// secret source (vault) whose values are imported as masked variables. A
// per-key import ({vault: <name>, keys: [a, b, c]}) is materialized at compile
// time as concrete VariableSecretSource variables (one per key) in the set's
// variables; an every-key import ({vault: <name>}) is dynamic (the vault's keys
// are not known at compile time) and is recorded here, to be expanded at
// dispatch (the API fetches the vault and creates one variable per key).
type VaultImport struct {
	// Source is the name of the secret source (vault) to import.
	Source string
	// Keys are the specific keys to import; empty means every key in the vault.
	Keys []string
}

// Resolver resolves a pipeline document's imports (F-21.4): it walks the
// document, fetches each referenced file (via the Fetcher), splices the
// imported unit in place (as if it were inline), and returns the flattened
// document plus the every-key vault imports (by set name). It handles
// transitive imports (a fetched file may itself import others) and detects
// import cycles.
//
// A locator is either a full reference (a "<type>:<spec>" string, F-21.2) or a
// path relative to the current file, resolved against the pipeline's source
// (the root reference): for a git source, relative to the file's path in the
// repo (the fetcher fetches the referenced file from the same repo/branch);
// for an http source, relative to the file's URL; for a file source, relative
// to the file's path under the fetcher's configured local-filesystem root.
//
// The spliced document is the one the rest of the compiler (Compile*,
// Resolve*) runs on, so an imported unit is validated exactly as if it were
// inline: a `ref` / empty step entry must name a shared step (inline or
// imported), a job's `variableSets` entry must name a set declared under
// spec.variableSets (inline or merged in by import), and a unit name is unique
// across the merged namespace (each imported file holds exactly one unit,
// spliced into the named slot that references it, with the local fields
// winning on a collision).
type Resolver struct {
	ctx     context.Context
	fetcher Fetcher
	// root is the main file's source reference, used to resolve relative
	// locators found in the main file. Nil when the main file has no source
	// (e.g. it was uploaded directly), in which case only full references are
	// resolvable.
	root *sourceref.Reference
	// visiting tracks the references currently on the resolution stack (DFS).
	// A reference met again while it is visiting is an import cycle.
	visiting map[string]bool
	// cache memoizes fetched + resolved documents by reference, so a file
	// imported by several units is fetched and resolved once.
	cache map[string]*cachedDoc
}

// cachedDoc is a fetched file's resolved form: the flattened document (its own
// imports spliced in) and, for a VariableSet file, the every-key vault imports
// discovered in it.
type cachedDoc struct {
	doc *Document
	vi  []VaultImport
}

// NewResolver creates a Resolver. ctx is the context for fetches, fetcher is
// the file fetcher (nil = imports cannot be resolved), and root is the main
// file's source reference (nil = the main file has no source, so only full
// references are resolvable).
func NewResolver(ctx context.Context, fetcher Fetcher, root *sourceref.Reference) *Resolver {
	return &Resolver{
		ctx:      ctx,
		fetcher:  fetcher,
		root:     root,
		visiting: make(map[string]bool),
		cache:    make(map[string]*cachedDoc),
	}
}

// ResolveMain parses the main document (from data) and resolves all of its
// imports, returning the flattened document (every import spliced in place)
// and the every-key vault imports (by set name, including those bubbled up
// from imported VariableSet files). name is the main file's name (carried into
// any validation errors). A document that is not a Pipeline (a Step /
// VariableSet / Job document) has no top-level imports to resolve and is
// returned as-is.
func (r *Resolver) ResolveMain(name string, data []byte) (*Document, map[string][]VaultImport, Errors) {
	doc, err := Parse(name, data)
	if err != nil {
		var es Errors
		if errors.As(err, &es) {
			return nil, nil, es
		}
		return nil, nil, Errors{Error{File: name, Reason: err.Error()}}
	}
	vaultImports := make(map[string][]VaultImport)
	if doc.Kind != KindPipeline {
		return doc, vaultImports, nil
	}
	var errs Errors

	// The pipeline's shared steps (spec.steps): each entry may carry an
	// importFrom that pulls the step from a Step file.
	if stepsVal, ok := doc.Spec.Get("steps"); ok {
		resolved, stepErrs := r.resolveStepsMap(name, r.root, NewPath("spec", "steps"), stepsVal)
		errs = append(errs, stepErrs...)
		doc.Spec = setKey(doc.Spec, "steps", resolved)
	}

	// The pipeline's jobs (spec.jobs): each entry may carry an importFrom that
	// pulls the whole job from a Job file (local fields override), and each
	// job's steps may carry importFrom that pulls individual steps.
	if jobsVal, ok := doc.Spec.Get("jobs"); ok {
		resolved, jobErrs := r.resolveJobsMap(name, r.root, NewPath("spec", "jobs"), jobsVal)
		errs = append(errs, jobErrs...)
		doc.Spec = setKey(doc.Spec, "jobs", resolved)
	}

	// The pipeline's variable sets (spec.variableSets): each entry may carry an
	// import list that pulls variables from VariableSet files or vaults.
	if setsVal, ok := doc.Spec.Get("variableSets"); ok {
		resolved, vi, setErrs := r.resolveSetsMap(name, r.root, NewPath("spec", "variableSets"), setsVal)
		errs = append(errs, setErrs...)
		for k, v := range vi {
			vaultImports[k] = v
		}
		doc.Spec = setKey(doc.Spec, "variableSets", resolved)
	}

	if len(errs) > 0 {
		return nil, nil, errs
	}
	return doc, vaultImports, nil
}

// resolveLocator resolves a locator against the current file's reference (cur).
// If the locator is a full reference (a "<type>:<spec>" string), it is parsed
// and returned as-is. Otherwise it is treated as a path relative to the
// current file, resolved against cur. A nil cur (the main file has no source)
// admits only full references.
func (r *Resolver) resolveLocator(cur *sourceref.Reference, locator string) (*sourceref.Reference, error) {
	if locator == "" {
		return nil, fmt.Errorf("empty import locator")
	}
	if cur == nil {
		ref, err := sourceref.ParseReference(locator)
		if err != nil {
			return nil, fmt.Errorf("a relative import locator (%q) cannot be resolved without the pipeline's source; use a full reference (<type>:<spec>) or attach a source to the pipeline", locator)
		}
		return ref, nil
	}
	return cur.ResolveRelative(locator)
}

// fetchAndResolve fetches the file at ref, parses it, and resolves its own
// imports (transitive, relative to the file's reference), returning the
// flattened document and, for a VariableSet file, the every-key vault imports
// discovered in it. Results are memoized by reference (a file imported by
// several units is fetched and resolved once), and import cycles are detected
// (a reference met again while it is on the resolution stack is an error).
func (r *Resolver) fetchAndResolve(ref *sourceref.Reference) (*cachedDoc, error) {
	key := ref.String()
	if c, ok := r.cache[key]; ok {
		return c, nil
	}
	if r.visiting[key] {
		return nil, fmt.Errorf("import cycle involving %s (an import may not pull in a file that already imports it, directly or transitively)", key)
	}
	if r.fetcher == nil {
		return nil, fmt.Errorf("cannot resolve import %s: no source-fetcher is configured", key)
	}
	r.visiting[key] = true
	defer delete(r.visiting, key)

	content, err := r.fetcher(r.ctx, key)
	if err != nil {
		return nil, fmt.Errorf("fetch import %s: %v", key, err)
	}
	doc, perr := Parse(key, content)
	if perr != nil {
		return nil, fmt.Errorf("parse import %s: %v", key, perr)
	}

	c := &cachedDoc{doc: doc}
	switch doc.Kind {
	case KindStep:
		// A Step file's spec is a single step definition; it may itself carry
		// an importFrom (transitive).
		rv, serrs := r.resolveStepSpec(key, ref, NewPath("spec"), doc.Spec)
		if len(serrs) > 0 {
			return nil, serrs[0]
		}
		doc.Spec = rv
	case KindJob:
		// A Job file's spec is a single job spec; its steps may carry
		// importFrom (transitive).
		if stepsVal, ok := doc.Spec.Get("steps"); ok {
			rv, jerrs := r.resolveStepsMap(key, ref, NewPath("spec", "steps"), stepsVal)
			if len(jerrs) > 0 {
				return nil, jerrs[0]
			}
			doc.Spec = setKey(doc.Spec, "steps", rv)
		}
	case KindVariableSet:
		// A VariableSet file's spec is the set entry itself; it may carry an
		// import list (transitive).
		rv, vi, verrs := r.resolveSetEntry(key, ref, NewPath("spec"), doc.Spec)
		if len(verrs) > 0 {
			return nil, verrs[0]
		}
		doc.Spec = rv
		c.vi = vi
	case KindPipeline:
		return nil, fmt.Errorf("import %s is a Pipeline document, not a reusable unit (a file holds exactly one Step / VariableSet / Job)", key)
	}

	r.cache[key] = c
	return c, nil
}

// resolveStepsMap resolves the step-level importFrom in a steps map (a name →
// step-spec map). Each entry that carries an importFrom is replaced by the
// imported step's spec (from a Step file), with the local settings (the
// entry's fields other than importFrom) overriding the imported step's.
// Entries without an importFrom are left untouched. cur is the current file's
// reference (for resolving relative locators), and base is the dotted path of
// the steps map (for error keys).
func (r *Resolver) resolveStepsMap(file string, cur *sourceref.Reference, base Path, steps Value) (Value, Errors) {
	if steps.Type != Map {
		return steps, nil
	}
	var errs Errors
	for i, e := range steps.Entries {
		if e.Value.Type != Map {
			continue
		}
		rv, serrs := r.resolveStepSpec(file, cur, base.Extend(e.Key), e.Value)
		errs = append(errs, serrs...)
		steps.Entries[i].Value = rv
	}
	return steps, errs
}

// resolveStepSpec resolves a single step entry's importFrom (F-21.4). An entry
// without an importFrom is returned unchanged. An entry with an importFrom is
// replaced by the imported step's spec (from a Step file), with the local
// settings overriding the imported step's (the imported step is the base).
// The importFrom key is consumed. file is the owning file's name (for error
// messages), cur is the current file's reference, and base is the dotted path
// of the step entry (e.g. "spec.steps.unit-test").
func (r *Resolver) resolveStepSpec(file string, cur *sourceref.Reference, base Path, entry Value) (Value, Errors) {
	raw, ok := entry.Get("importFrom")
	if !ok {
		return entry, nil
	}
	if raw.Type != Scalar {
		return entry, Errors{Error{File: file, Key: base.Extend("importFrom").String(),
			Reason: "a step's importFrom must be a string (a full reference or a path relative to the current file)"}}
	}
	target, err := r.resolveLocator(cur, raw.String)
	if err != nil {
		return entry, Errors{Error{File: file, Key: base.Extend("importFrom").String(),
			Reason: fmt.Sprintf("importFrom: %v", err)}}
	}
	c, err := r.fetchAndResolve(target)
	if err != nil {
		return entry, Errors{Error{File: file, Key: base.Extend("importFrom").String(),
			Reason: fmt.Sprintf("importFrom: %v", err)}}
	}
	if c.doc.Kind != KindStep {
		return entry, Errors{Error{File: file, Key: base.Extend("importFrom").String(),
			Reason: fmt.Sprintf("importFrom: %s is a %s document, not a Step document", target.String(), c.doc.Kind)}}
	}
	// Splice: the imported step's spec is the base; the local settings (the
	// entry's fields other than importFrom) override it.
	return mergeValues(c.doc.Spec, removeKey(entry, "importFrom")), nil
}

// resolveJobsMap resolves the job-level importFrom in a jobs map (a name →
// job-spec map). Each entry that carries an importFrom is replaced by the
// imported job's spec (from a Job file), with the local fields overriding the
// imported job's (a local steps map overrides the imported job's steps by
// name). Afterwards each job's steps (imported or local) have their step-level
// importFrom resolved. cur is the current file's reference, and base is the
// dotted path of the jobs map.
func (r *Resolver) resolveJobsMap(file string, cur *sourceref.Reference, base Path, jobs Value) (Value, Errors) {
	if jobs.Type != Map {
		return jobs, nil
	}
	var errs Errors
	for i, e := range jobs.Entries {
		if e.Value.Type != Map {
			continue
		}
		entryBase := base.Extend(e.Key)
		raw, ok := e.Value.Get("importFrom")
		if ok {
			if raw.Type != Scalar {
				errs = append(errs, Error{File: file, Key: entryBase.Extend("importFrom").String(),
					Reason: "a job's importFrom must be a string (a full reference or a path relative to the current file)"})
				continue
			}
			target, terr := r.resolveLocator(cur, raw.String)
			if terr != nil {
				errs = append(errs, Error{File: file, Key: entryBase.Extend("importFrom").String(),
					Reason: fmt.Sprintf("importFrom: %v", terr)})
				continue
			}
			c, ferr := r.fetchAndResolve(target)
			if ferr != nil {
				errs = append(errs, Error{File: file, Key: entryBase.Extend("importFrom").String(),
					Reason: fmt.Sprintf("importFrom: %v", ferr)})
				continue
			}
			if c.doc.Kind != KindJob {
				errs = append(errs, Error{File: file, Key: entryBase.Extend("importFrom").String(),
					Reason: fmt.Sprintf("importFrom: %s is a %s document, not a Job document", target.String(), c.doc.Kind)})
				continue
			}
			// Splice: the imported job's spec is the base; the local fields
			// (other than importFrom) override it, including its steps (a
			// local steps map overrides the imported job's steps by name).
			e.Value = spliceJob(c.doc.Spec, removeKey(e.Value, "importFrom"))
			jobs.Entries[i].Value = e.Value
		}
		// Resolve the step-level importFrom in the job's steps (whether the
		// job was imported or not), relative to the current file's reference.
		if stepsVal, sok := e.Value.Get("steps"); sok {
			rv, serrs := r.resolveStepsMap(file, cur, entryBase.Extend("steps"), stepsVal)
			errs = append(errs, serrs...)
			e.Value = setKey(e.Value, "steps", rv)
			jobs.Entries[i].Value = e.Value
		}
	}
	return jobs, errs
}

// resolveSetsMap resolves the set-level import in a variableSets map (a name →
// set-entry map). Each entry that carries an import list is resolved (its
// variables merged from VariableSet files and vaults, its secretSources merged
// from the imported files, the import key consumed). It returns the resolved
// map and the every-key vault imports (by set name). cur is the current file's
// reference, and base is the dotted path of the variableSets map.
func (r *Resolver) resolveSetsMap(file string, cur *sourceref.Reference, base Path, sets Value) (Value, map[string][]VaultImport, Errors) {
	if sets.Type != Map {
		return sets, nil, nil
	}
	vi := make(map[string][]VaultImport)
	var errs Errors
	for i, e := range sets.Entries {
		if e.Value.Type != Map {
			continue
		}
		if _, ok := e.Value.Get("import"); !ok {
			continue
		}
		resolved, setVI, setErrs := r.resolveSetEntry(file, cur, base.Extend(e.Key), e.Value)
		errs = append(errs, setErrs...)
		if len(setVI) > 0 {
			vi[e.Key] = setVI
		}
		sets.Entries[i].Value = resolved
	}
	return sets, vi, errs
}

// resolveSetEntry resolves a single variable set's import list (F-21.4). The
// set's variables are the union of its inline variables and the imported ones
// (from VariableSet files and per-key vault imports), with the inline ones
// winning on a key collision; among imported sources an earlier-listed entry
// has lower priority than a later one. The set's secretSources are the union
// of its own and the imported files', with the set's own winning on a name
// collision. The import key is consumed. It returns the resolved set entry,
// the every-key vault imports (dynamic — expanded at dispatch), and any
// validation errors. cur is the current file's reference (for resolving
// relative locators), and base is the dotted path of the set entry.
func (r *Resolver) resolveSetEntry(file string, cur *sourceref.Reference, base Path, setEntry Value) (Value, []VaultImport, Errors) {
	importList, ok := setEntry.Get("import")
	if !ok {
		return setEntry, nil, nil
	}
	if importList.Type != List {
		return setEntry, nil, Errors{Error{File: file, Key: base.Extend("import").String(),
			Reason: "a variable set's import must be a list of {from: <uri>} / {vault: <name>} / {vault: <name>, keys: [...]} entries"}}
	}

	// The set's inline variables and secretSources (the highest priority; they
	// win on a key/name collision).
	inlineVars, _ := setEntry.Get("variables")
	if inlineVars.Type != Map {
		inlineVars = Value{Type: Map}
	}
	inlineSrcs, _ := setEntry.Get("secretSources")
	if inlineSrcs.Type != Map {
		inlineSrcs = Value{Type: Map}
	}

	// baseVars / baseSrcs accumulate the imported variables and secret sources
	// (folded in list order: a later entry feeds the accumulator after an
	// earlier one, so it outranks it; the inline variables / secret sources
	// are layered on top of the whole imported base and therefore win).
	baseVars := Value{Type: Map}
	baseSrcs := Value{Type: Map}
	var vaultImports []VaultImport

	for idx, item := range importList.Items {
		entryKey := base.Extend("import").String() + fmt.Sprintf("[%d]", idx)
		if item.Type != Map {
			return setEntry, vaultImports, Errors{Error{File: file, Key: entryKey,
				Reason: "an import entry must be a mapping ({from: <uri>} or {vault: <name>})"}}
		}
		// {from: <uri>} — pull a VariableSet file and merge its variables and
		// secret sources into the set.
		fromRaw, hasFrom := item.Get("from")
		if hasFrom {
			if fromRaw.Type != Scalar {
				return setEntry, vaultImports, Errors{Error{File: file, Key: entryKey + ".from",
					Reason: "an import's from must be a string (a full reference or a path relative to the current file)"}}
			}
			target, err := r.resolveLocator(cur, fromRaw.String)
			if err != nil {
				return setEntry, vaultImports, Errors{Error{File: file, Key: entryKey + ".from",
					Reason: fmt.Sprintf("import.from: %v", err)}}
			}
			c, ferr := r.fetchAndResolve(target)
			if ferr != nil {
				return setEntry, vaultImports, Errors{Error{File: file, Key: entryKey + ".from",
					Reason: fmt.Sprintf("import.from: %v", ferr)}}
			}
			if c.doc.Kind != KindVariableSet {
				return setEntry, vaultImports, Errors{Error{File: file, Key: entryKey + ".from",
					Reason: fmt.Sprintf("import.from: %s is a %s document, not a VariableSet document", target.String(), c.doc.Kind)}}
			}
			if importedVars, vok := c.doc.Spec.Get("variables"); vok && importedVars.Type == Map {
				baseVars = mergeValues(baseVars, importedVars)
			}
			if importedSrcs, sok := c.doc.Spec.Get("secretSources"); sok && importedSrcs.Type == Map {
				baseSrcs = mergeValues(baseSrcs, importedSrcs)
			}
			// Bubble up the imported file's every-key vault imports.
			vaultImports = append(vaultImports, c.vi...)
			continue
		}
		// {vault: <name>} or {vault: <name>, keys: [...]} — import the vault's
		// values as masked variables.
		vaultRaw, hasVault := item.Get("vault")
		if hasVault {
			if vaultRaw.Type != Scalar {
				return setEntry, vaultImports, Errors{Error{File: file, Key: entryKey + ".vault",
					Reason: "an import's vault must be a string (the name of a secretSources entry)"}}
			}
			vault := vaultRaw.String
			keys, _ := item.StringSlice("keys")
			if len(keys) > 0 {
				// Per-key: materialize as concrete VariableSecretSource
				// variables (one per key) in the set's variables namespace.
				for _, k := range keys {
					varVal := Value{Type: Map, Entries: []Entry{
						{Key: "secretSource", Value: Value{Type: Scalar, String: vault}},
						{Key: "key", Value: Value{Type: Scalar, String: k}},
					}}
					baseVars = setKey(baseVars, k, varVal)
				}
			} else {
				// Every-key: dynamic (the vault's keys are not known at compile
				// time); record it as a vault import, expanded at dispatch.
				vaultImports = append(vaultImports, VaultImport{Source: vault})
			}
			continue
		}
		return setEntry, vaultImports, Errors{Error{File: file, Key: entryKey,
			Reason: "an import entry has only from or vault keys"}}
	}

	// Rebuild the set entry: the merged variables (the inline ones win over
	// the imported base) and the merged secret sources (the set's own win over
	// the imported base), with the import key consumed.
	out := removeKey(setEntry, "import")
	out = setKey(out, "variables", mergeValues(baseVars, inlineVars))
	out = setKey(out, "secretSources", mergeValues(baseSrcs, inlineSrcs))
	sort.SliceStable(vaultImports, func(a, b int) bool { return vaultImports[a].Source < vaultImports[b].Source })
	return out, vaultImports, nil
}

// spliceJob merges an imported job's spec (base) with the local job's fields
// (override), returning the spliced job spec. The local fields override the
// imported job's; a local steps map overrides the imported job's steps by name
// (the imported steps are the base, local steps win).
func spliceJob(base, local Value) Value {
	merged := mergeValues(base, local)
	// Special-case steps: merge by step name (local steps override imported
	// steps by name), rather than letting the local steps map wholesale replace
	// the imported one.
	if baseSteps, ok := base.Get("steps"); ok && baseSteps.Type == Map {
		if localSteps, lok := local.Get("steps"); lok && localSteps.Type == Map {
			merged = setKey(merged, "steps", mergeValues(baseSteps, localSteps))
		}
	}
	return merged
}

// mergeValues returns a new map Value that is the union of base and override:
// for each key in base the entry is taken from base; for each key in override
// the entry overrides base's (override wins). The result's entries preserve
// base's order, with override-only keys appended in override order. Neither
// input is mutated.
func mergeValues(base, override Value) Value {
	out := Value{Type: Map}
	if base.Type == Map {
		out.Entries = append(out.Entries, base.Entries...)
	}
	pos := make(map[string]int, len(out.Entries))
	for i, e := range out.Entries {
		pos[e.Key] = i
	}
	if override.Type != Map {
		return out
	}
	for _, e := range override.Entries {
		if idx, ok := pos[e.Key]; ok {
			out.Entries[idx] = e
		} else {
			out.Entries = append(out.Entries, e)
			pos[e.Key] = len(out.Entries) - 1
		}
	}
	return out
}

// setKey returns a new map Value equal to v with key set to val (replacing the
// existing entry in place when present, appending it otherwise). Inputs are not
// mutated.
func setKey(v Value, key string, val Value) Value {
	out := Value{Type: Map}
	if v.Type == Map {
		out.Entries = append(out.Entries, v.Entries...)
	}
	for i, e := range out.Entries {
		if e.Key == key {
			out.Entries[i] = Entry{Key: key, Value: val}
			return out
		}
	}
	out.Entries = append(out.Entries, Entry{Key: key, Value: val})
	return out
}

// removeKey returns a new map Value equal to v with key removed. Inputs are not
// mutated.
func removeKey(v Value, key string) Value {
	out := Value{Type: Map}
	if v.Type != Map {
		return out
	}
	for _, e := range v.Entries {
		if e.Key != key {
			out.Entries = append(out.Entries, e)
		}
	}
	return out
}
