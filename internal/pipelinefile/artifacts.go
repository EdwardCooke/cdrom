// artifacts.go — the spec.artifacts compiler (F-21.6): the pipeline's named
// package references, validated at import.
//
// The file form:
//
//	spec:
//	  artifacts:
//	    - name: xyz                    # the reference name (required, unique)
//	      package:
//	        name: somecoolapi          # the package itself (optional; defaults to the reference name)
//	        tag: 6.6.6                 # the exact version (required unless useLatest)
//	        preReleaseRegex: ""        # constrains eligible pre-releases (implies allowPreRelease relevance)
//	        allowPreRelease: false     # opt into pre-release versions
//	        useLatest: true            # latest by date (ignores tag)
//
// Compilation maps the file onto the schema's PackageArtifact list; unknown
// keys are rejected (the house style: actionable errors, no silent ignores).
package pipelinefile

import (
	"fmt"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// CompilePackageArtifacts compiles the spec.artifacts VALUE (a sequence of
// {name, package {...}} entries) into the schema's PackageArtifact list.
// file is the offending file's name (carried into errors); an absent or null
// value yields (nil, nil) (the pipeline declares no packages).
func CompilePackageArtifacts(file string, val Value) ([]*dbpb.PackageArtifact, Errors) {
	if val.IsNull() {
		return nil, nil
	}
	if val.Type != List {
		return nil, Errors{Error{File: file, Key: "spec.artifacts",
			Reason: "spec.artifacts must be a list of named package references"}}
	}
	var out []*dbpb.PackageArtifact
	var errs Errors
	seen := map[string]bool{}
	for i, e := range val.Items {
		base := NewPath("spec", "artifacts").Extend(fmt.Sprintf("[%d]", i))
		ref, refErrs := compilePackageArtifact(file, base, e)
		if len(refErrs) > 0 {
			errs = append(errs, refErrs...)
			continue
		}
		if seen[ref.GetName()] {
			errs = append(errs, Error{File: file, Key: base.Extend("name").String(),
				Reason: fmt.Sprintf("duplicate package reference name %q", ref.GetName())})
			continue
		}
		seen[ref.GetName()] = true
		out = append(out, ref)
	}
	if len(errs) > 0 {
		return nil, errs
	}
	return out, nil
}

// compilePackageArtifact compiles one spec.artifacts entry.
func compilePackageArtifact(file string, base Path, e Value) (*dbpb.PackageArtifact, Errors) {
	if e.Type != Map {
		return nil, Errors{Error{File: file, Key: base.String(),
			Reason: "a package reference must be a mapping (name + a package block)"}}
	}
	var errs Errors
	ref := &dbpb.PackageArtifact{}
	known := map[string]bool{"name": true, "package": true}
	for _, fe := range e.Entries {
		if !known[fe.Key] {
			errs = append(errs, Error{File: file, Key: base.Extend(fe.Key).String(),
				Reason: fmt.Sprintf("unknown key %q (a package reference carries name and package)", fe.Key)})
			continue
		}
		switch fe.Key {
		case "name":
			s := fe.Value.AsString()
			if s == "" {
				errs = append(errs, Error{File: file, Key: base.Extend("name").String(),
					Reason: "a package reference's name must be a non-empty string"})
				break
			}
			ref.Name = s
		case "package":
			pkg, pkgErrs := compilePackageBlock(file, base.Extend("package"), fe.Value)
			if len(pkgErrs) > 0 {
				errs = append(errs, pkgErrs...)
				break
			}
			ref.PackageName = pkg.pkgName
			ref.Tag = pkg.tag
			ref.UseLatest = pkg.useLatest
			ref.AllowPreRelease = pkg.allowPreRelease
			ref.PreReleaseRegex = pkg.preReleaseRegex
		}
	}
	if ref.Name == "" {
		errs = append(errs, Error{File: file, Key: base.Extend("name").String(),
			Reason: "a package reference requires a name"})
	}
	// A reference that does not use the latest version must pin a tag.
	if !ref.UseLatest && ref.Tag == "" {
		errs = append(errs, Error{File: file, Key: base.Extend("package", "tag").String(),
			Reason: "a package reference that does not set useLatest must pin a tag"})
	}
	if len(errs) > 0 {
		return nil, errs
	}
	return ref, nil
}

// packageBlockSettings is the compiled form of a reference's package block.
type packageBlockSettings struct {
	pkgName         string
	tag             string
	useLatest       bool
	allowPreRelease bool
	preReleaseRegex string
}

// compilePackageBlock compiles the package block (name, tag, preReleaseRegex,
// allowPreRelease, useLatest). An absent block is valid (all defaults: the
// reference name is the package name, and a tag is REQUIRED — enforced by the
// caller).
func compilePackageBlock(file string, base Path, val Value) (*packageBlockSettings, Errors) {
	settings := &packageBlockSettings{}
	if val.IsNull() {
		return settings, nil
	}
	if val.Type != Map {
		return nil, Errors{Error{File: file, Key: base.String(),
			Reason: "a package block must be a mapping (name, tag, preReleaseRegex, allowPreRelease, useLatest)"}}
	}
	known := map[string]bool{"name": true, "tag": true, "preReleaseRegex": true, "allowPreRelease": true, "useLatest": true}
	for _, fe := range val.Entries {
		if !known[fe.Key] {
			return nil, Errors{Error{File: file, Key: base.Extend(fe.Key).String(),
				Reason: fmt.Sprintf("unknown key %q (a package block carries name, tag, preReleaseRegex, allowPreRelease, useLatest)", fe.Key)}}
		}
		switch fe.Key {
		case "name":
			s := fe.Value.AsString()
			if s == "" {
				return nil, Errors{Error{File: file, Key: base.Extend("name").String(), Reason: "the package name must be a non-empty string"}}
			}
			settings.pkgName = s
		case "tag":
			s := fe.Value.AsString()
			if s == "" {
				return nil, Errors{Error{File: file, Key: base.Extend("tag").String(), Reason: "the package tag must be a non-empty string"}}
			}
			settings.tag = s
		case "preReleaseRegex":
			settings.preReleaseRegex = fe.Value.AsString()
		case "allowPreRelease":
			b, ok := fe.Value.AsBool()
			if !ok {
				return nil, Errors{Error{File: file, Key: base.Extend("allowPreRelease").String(), Reason: "allowPreRelease must be a boolean"}}
			}
			settings.allowPreRelease = b
		case "useLatest":
			b, ok := fe.Value.AsBool()
			if !ok {
				return nil, Errors{Error{File: file, Key: base.Extend("useLatest").String(), Reason: "useLatest must be a boolean"}}
			}
			settings.useLatest = b
		}
	}
	return settings, nil
}
