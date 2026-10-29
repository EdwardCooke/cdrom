// package_registry.go — the package registry's naming conventions and
// publication-time validation (F-21.6, storage-redesign era).
//
// RESOLUTION no longer lives here: it is pure SQL over the storage_objects
// records (storage_objects.go, ResolvePackageReference). What remains is the
// registry's NAMING CONVENTION — the display name a filing is stored UNDER
// ("<package>-<version>.<ext>") — and the publication-time checks that keep
// filings consistent with it. The convention is COSMETIC: nothing parses a
// filing's name to recover its coordinates (the package_name / version /
// is_pre_release columns ARE the coordinates), so a name that merely LOOKS
// ambiguous can never confuse resolution.
package database

import (
	"fmt"
	"regexp"

	"cdrom/internal/services/artifacts"
)

// PackageRegistryNamespace is the reserved store namespace holding published
// package versions (F-21.6). Aliased from the artifacts package so the API's
// publish/browse surface agrees on the same constant.
const PackageRegistryNamespace = artifacts.RegistryNamespace

// registryFileName synthesizes a filing's display name from its coordinates
// (delegates to the shared composer in the artifacts package so the API's
// publish surface and any database-side synthesis agree).
func registryFileName(pkg, version, ext string) string {
	return artifacts.ComposePackageNameVersion(pkg, version, ext)
}

// registryCoordinateCharset is the conservative charset a filing's package
// name and version may draw from (keeps synthesized display names tidy and
// predictable across platforms). Letters, digits, ".", "_" and "-".
var registryCoordinateCharset = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// ValidatePublicationChecks enforces the publication-time rules for a
// registry filing (EXPORTED: the API's publish endpoint invokes it before
// allocating the record — a single source of truth for what a filing's
// coordinates may look like):
//
//   - the coordinates fit the charset (cosmetic tidiness);
//   - the SYNTHESIZED display name parses back to exactly the advertised
//     (package, version) under the shared registry parser — this rejects
//     names that WOULD BE ambiguously re-split if anyone ever relied on the
//     name (defense in depth for the cosmetic convention).
//
// Called by the API's publish endpoint before it allocates the record.
func ValidatePublicationChecks(pkg, version, ext string) error {
	if !registryCoordinateCharset.MatchString(pkg) {
		return fmt.Errorf("package name %q may only contain letters, digits, '.', '_' and '-'", pkg)
	}
	if !registryCoordinateCharset.MatchString(version) {
		return fmt.Errorf("version %q may only contain letters, digits, '.', '_' and '-'", version)
	}
	// Extension-less coordinations (a promotion from an extension-less
	// source) skip the round-trip: the parser expresses no-extension names
	// poorly, and the CHARSET check above is the substantive guarantee.
	if ext == "" {
		return nil
	}
	name := registryFileName(pkg, version, ext)
	back, ver, _, ok := artifacts.ParsePackageNameVersion(name)
	if !ok || back != pkg || ver != version {
		return fmt.Errorf("synthesized filing name %q parses back to (%q, %q), not the advertised (%q, %q); choose less ambiguous coordinates", name, back, ver, pkg, version)
	}
	return nil
}
