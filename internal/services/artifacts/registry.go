// registry.go — the package registry's shared conventions (F-21.6): the
// reserved namespace and the "<package>-<version>.<ext>" filing/parser.
// Lives in the artifacts package because BOTH the database service (which
// resolves spec.artifacts against the registry at run creation) and the API
// (which publishes and browses it) need the same parsing — divergence would
// silently disagree on what a version is.
package artifacts

import "strings"

// RegistryNamespace is the reserved store namespace holding published
// package versions (F-21.6).
const RegistryNamespace = "packages"

// ComposePackageNameVersion synthesizes a registry filing's DISPLAY NAME
// from its coordinates: "<package>-<version>.<ext>". The extension is taken
// VERBATIM from the publisher (leading dot ensured). The convention is
// cosmetic — the record's package_name / version / is_pre_release columns
// are the coordinates, and nothing parses a filing's name to recover them —
// but composing the name uniformly keeps the registry readable and the
// publish endpoint's round-trip check meaningful.
func ComposePackageNameVersion(pkg, version, ext string) string {
	if ext == "" {
		return pkg + "-" + version
	}
	if ext[0] != '.' {
		ext = "." + ext
	}
	return pkg + "-" + version + ext
}

// knownCompoundExts are multi-part extensions recognized when parsing a
// registry file name (LONGEST first, so ".tar.gz" beats ".gz"-style guesses).
var knownCompoundExts = []string{
	".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz",
	".zip", ".jar", ".war", ".whl", ".deb", ".rpm", ".msi", ".apk",
}

// ParsePackageNameVersion splits a registry file name
// ("<package>-<version>.<ext>") into its parts. Parsing conventions (fixed by
// the ambiguity of dash-containing names):
//
//   - Compound extensions (.tar.gz et al.) are tried LONGEST-FIRST; otherwise
//     the extension is the DOT plus the trailing alphanumeric run (a run of
//     dots, e.g. "...", is not an extension).
//   - The remainder after the extension is "<package>-<version>". The split
//     point is the EARLIEST "-" whose RIGHT side begins with a DIGIT:
//     versions (semver, calver) lead with a digit, so "coolapi-6.6.6-rc.1"
//     parses as (coolapi, 6.6.6-rc.1) — pre-release tags keep their dashes —
//     while "a-b-c-1.2.3" parses as (a-b-c, 1.2.3). Consequently PACKAGE
//     NAMES SHOULD NOT CONTAIN DASHES FOLLOWED BY A DIGIT-LIKE TOKEN; the
//     sanctioned spelling for spaced names is the underscore (my_pkg).
func ParsePackageNameVersion(fileName string) (pkg, version, ext string, ok bool) {
	ext = ""
	for _, ce := range knownCompoundExts {
		if strings.HasSuffix(fileName, ce) {
			ext = ce
			break
		}
	}
	if ext == "" {
		end := len(fileName)
		for end > 0 && ((fileName[end-1] >= 'a' && fileName[end-1] <= 'z') ||
			(fileName[end-1] >= 'A' && fileName[end-1] <= 'Z') ||
			(fileName[end-1] >= '0' && fileName[end-1] <= '9')) {
			end--
		}
		if end == 0 || fileName[end-1] != '.' {
			return "", "", "", false
		}
		ext = fileName[end-1:]
	}
	remainder := fileName[:len(fileName)-len(ext)]
	for i := 0; i < len(remainder); i++ {
		if remainder[i] != '-' {
			continue
		}
		candPkg, candVer := remainder[:i], remainder[i+1:]
		if candPkg == "" || candVer == "" || candVer[len(candVer)-1] == '-' {
			continue
		}
		if candVer[0] >= '0' && candVer[0] <= '9' {
			return candPkg, candVer, ext, true
		}
	}
	return "", "", "", false
}
