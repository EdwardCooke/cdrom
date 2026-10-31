package idp

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/crypto/argon2"
)

// Secret-key KDF (F-24/F-25/F-26/F-28.2). All credential secrets — user
// passwords, per-user API keys, service-account keys, and SCIM API keys —
// are stored as ARGON2ID encodings ($argon2id$, PHC string format) of
// pepper || secret, where the pepper is an optional deployment-shared secret
// PREPENDed to the input (the same role it played in the retired
// SHA-512(pepper + salt + key) formulas). Argon2ID embeds its own random
// salt and parameters in the encoding, so no separate salt column is stored
// and verification parses the encoding, re-derives with the stored salt and
// parameters, and constant-time-compares the tags. Backward compatibility
// with the previous algorithms (bcrypt / SHA-512) is intentionally NOT
// maintained: deploying this change invalidates all existing credentials.
//
// Parameter profiles are DELIBERATELY different per secret KIND:
// passwords are low-entropy and receive the heaviest treatment (the OWASP
// minimum), while high-entropy machine keys receive a lighter profile so
// per-request verification stays affordable. Profiles are fixed constants —
// never negotiated or migrated.
const (
	argon2Variant = "argon2id"
	argon2Version = 19

	passwordParamMemKiB = 19456 // 19 MiB (OWASP minimum)
	passwordParamTime   = 2
	passwordParamThreads = 1

	// Machine keys (API keys, service-account keys, SCIM keys) are HIGH-ENTROPY
	// (≥64 random chars), so online guessing is infeasible regardless of KDF
	// strength; the KDF's job is defending against an OFFLINE attack on a
	// dumped database (dictionary guesses amplified by the pepper). Their
	// verification is PER-REQUEST (hot path), unlike passwords, so the profile
	// trades cost for affordability: 8 MiB / t=2 / p=1 measures ~6-7 ms/op on
	// commodity hardware (roughly 30 ms for a full 5-entry SCIM bundle walk),
	// which is comfortable for a machine channel and largely hidden by the
	// guard's ~1 s verdict memo. Threads stay at 1 so concurrent verifications
	// serialize predictably instead of contending for cores.
	keyParamMemKiB  = 8192 // 8 MiB
	keyParamTime    = 2
	keyParamThreads = 1

	kdfSaltSize = 16 // random salt embedded in the encoding
	kdfTagSize  = 32 // derived tag size
)

// kdfParams is one Argon2ID parameter profile.
type kdfParams struct {
	memKiB   int32
	time     int32
	threads  int32
}

// kdfKind selects a parameter profile.
type kdfKind int

const (
	// kdfPasswords is the profile for user passwords (F-24).
	kdfPasswords kdfKind = iota
	// kdfAPIKeys is the profile for high-entropy machine keys: per-user API
	// keys (F-25), service-account keys (F-26), and SCIM API keys (F-28.2).
	kdfAPIKeys
)

func (k kdfKind) params() kdfParams {
	switch k {
	case kdfPasswords:
		return kdfParams{memKiB: passwordParamMemKiB, time: passwordParamTime, threads: passwordParamThreads}
	default:
		return kdfParams{memKiB: keyParamMemKiB, time: keyParamTime, threads: keyParamThreads}
	}
}

// kdfInput joins the pepper and the secret: the pepper (possibly empty) is
// UTF-8 prepended to the secret, mirroring the historic pepper-first
// concatenation.
func kdfInput(pepper, secret string) []byte {
	in := make([]byte, 0, len(pepper)+len(secret))
	in = append(in, pepper...)
	in = append(in, secret...)
	return in
}

// derive runs Argon2ID over kdfInput(pepper, secret) with the given salt and
// parameters, producing a kdfTagSize-byte tag.
func derive(params kdfParams, salt []byte, pepper, secret string) []byte {
	return argon2.IDKey(kdfInput(pepper, secret), salt, uint32(params.time), uint32(params.memKiB), uint8(params.threads), kdfTagSize)
}

// EncodeSecret hashes secret with the kind's profile: a fresh random salt is
// drawn, the tag derived over pepper || secret, and the result rendered as a
// PHC-format $argon2id$ string.
func EncodeSecret(kind kdfKind, pepper, secret string) (string, error) {
	params := kind.params()
	salt := make([]byte, kdfSaltSize)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("idp: draw kdf salt: %w", err)
	}
	tag := derive(params, salt, pepper, secret)
	return encodeArgon2ID(params, salt, tag), nil
}

// VerifySecret reports whether secret reproduces the tag of the given
// $argon2id$ encoding under the supplied pepper: the encoding's salt and
// parameters are extracted, the tag re-derived, and compared in CONSTANT TIME.
// Malformed encodings yield false (they are ours, not attacker-chosen).
func VerifySecret(encoding, pepper, secret string) bool {
	params, salt, tag, ok := decodeArgon2ID(encoding)
	if !ok {
		return false
	}
	actual := derive(params, salt, pepper, secret)
	return subtle.ConstantTimeCompare(actual, tag) == 1
}

// encodeArgon2ID renders a PHC-format string:
// $argon2id$v=<ver>,m=<memKiB>,t=<time>,p=<threads>$<b64(salt)>$<b64(tag)>.
func encodeArgon2ID(params kdfParams, salt, tag []byte) string {
	b64 := base64.RawStdEncoding
	return "$" + argon2Variant + "$v=" + strconv.Itoa(argon2Version) +
		",m=" + strconv.FormatInt(int64(params.memKiB), 10) +
		",t=" + strconv.FormatInt(int64(params.time), 10) +
		",p=" + strconv.FormatInt(int64(params.threads), 10) +
		"$" + b64.EncodeToString(salt) +
		"$" + b64.EncodeToString(tag)
}

// decodeArgon2ID parses a PHC-format $argon2id$ encoding into its parameters,
// salt, and tag. ok is false for any malformation (wrong variant, missing
// segments, unparsable parameters, undersized salt/tag).
func decodeArgon2ID(encoding string) (params kdfParams, salt, tag []byte, ok bool) {
	fields := strings.Split(strings.TrimPrefix(encoding, "$"), "$")
	if len(fields) != 4 || fields[0] != argon2Variant {
		return params, nil, nil, false
	}
	meta := strings.Split(fields[1], ",")
	if len(meta) != 4 || !strings.HasPrefix(meta[0], "v="+strconv.Itoa(argon2Version)) {
		return params, nil, nil, false
	}
	v, err := strconv.Atoi(strings.TrimPrefix(meta[0], "v="))
	if err != nil || v != argon2Version {
		return params, nil, nil, false
	}
	vals := map[string]int32{}
	for _, kv := range meta[1:] {
		k, valStr, found := strings.Cut(kv, "=")
		if !found {
			return params, nil, nil, false
		}
		n, err := strconv.ParseInt(valStr, 10, 32)
		if err != nil || n <= 0 {
			return params, nil, nil, false
		}
		vals[k] = int32(n)
	}
	mem, mok := vals["m"]
	ti, tok := vals["t"]
	th, thok := vals["p"]
	if !mok || !tok || !thok {
		return params, nil, nil, false
	}
	decodedSalt, err := base64.RawStdEncoding.DecodeString(fields[2])
	if err != nil || len(decodedSalt) < 1 {
		return params, nil, nil, false
	}
	decodedTag, err := base64.RawStdEncoding.DecodeString(fields[3])
	if err != nil || len(decodedTag) != kdfTagSize {
		return params, nil, nil, false
	}
	return kdfParams{memKiB: mem, time: ti, threads: th}, decodedSalt, decodedTag, true
}
