package idp

import (
	"crypto/rand"
	"fmt"
	"math/big"
)

// SCIM connector key shape (F-28.2): 32 characters drawn uniformly from a
// curated alphabet, encoded with Argon2ID (the machine-key profile), pepper
// prepended to the KDF input. The plaintext is shown to the operator exactly
// once (at creation) and is never persisted; only the encoding and an 8-char
// prefix travel onward.
const (
	scimKeyLength    = 32
	scimKeyPrefixLen = 8
)

// scimKeyAlphabet is the character set a SCIM key's random characters are
// drawn from (F-28.2): uppercase, lowercase, digits, plus a curated set of
// punctuation. Quotes, apostrophe, backslash, forward slash, percent,
// brackets, and parentheses are deliberately absent: they invite escape bugs
// in shells, URLs, and config files. Spaces are excluded outright.
var scimKeyAlphabet = []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@$^&*_-=+.")

// NewScimKey draws a fresh SCIM connector key's plaintext: 32 characters,
// uniformly, from scimKeyAlphabet via crypto/rand.
func NewScimKey() (string, error) {
	b := make([]rune, scimKeyLength)
	for i := range b {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(scimKeyAlphabet))))
		if err != nil {
			return "", fmt.Errorf("idp: generate scim key: %w", err)
		}
		b[i] = scimKeyAlphabet[n.Int64()]
	}
	return string(b), nil
}

// scimKeyPrefixOf returns the short, non-secret prefix of a key shown in
// listings so an operator can tell keys apart without the plaintext.
func scimKeyPrefixOf(key string) string {
	if len(key) < scimKeyPrefixLen {
		return key
	}
	return key[:scimKeyPrefixLen]
}

// EncodeScimKey returns the Argon2ID encoding (PHC string, drawing and
// embedding its own random salt, machine-key parameter profile) of a SCIM
// key's plaintext, with the optional pepper PREPENDED to the KDF input. The
// encoding is what gets stored; the plaintext is shown exactly once and is
// never persisted. Follows the F-25/F-26 house pattern (unified on Argon2ID).
func EncodeScimKey(pepper, key string) (string, error) {
	return EncodeSecret(kdfAPIKeys, pepper, key)
}
