package idp

// HashPassword hashes a plaintext password with the shared Argon2ID KDF
// (kdf.go) under the PASSWORDS profile, prepending the optional pepper to
// the input, so the IdP can store only the encoding and verify logins
// against it. The encoding embeds its own random salt and parameters; the
// plaintext is never persisted.
func HashPassword(pepper, password string) (string, error) {
	encoding, err := EncodeSecret(kdfPasswords, pepper, password)
	if err != nil {
		return "", err
	}
	return encoding, nil
}

// VerifyPassword reports whether password matches the Argon2ID encoding
// produced by HashPassword under the same pepper (constant-time tag
// compare; malformed encodings yield false).
func VerifyPassword(pepper, encoding, password string) bool {
	return VerifySecret(encoding, pepper, password)
}

// nonNil returns a non-nil slice so that an empty role set marshals to []
// rather than null in JSON responses.
func nonNil[T any](slice []T) []T {
	if slice == nil {
		return []T{}
	}
	return slice
}
