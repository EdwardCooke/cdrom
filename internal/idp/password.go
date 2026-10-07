package idp

import (
	"golang.org/x/crypto/bcrypt"
)

// HashPassword hashes a plaintext password with bcrypt (which embeds a salt)
// so the IdP can store only the hash and verify logins against it. The
// plaintext is never persisted.
func HashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

// VerifyPassword reports whether password matches the bcrypt hash produced by
// HashPassword.
func VerifyPassword(hash, password string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
}

// nonNil returns a non-nil slice so that an empty role set marshals to []
// rather than null in JSON responses.
func nonNil[T any](slice []T) []T {
	if slice == nil {
		return []T{}
	}
	return slice
}
