package database

// ptrBool returns a pointer to b (helper for boolean fields that distinguish
// "unset" from a concrete value).
func ptrBool(b bool) *bool { return &b }
