package cratedb

// Type-conversion helpers for JSON-decoded interface{} values
// returned by the CrateDB /_sql API. These are the single source
// of truth — used by both the cratedb and collector packages.

// ToFloat64 converts a JSON number (float64 or int64) to float64.
func ToFloat64(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int64:
		return float64(n)
	}
	return 0
}

// ToInt64 converts a JSON number to int64.
func ToInt64(v interface{}) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	}
	return 0
}

// ToInt16 converts a JSON number to int16.
func ToInt16(v interface{}) int16 {
	switch n := v.(type) {
	case float64:
		return int16(n)
	case int64:
		return int16(n)
	}
	return 0
}

// ToString converts a JSON string value, returning "" for nil or non-string.
func ToString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// ToBool converts a JSON boolean value, returning false for nil or non-bool.
func ToBool(v interface{}) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	return false
}
