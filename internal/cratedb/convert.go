package cratedb

// Type-conversion helpers for JSON-decoded interface{} values
// returned by the CrateDB /_sql API. Single source of truth —
// used by both the cratedb and collector packages.

func ToFloat64(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int64:
		return float64(n)
	}
	return 0
}

func ToInt64(v interface{}) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	}
	return 0
}

func ToInt16(v interface{}) int16 {
	switch n := v.(type) {
	case float64:
		return int16(n)
	case int64:
		return int16(n)
	}
	return 0
}

func ToString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func ToBool(v interface{}) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	return false
}
