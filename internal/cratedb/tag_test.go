package cratedb

import (
	"strings"
	"testing"
)

func TestQueryTagLikeMatchesTag(t *testing.T) {
	pattern := strings.Trim(QueryTagLike, "'")
	if !strings.HasPrefix(pattern, "%") || !strings.HasSuffix(QueryTag, strings.TrimPrefix(pattern, "%")) {
		t.Fatalf("QueryTagLike %s does not match QueryTag %q", QueryTagLike, QueryTag)
	}
	if strings.ContainsAny(strings.TrimPrefix(pattern, "%"), "%_'") {
		t.Fatalf("tag must not contain LIKE wildcards or quotes: %q", QueryTag)
	}
}
