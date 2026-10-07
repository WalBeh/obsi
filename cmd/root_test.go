package cmd

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// A dead port-forward times out or is refused; only a 401/403 should lead
// to a password prompt.
func TestIsAuthError(t *testing.T) {
	wrap := func(err error) error { return fmt.Errorf("bootstrap cluster name: %w", err) }
	cases := []struct {
		err  error
		want bool
	}{
		{wrap(&cratedb.CrateDBError{StatusCode: 401}), true},
		{wrap(&cratedb.CrateDBError{StatusCode: 403}), true},
		{wrap(&cratedb.CrateDBError{StatusCode: 500}), false},
		{wrap(context.DeadlineExceeded), false},
		{wrap(errors.New("dial tcp 127.0.0.1:4200: connect: connection refused")), false},
	}
	for _, c := range cases {
		if got := isAuthError(c.err); got != c.want {
			t.Errorf("isAuthError(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}
