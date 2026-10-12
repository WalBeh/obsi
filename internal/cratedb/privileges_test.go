package cratedb

import (
	"reflect"
	"testing"
)

// The cases ran on the 6.4.5 shardlab (SELECT count(*) as each user).
func TestUsersWhoCanRead(t *testing.T) {
	privs := []Privilege{
		{Grantee: "lab_r", Class: "SCHEMA", Ident: "doc"},
		{Grantee: "lab_r", Class: "TABLE", Ident: "doc.repl", Deny: true},
		{Grantee: "lab_u1", Class: "TABLE", Ident: "doc.repl", Deny: true},
		{Grantee: "lab_u2", Class: "SCHEMA", Ident: "doc", Deny: true},
		{Grantee: "lab_u2", Class: "TABLE", Ident: "doc.repl"},
		{Grantee: "lab_r2", Class: "TABLE", Ident: "doc.repl"},
		{Grantee: "lab_u3", Class: "SCHEMA", Ident: "doc", Deny: true},
		{Grantee: "lab_u4", Class: "TABLE", Ident: "doc.repl"},
		{Grantee: "ops", Class: "CLUSTER"},
	}
	grantees := []Grantee{
		{Name: "crate", Superuser: true, User: true},
		{Name: "lab_r"}, {Name: "lab_r2"},
		{Name: "lab_u1", Roles: []string{"lab_r"}, User: true},  // own table DENY beats the role's schema GRANT
		{Name: "lab_u2", User: true},                            // table GRANT beats schema DENY
		{Name: "lab_u3", Roles: []string{"lab_r2"}, User: true}, // own schema DENY beats the role's table GRANT
		{Name: "lab_u4", Roles: []string{"lab_r"}, User: true},  // own table GRANT beats the role's table DENY
		{Name: "lab_u5", Roles: []string{"lab_r"}, User: true},  // only the role: its table DENY
		{Name: "ops", User: true},
		{Name: "nobody", User: true},
	}
	cases := map[string][]string{
		"doc.repl":   {"crate", "lab_u2", "lab_u4", "ops"},
		"doc.repl_p": {"crate", "lab_u1", "lab_u4", "lab_u5", "ops"},
	}
	for table, want := range cases {
		if got := UsersWhoCanRead([]string{table}, privs, grantees); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: got %v, want %v", table, got, want)
		}
	}
	if got := UsersWhoCanRead([]string{"doc.repl", "doc.repl_p"}, privs, grantees); !reflect.DeepEqual(got, []string{"crate", "lab_u4", "ops"}) {
		t.Errorf("both: got %v", got)
	}
}
