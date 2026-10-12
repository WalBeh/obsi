package cratedb

import "sort"

// Privilege is a row of sys.privileges.
type Privilege struct {
	Grantee string
	Class   string // CLUSTER, SCHEMA, TABLE, VIEW
	Ident   string // schema, or schema.table; empty for CLUSTER
	Deny    bool   // state DENY rather than GRANT
}

// Grantee is a user or role with the roles granted to it, from sys.users
// and sys.roles.
type Grantee struct {
	Name      string
	Superuser bool
	Roles     []string
	User      bool // false for roles
}

// UsersWhoCanRead lists the users (not roles) that may read every table in
// tables (schema.table), with privileges of one type, e.g. DQL. It
// resolves as CrateDB's Roles.hasPrivilege does: superusers may; else the
// grantee's own most specific entry (table, schema, cluster) decides; only
// without one do its roles count, where any DENY wins.
func UsersWhoCanRead(tables []string, privs []Privilege, grantees []Grantee) []string {
	byName := map[string]Grantee{}
	for _, g := range grantees {
		byName[g.Name] = g
	}
	type key struct{ grantee, class, ident string }
	entries := map[key]bool{} // value: deny
	for _, p := range privs {
		entries[key{p.Grantee, p.Class, p.Ident}] = p.Deny
	}
	// own returns 1 grant, -1 deny, 0 no entry.
	own := func(name, schema, table string) int {
		for _, k := range []key{{name, "TABLE", schema + "." + table}, {name, "SCHEMA", schema}, {name, "CLUSTER", ""}} {
			if deny, ok := entries[k]; ok {
				if deny {
					return -1
				}
				return 1
			}
		}
		return 0
	}
	var resolve func(name, schema, table string, seen map[string]bool) int
	resolve = func(name, schema, table string, seen map[string]bool) int {
		g := byName[name]
		if g.Superuser {
			return 1
		}
		if r := own(name, schema, table); r != 0 {
			return r
		}
		if seen[name] {
			return 0
		}
		seen[name] = true
		result := 0
		for _, parent := range g.Roles {
			switch resolve(parent, schema, table, seen) {
			case -1:
				return -1
			case 1:
				result = 1
			}
		}
		return result
	}
	var out []string
	for _, g := range grantees {
		if !g.User {
			continue
		}
		ok := true
		for _, t := range tables {
			schema, table := SplitRegclass(t)
			if resolve(g.Name, schema, table, map[string]bool{}) != 1 {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, g.Name)
		}
	}
	sort.Strings(out)
	return out
}
