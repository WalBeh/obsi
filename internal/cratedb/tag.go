package cratedb

// QueryTag is appended to every statement obsi issues on its own (collectors,
// heartbeat, node discovery), so its polling can be filtered out of sys.jobs
// and sys.jobs_log. CrateDB keeps comments verbatim in the logged stmt.
// Statements the user runs from the SQL tab, KILL and SET GLOBAL stay
// untagged and visible.
const QueryTag = " /* obsi */"

// QueryTagLike is a LIKE pattern literal matching QueryTag-suffixed stmts.
const QueryTagLike = "'%/* obsi */'"
