package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

var doctorCmd = &cobra.Command{
	Use:   "doctor [endpoint|profile]",
	Short: "Check connectivity, permissions, and exit",
	Args:  cobra.MaximumNArgs(1),
	RunE:  runDoctor,
}

func runDoctor(cmd *cobra.Command, args []string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	_, registry, _, err := resolveConnection(ctx, cmd, args, false)
	if err != nil {
		return err
	}

	runDoctorChecks(ctx, registry)
	return nil
}

// runDoctorChecks checks connectivity, permissions, and reports what the observer can and can't do.
func runDoctorChecks(ctx context.Context, registry *cratedb.Registry) {
	fmt.Print("\nChecking CrateDB connectivity and permissions...\n\n")

	// 1. Basic connectivity + version
	var version string
	var nodeCount int
	var clusterName string
	if resp, err := registry.Query(ctx, "SELECT name FROM sys.cluster"); err != nil {
		printCheck(false, "Connection", err.Error())
		fmt.Println("\nCannot proceed without basic connectivity.")
		os.Exit(1)
	} else if len(resp.Rows) > 0 {
		clusterName = cratedb.ToString(resp.Rows[0][0])
	}

	if resp, err := registry.Query(ctx, "SELECT count(*), max(version['number']) FROM sys.nodes"); err == nil && len(resp.Rows) > 0 {
		nodeCount = int(cratedb.ToFloat64(resp.Rows[0][0]))
		version = cratedb.ToString(resp.Rows[0][1])
	}
	printCheck(true, "Connection", fmt.Sprintf("%s (%d nodes, cluster: %s)", version, nodeCount, clusterName))

	// 2. sys.nodes
	checkTable(ctx, registry, "sys.nodes",
		"SELECT count(*) FROM sys.nodes",
		"Required for node monitoring (Overview, Nodes tabs)")

	// 3. sys.cluster
	checkScalar(ctx, registry, "sys.cluster",
		"SELECT name FROM sys.cluster",
		"Required for cluster settings (Overview tab)")

	// 4. sys.checks
	checkTable(ctx, registry, "sys.checks",
		"SELECT count(*) FROM sys.checks",
		"Required for health checks (Overview tab)")

	// 5. sys.health
	checkTable(ctx, registry, "sys.health",
		"SELECT count(*) FROM sys.health",
		"Required for table health (Overview tab)")

	// 6. sys.shards
	checkTable(ctx, registry, "sys.shards",
		"SELECT count(*) FROM sys.shards",
		"Required for table sizes, shard distribution (Tables, Shards tabs, Overview data line)")

	// 7. sys.jobs
	checkTable(ctx, registry, "sys.jobs",
		"SELECT count(*) FROM sys.jobs",
		"Required for active queries (Queries tab)")

	// 8. sys.allocations
	checkTable(ctx, registry, "sys.allocations",
		"SELECT count(*) FROM sys.allocations",
		"Required for shard allocation reasons (Shards tab, CrateDB 4.2+)")

	// 9. information_schema.tables
	var totalTables, viewCount, baseCount int
	if resp, err := registry.Query(ctx, `SELECT table_type, count(*) FROM information_schema.tables
		WHERE table_schema NOT IN ('sys', 'information_schema', 'pg_catalog', 'blob')
		GROUP BY table_type`); err != nil {
		printCheck(false, "information_schema.tables", err.Error())
	} else {
		for _, row := range resp.Rows {
			tt := cratedb.ToString(row[0])
			cnt := int(cratedb.ToFloat64(row[1]))
			totalTables += cnt
			switch tt {
			case "VIEW":
				viewCount = cnt
			case "BASE TABLE":
				baseCount = cnt
			}
		}
		printCheck(true, "information_schema.tables", fmt.Sprintf("%d tables, %d views", baseCount, viewCount))
	}

	// 10. Current user and privileges
	fmt.Println()
	var currentUser string
	if resp, err := registry.Query(ctx, "SELECT CURRENT_USER"); err == nil && len(resp.Rows) > 0 {
		currentUser = cratedb.ToString(resp.Rows[0][0])
	}

	isSuperuser := currentUser == "crate"
	fmt.Printf("  User: %s", currentUser)
	if isSuperuser {
		fmt.Printf(" (superuser)")
	}
	fmt.Println()

	if resp, err := registry.Query(ctx, `SELECT class, type, state, ident
		FROM sys.privileges WHERE grantee = ?`, currentUser); err == nil {
		if len(resp.Rows) == 0 && !isSuperuser {
			fmt.Println("  Privileges: none found (may lack DQL on sys.privileges)")
		} else if len(resp.Rows) > 0 {
			var grants, denies []string
			for _, row := range resp.Rows {
				class := cratedb.ToString(row[0])
				ptype := cratedb.ToString(row[1])
				state := cratedb.ToString(row[2])
				ident := cratedb.ToString(row[3])

				target := class
				if ident != "" {
					target = fmt.Sprintf("%s %q", class, ident)
				}
				entry := fmt.Sprintf("%s on %s", ptype, target)
				if state == "GRANT" {
					grants = append(grants, entry)
				} else {
					denies = append(denies, entry)
				}
			}
			if len(grants) > 0 {
				fmt.Printf("  Grants: %s\n", strings.Join(grants, ", "))
			}
			if len(denies) > 0 {
				fmt.Printf("  Denies: %s\n", strings.Join(denies, ", "))
			}
		}
	}

	fmt.Println()
}

func checkScalar(ctx context.Context, registry *cratedb.Registry, name, query, purpose string) {
	_, err := registry.Query(ctx, query)
	if err != nil {
		errMsg := err.Error()
		if idx := strings.Index(errMsg, "message:"); idx >= 0 {
			errMsg = strings.TrimSpace(errMsg[idx+8:])
		}
		printCheck(false, name, fmt.Sprintf("%s — %s", errMsg, purpose))
		return
	}
	printCheck(true, name, "ok")
}

func checkTable(ctx context.Context, registry *cratedb.Registry, name, query, purpose string) {
	resp, err := registry.Query(ctx, query)
	if err != nil {
		errMsg := err.Error()
		if idx := strings.Index(errMsg, "message:"); idx >= 0 {
			errMsg = strings.TrimSpace(errMsg[idx+8:])
		}
		printCheck(false, name, fmt.Sprintf("%s — %s", errMsg, purpose))
		return
	}

	detail := "ok"
	if len(resp.Rows) > 0 {
		count := int(cratedb.ToFloat64(resp.Rows[0][0]))
		if count == 0 {
			detail = fmt.Sprintf("0 rows — %s", purpose)
			printWarn(name, detail)
			return
		}
		detail = fmt.Sprintf("%d rows", count)
	}
	printCheck(true, name, detail)
}

func printCheck(ok bool, name, detail string) {
	if ok {
		fmt.Printf("  \033[32m✓\033[0m %-28s %s\n", name, detail)
	} else {
		fmt.Printf("  \033[31m✗\033[0m %-28s %s\n", name, detail)
	}
}

func printWarn(name, detail string) {
	fmt.Printf("  \033[33m!\033[0m %-28s %s\n", name, detail)
}

