package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/waltergrande/cratedb-observer/internal/config"
)

var profilesCmd = &cobra.Command{
	Use:   "profiles",
	Short: "List saved cluster profiles",
	Args:  cobra.NoArgs,
	RunE:  runProfiles,
}

func runProfiles(cmd *cobra.Command, args []string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("error loading config: %w", err)
	}

	if len(cfg.Profiles) == 0 {
		fmt.Println("No profiles saved.")
		fmt.Println("Create one: obsi https://user:pass@host:4200 --profile <name>")
		return nil
	}

	fmt.Printf("Profiles in %s:\n\n", configPath)
	for name, p := range cfg.Profiles {
		marker := "  "
		if name == cfg.LastProfile {
			marker = "* "
		}
		keyStatus := "no password in keyring"
		if pw, err := config.ResolvePasswordFor(p.Endpoint, p.Username); err == nil && pw != "" {
			keyStatus = "password in keyring"
		}
		fmt.Printf("  %s%-12s  %s@%s  (%s)\n", marker, name, p.Username, p.Endpoint, keyStatus)
	}
	fmt.Println()
	fmt.Println("  * = last used")
	return nil
}
