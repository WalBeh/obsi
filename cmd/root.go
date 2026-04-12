package cmd

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/spf13/cobra"

	"github.com/waltergrande/cratedb-observer/internal/collector"
	"github.com/waltergrande/cratedb-observer/internal/config"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
	"github.com/waltergrande/cratedb-observer/internal/tui"
)

var (
	endpoint   string
	username   string
	password   string
	configPath string
	skipVerify bool
	profile    string
)

func newRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "obsi [endpoint|profile]",
		Short: "CrateDB cluster observer TUI",
		Long:  "A terminal-based monitoring tool for CrateDB clusters.",
		Args:  cobra.MaximumNArgs(1),
		RunE:  runRoot,
	}

	cmd.PersistentFlags().StringVar(&endpoint, "endpoint", "", "CrateDB URL, e.g. https://user:pass@host:4200")
	cmd.PersistentFlags().StringVar(&username, "username", "", "CrateDB username (overrides URL userinfo)")
	cmd.PersistentFlags().StringVar(&password, "password", "", "CrateDB password (overrides URL userinfo)")
	cmd.PersistentFlags().StringVar(&configPath, "config", defaultConfigPath(), "Path to TOML config file")
	cmd.PersistentFlags().BoolVar(&skipVerify, "skip-verify", false, "Skip TLS certificate verification")
	cmd.PersistentFlags().StringVar(&profile, "profile", "", "Named cluster profile from config")

	cmd.AddCommand(doctorCmd)
	cmd.AddCommand(profilesCmd)
	return cmd
}

func Execute() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

// resolvePositionalArg interprets a positional argument as either a URL or profile name.
func resolvePositionalArg(args []string) {
	if len(args) == 0 {
		return
	}
	arg := args[0]
	if strings.Contains(arg, "://") {
		if endpoint == "" {
			endpoint = arg
		}
	} else if profile == "" {
		profile = arg
	}
}

// resolveConnection loads config, resolves profile/endpoint/credentials, connects,
// and returns the registry. Shared by root and doctor commands.
func resolveConnection(ctx context.Context, cmd *cobra.Command, args []string) (*config.Config, *cratedb.Registry, error) {
	resolvePositionalArg(args)

	passwordSet := cmd.Flags().Lookup("password").Changed
	usernameSet := cmd.Flags().Lookup("username").Changed

	// Load config
	cfg, err := config.Load(configPath)
	if err != nil {
		return nil, nil, fmt.Errorf("error loading config: %w", err)
	}

	// Resolve profile: explicit --profile, positional name, or last_profile from config
	if profile == "" && endpoint == "" && cfg.LastProfile != "" {
		profile = cfg.LastProfile
	}

	// Load profile connection settings if a profile is specified
	if profile != "" && cfg.Profiles != nil {
		if p, ok := cfg.Profiles[profile]; ok {
			if endpoint == "" {
				cfg.Connection.Endpoint = p.Endpoint
			}
			if !usernameSet && username == "" {
				cfg.Connection.Username = p.Username
			}
		} else if endpoint == "" {
			msg := fmt.Sprintf("profile %q not found in config", profile)
			if len(cfg.Profiles) > 0 {
				names := make([]string, 0, len(cfg.Profiles))
				for name := range cfg.Profiles {
					names = append(names, name)
				}
				msg += fmt.Sprintf("\navailable profiles: %s", strings.Join(names, ", "))
			}
			return nil, nil, fmt.Errorf("%s", msg)
		}
	}

	// Parse endpoint URL — extract embedded credentials if present
	if endpoint != "" {
		parsedEndpoint, parsedUser, parsedPass, hasAuth := parseEndpointURL(endpoint)
		cfg.Connection.Endpoint = parsedEndpoint
		if hasAuth {
			if !usernameSet {
				username = parsedUser
			}
			if !passwordSet {
				password = parsedPass
				passwordSet = true
			}
		}
	}
	if usernameSet || username != "" {
		cfg.Connection.Username = username
	}

	// Save profile if --profile was given with a URL (first-time setup)
	if profile != "" && endpoint != "" {
		if cfg.Profiles == nil {
			cfg.Profiles = make(map[string]config.ProfileConfig)
		}
		cfg.Profiles[profile] = config.ProfileConfig{
			Endpoint: cfg.Connection.Endpoint,
			Username: cfg.Connection.Username,
		}
		if passwordSet && password != "" {
			_ = config.StorePassword(cfg.Connection.Endpoint, cfg.Connection.Username, password)
		}
		cfg.LastProfile = profile
		if err := config.Save(configPath, cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: could not save profile: %v\n", err)
		}
	} else if profile != "" {
		if cfg.LastProfile != profile {
			cfg.LastProfile = profile
			_ = config.Save(configPath, cfg)
		}
	}

	// Resolve password
	var pw string
	if passwordSet {
		pw = password
	} else {
		pw, err = config.ResolvePassword(&cfg.Connection)
		if err != nil {
			return nil, nil, fmt.Errorf("error resolving password: %w", err)
		}
	}

	// Setup logging
	setupLogging(cfg.Logging)

	// Try connecting
	registry, err := tryConnect(ctx, cfg, pw, passwordSet, skipVerify)
	if err != nil {
		return nil, nil, fmt.Errorf("error connecting to CrateDB: %w", err)
	}

	return cfg, registry, nil
}

func runRoot(cmd *cobra.Command, args []string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg, registry, err := resolveConnection(ctx, cmd, args)
	if err != nil {
		return err
	}

	// Start heartbeat and node refresh
	registry.Start(ctx)

	// Create store
	st := store.New(cfg.TUI.SparklineHistory, cfg.Collectors)

	// Create query tracker and attach to registry
	tracker := collector.NewQueryTracker(cfg.Collectors)
	registry.SetRecorder(tracker)

	// Create and start collectors
	collectors := collector.DefaultCollectors(cfg.Collectors, tracker)
	mgr := collector.NewManager(registry, st, tracker, collectors...)
	mgr.Start(ctx)

	// Create and run TUI
	app := tui.NewApp(st, registry, mgr, ctx, cfg.TUI.RefreshRate.Duration)
	p := tea.NewProgram(app, tea.WithAltScreen())

	// Run TUI (blocks until quit)
	if _, err := p.Run(); err != nil {
		return fmt.Errorf("error running TUI: %w", err)
	}

	// Cleanup
	cancel()
	mgr.Stop()
	return nil
}

func defaultConfigPath() string {
	configDir, err := os.UserConfigDir()
	if err != nil {
		return "config.toml"
	}
	dir := filepath.Join(configDir, "obsi")
	_ = os.MkdirAll(dir, 0o755)
	return filepath.Join(dir, "config.toml")
}

func setupLogging(cfg config.LoggingConfig) {
	var level slog.Level
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelWarn
	}

	opts := &slog.HandlerOptions{Level: level}

	var handler slog.Handler
	if cfg.File != "" {
		f, err := os.OpenFile(cfg.File, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: cannot open log file %s: %v\n", cfg.File, err)
			handler = slog.NewTextHandler(os.Stderr, opts)
		} else {
			handler = slog.NewTextHandler(f, opts)
		}
	} else {
		handler = slog.NewTextHandler(io.Discard, opts)
	}

	slog.SetDefault(slog.New(handler))
}

// parseEndpointURL extracts credentials from a URL like https://user:pass@host:4200
// and returns the clean endpoint URL (without userinfo), username, password, and whether auth was present.
func parseEndpointURL(raw string) (endpoint, username, password string, hasAuth bool) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return raw, "", "", false
	}

	if u.User != nil {
		username = u.User.Username()
		password, _ = u.User.Password()
		hasAuth = true
		u.User = nil
	}

	return u.String(), username, password, hasAuth
}

func tryConnect(ctx context.Context, cfg *config.Config, pw string, passwordExplicit, skipVerify bool) (*cratedb.Registry, error) {
	makeRegistry := func(password string) *cratedb.Registry {
		return cratedb.NewRegistry(
			cfg.Connection.Endpoint,
			cfg.Connection.Username,
			password,
			cfg.Connection.Timeout.Duration,
			cfg.Connection.QueryTimeout.Duration,
			cfg.Connection.HeartbeatInterval.Duration,
			cfg.Connection.NodeRefreshInterval.Duration,
			skipVerify,
		)
	}

	// If password was explicitly provided (flag, env, keyring), just try it
	if passwordExplicit {
		reg := makeRegistry(pw)
		slog.Info("connecting to CrateDB", "endpoint", cfg.Connection.Endpoint)
		if err := reg.Bootstrap(ctx); err != nil {
			return nil, err
		}
		return reg, nil
	}

	// Try with resolved password first (may be empty if nothing was found)
	reg := makeRegistry(pw)
	slog.Info("connecting to CrateDB", "endpoint", cfg.Connection.Endpoint)
	if err := reg.Bootstrap(ctx); err == nil {
		return reg, nil
	}

	// If resolved password was non-empty, also try empty password
	if pw != "" {
		reg = makeRegistry("")
		fmt.Fprintf(os.Stderr, "Trying empty password...\n")
		if err := reg.Bootstrap(ctx); err == nil {
			return reg, nil
		}
	}

	// Prompt interactively as last resort
	if config.IsTerminal() {
		prompted, err := config.PromptPassword(cfg.Connection.Endpoint, cfg.Connection.Username)
		if err != nil {
			return nil, fmt.Errorf("all connection attempts failed")
		}
		reg = makeRegistry(prompted)
		if err := reg.Bootstrap(ctx); err != nil {
			return nil, err
		}
		_ = config.StorePassword(cfg.Connection.Endpoint, cfg.Connection.Username, prompted)
		return reg, nil
	}

	return nil, fmt.Errorf("authentication failed and no terminal available for password prompt")
}
