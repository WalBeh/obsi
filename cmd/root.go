package cmd

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/waltergrande/cratedb-observer/internal/collector"
	"github.com/waltergrande/cratedb-observer/internal/config"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
	"github.com/waltergrande/cratedb-observer/internal/tui"
)

func Execute() {
	var (
		endpoint   string
		username   string
		password   string
		configPath string
		skipVerify bool
		doctor       bool
		profile      string
		listProfiles bool
	)

	flag.StringVar(&endpoint, "endpoint", "", "CrateDB URL, e.g. https://user:pass@host:4200")
	flag.StringVar(&username, "username", "", "CrateDB username (overrides URL userinfo)")
	flag.StringVar(&password, "password", "", "CrateDB password (overrides URL userinfo)")
	flag.StringVar(&configPath, "config", defaultConfigPath(), "Path to TOML config file")
	flag.BoolVar(&skipVerify, "skip-verify", false, "Skip TLS certificate verification (for port-forwarding)")
	flag.BoolVar(&doctor, "doctor", false, "Check connectivity, permissions, and exit")
	flag.StringVar(&profile, "profile", "", "Named cluster profile from config")
	flag.BoolVar(&listProfiles, "list-profiles", false, "List saved profiles and exit")

	// Reorder os.Args so flags can appear anywhere (before or after positional URL)
	reorderArgs()
	flag.Parse()

	// Support positional arguments:
	//   obsi https://admin:pass@host:4200       → URL
	//   obsi prod                                → profile name (no scheme = profile)
	//   obsi prod --doctor                       → profile + flag
	if flag.NArg() > 0 {
		arg := flag.Arg(0)
		if strings.Contains(arg, "://") {
			if endpoint == "" {
				endpoint = arg
			}
		} else if profile == "" {
			profile = arg
		}
	}

	// Check which flags were explicitly provided
	passwordSet := false
	usernameSet := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "password" {
			passwordSet = true
		}
		if f.Name == "username" {
			usernameSet = true
		}
	})

	// Load config
	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	// List profiles and exit
	if listProfiles {
		if len(cfg.Profiles) == 0 {
			fmt.Println("No profiles saved.")
			fmt.Println("Create one: obsi https://user:pass@host:4200 --profile <name>")
		} else {
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
		}
		return
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
			fmt.Fprintf(os.Stderr, "Profile %q not found in config.\n", profile)
			if len(cfg.Profiles) > 0 {
				fmt.Fprintf(os.Stderr, "Available profiles:")
				for name := range cfg.Profiles {
					fmt.Fprintf(os.Stderr, " %s", name)
				}
				fmt.Fprintln(os.Stderr)
			}
			os.Exit(1)
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
		// Store password in keyring so future --profile runs don't need the URL
		if passwordSet && password != "" {
			_ = config.StorePassword(cfg.Connection.Endpoint, cfg.Connection.Username, password)
		}
		cfg.LastProfile = profile
		if err := config.Save(configPath, cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: could not save profile: %v\n", err)
		}
	} else if profile != "" {
		// Update last_profile even when loading existing profile
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
			fmt.Fprintf(os.Stderr, "Error resolving password: %v\n", err)
			os.Exit(1)
		}
	}

	// Setup logging
	setupLogging(cfg.Logging)

	// Create context with signal handling
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Try connecting; if password wasn't explicitly set and connection fails,
	// try empty password first, then prompt interactively.
	registry, err := tryConnect(ctx, cfg, pw, passwordSet, skipVerify)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to CrateDB: %v\n", err)
		os.Exit(1)
	}

	// Doctor mode: check permissions and exit
	if doctor {
		RunDoctor(ctx, registry)
		return
	}

	// Start heartbeat and node refresh
	registry.Start(ctx)

	// Create store
	st := store.New(cfg.TUI.SparklineHistory, cfg.Collectors)

	// Create and start collectors
	mgr := collector.NewManager(registry, st, collector.DefaultCollectors(cfg.Collectors)...)
	mgr.Start(ctx)

	// Create and run TUI
	app := tui.NewApp(st, registry, mgr, ctx, cfg.TUI.RefreshRate.Duration)
	p := tea.NewProgram(app, tea.WithAltScreen())

	// Run TUI (blocks until quit)
	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error running TUI: %v\n", err)
	}

	// Cleanup
	cancel()
	mgr.Stop()
}

// reorderArgs moves flags after positional args so Go's flag package can parse them.
// e.g. "obsi https://... --doctor" → "obsi --doctor https://..."
func reorderArgs() {
	args := os.Args[1:]
	var flags, positional []string
	for i := 0; i < len(args); i++ {
		if strings.HasPrefix(args[i], "-") {
			flags = append(flags, args[i])
			// If it's a non-bool flag, grab the next arg as its value
			// Bool flags: --doctor, --skip-verify
			name := strings.TrimLeft(args[i], "-")
			if eq := strings.IndexByte(name, '='); eq >= 0 {
				continue // --flag=value, already consumed
			}
			if name != "doctor" && name != "skip-verify" && name != "list-profiles" && name != "h" && name != "help" && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				i++
				flags = append(flags, args[i])
			}
		} else {
			positional = append(positional, args[i])
		}
	}
	copy(os.Args[1:], append(flags, positional...))
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
		// When no log file, discard logs (TUI owns the terminal)
		handler = slog.NewTextHandler(os.NewFile(0, os.DevNull), opts)
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
		u.User = nil // strip credentials from URL
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
		// Store for next time
		_ = config.StorePassword(cfg.Connection.Endpoint, cfg.Connection.Username, prompted)
		return reg, nil
	}

	return nil, fmt.Errorf("authentication failed and no terminal available for password prompt")
}
