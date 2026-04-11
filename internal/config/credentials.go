package config

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/zalando/go-keyring"
)

const keyringService = "obsi"

// ResolvePassword resolves the CrateDB password without prompting.
// Checks: OBSI_PASSWORD env var, then OS keyring. Returns empty string if neither found.
func ResolvePassword(cfg *ConnectionConfig) (string, error) {
	// Tier 1: Environment variable
	if pw := os.Getenv("OBSI_PASSWORD"); pw != "" {
		return pw, nil
	}

	// Tier 2: OS keyring
	key := keyringKey(cfg.Endpoint, cfg.Username)
	pw, err := keyring.Get(keyringService, key)
	if err == nil && pw != "" {
		return pw, nil
	}

	// Nothing found — caller should try empty password, then prompt
	return "", nil
}

// StorePassword stores a password in the OS keyring.
func StorePassword(endpoint, username, password string) error {
	key := keyringKey(endpoint, username)
	return keyring.Set(keyringService, key, password)
}

// DeletePassword removes a stored password from the OS keyring.
func DeletePassword(endpoint, username string) error {
	key := keyringKey(endpoint, username)
	return keyring.Delete(keyringService, key)
}

// PromptPassword asks the user for a password interactively.
// Ctrl+C during the prompt exits the process cleanly.
func PromptPassword(endpoint, username string) (string, error) {
	// Catch Ctrl+C during prompt
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	defer signal.Stop(sigCh)

	doneCh := make(chan struct{})
	var pw string
	var readErr error

	fmt.Fprintf(os.Stderr, "Password for %s@%s: ", username, endpoint)

	go func() {
		reader := bufio.NewReader(os.Stdin)
		line, err := reader.ReadString('\n')
		pw = strings.TrimSpace(line)
		readErr = err
		close(doneCh)
	}()

	select {
	case <-sigCh:
		fmt.Fprintln(os.Stderr)
		os.Exit(130)
		return "", nil // unreachable
	case <-doneCh:
		return pw, readErr
	}
}

// IsTerminal returns true if stdin is a terminal.
func IsTerminal() bool {
	fi, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

func keyringKey(endpoint, username string) string {
	return username + "@" + endpoint
}
