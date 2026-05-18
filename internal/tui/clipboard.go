package tui

import (
	"os"

	"github.com/aymanbagabas/go-osc52/v2"
)

// writeClipboard emits an OSC 52 escape sequence so the host terminal copies
// `s` to the system clipboard. Works locally and over SSH (provided the
// terminal emulator has OSC 52 enabled — iTerm2, kitty, WezTerm, modern
// xterm, tmux with `set -g set-clipboard on`, ...).
//
// Bubbletea 1.3.10 doesn't expose a clipboard command, so we write the
// sequence to stderr directly: terminal emulators intercept OSC 52 from
// any stream, and stderr is unbuffered so the sequence arrives immediately
// without disturbing the bubbletea-managed stdout render loop.
//
// Returns an empty string on success, or a human-readable error message.
func writeClipboard(s string) string {
	if _, err := osc52.New(s).WriteTo(os.Stderr); err != nil {
		return err.Error()
	}
	return ""
}
