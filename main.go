package main

import (
	"runtime/debug"

	"github.com/waltergrande/cratedb-observer/cmd"
)

// version is set by the Makefile and GoReleaser via -ldflags.
var version = "dev"

func main() {
	// go install builds don't pass ldflags; the module version is the tag.
	if bi, ok := debug.ReadBuildInfo(); ok && version == "dev" && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		version = bi.Main.Version
	}
	cmd.Execute(version)
}
