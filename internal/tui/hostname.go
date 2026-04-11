package tui

import (
	"regexp"
	"strings"
)

// CrateDB Cloud K8s pod hostname patterns:
// crate-data-hot-b7ce0f7f-21aa-4066-8728-3f1d1cc8008a-0
// crate-data-cold-<uuid>-N
// crate-master-<uuid>-N
var crateCloudHostnameRe = regexp.MustCompile(
	`^crate-((?:data-(?:hot|warm|cold)|master))-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-(\d+)$`,
)

// Generic long hostnames with UUID segments
var uuidSegmentRe = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)

// shortenHostname produces a display-friendly hostname.
// For CrateDB Cloud K8s pods: "crate-data-hot-<uuid>-2" → "data-hot-2"
// For other long hostnames: truncate with ellipsis.
func shortenHostname(hostname string, maxLen int) string {
	// CrateDB Cloud pattern
	if m := crateCloudHostnameRe.FindStringSubmatch(hostname); m != nil {
		return m[1] + "-" + m[2] // e.g. "data-hot-2"
	}

	// Strip UUID segments from other hostnames
	if uuidSegmentRe.MatchString(hostname) {
		short := uuidSegmentRe.ReplaceAllString(hostname, "..")
		// Clean up resulting double dashes/dots
		short = strings.ReplaceAll(short, "-..-", "-")
		short = strings.TrimRight(short, "-.")
		if len(short) <= maxLen {
			return short
		}
	}

	// Simple truncation for anything still too long
	if len(hostname) > maxLen {
		return hostname[:maxLen-2] + ".."
	}
	return hostname
}
