package jmx

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Parse reads a Prometheus text exposition and returns its samples plus the
// croudng metadata header if present. HELP/TYPE comments are skipped.
func Parse(r io.Reader) (*Scrape, error) {
	s := &Scrape{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<22) // accommodate long label lines
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if line[0] == '#' {
			if meta, ok := parseCroudngMeta(line); ok {
				s.Meta = meta
			}
			continue
		}
		sample, err := parseSample(line)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNo, err)
		}
		s.Samples = append(s.Samples, sample)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return s, nil
}

// parseSample parses a single "name[{labels}] value [timestamp]" line.
func parseSample(line string) (Sample, error) {
	var s Sample
	i := 0
	for i < len(line) && isNameChar(line[i]) {
		i++
	}
	if i == 0 {
		return s, errors.New("missing metric name")
	}
	s.Name = line[:i]
	if i < len(line) && line[i] == '{' {
		labels, n, err := parseLabels(line[i:])
		if err != nil {
			return s, err
		}
		s.Labels = labels
		i += n
	}
	rest := strings.Fields(line[i:])
	if len(rest) < 1 || len(rest) > 2 {
		return s, fmt.Errorf("expected value [timestamp], got %q", strings.TrimSpace(line[i:]))
	}
	v, err := strconv.ParseFloat(rest[0], 64)
	if err != nil {
		return s, fmt.Errorf("invalid value %q: %w", rest[0], err)
	}
	s.Value = v
	if len(rest) == 2 {
		ts, err := strconv.ParseInt(rest[1], 10, 64)
		if err != nil {
			return s, fmt.Errorf("invalid timestamp %q: %w", rest[1], err)
		}
		s.Timestamp = ts
	}
	return s, nil
}

// parseLabels consumes a "{k="v",...}" block and returns the byte count read.
func parseLabels(s string) (map[string]string, int, error) {
	if s == "" || s[0] != '{' {
		return nil, 0, errors.New("expected '{'")
	}
	labels := map[string]string{}
	i := 1
	for i < len(s) {
		for i < len(s) && (s[i] == ' ' || s[i] == ',') {
			i++
		}
		if i < len(s) && s[i] == '}' {
			return labels, i + 1, nil
		}
		nameStart := i
		for i < len(s) && isNameChar(s[i]) {
			i++
		}
		if i == nameStart {
			return nil, 0, fmt.Errorf("expected label name at offset %d", i)
		}
		name := s[nameStart:i]
		if i >= len(s) || s[i] != '=' {
			return nil, 0, fmt.Errorf("expected '=' after label %q", name)
		}
		i++
		if i >= len(s) || s[i] != '"' {
			return nil, 0, fmt.Errorf("expected '\"' for label %q value", name)
		}
		i++
		var b strings.Builder
		closed := false
		for i < len(s) {
			c := s[i]
			if c == '\\' && i+1 < len(s) {
				switch s[i+1] {
				case '\\':
					b.WriteByte('\\')
				case '"':
					b.WriteByte('"')
				case 'n':
					b.WriteByte('\n')
				default:
					b.WriteByte(s[i+1])
				}
				i += 2
				continue
			}
			if c == '"' {
				i++
				closed = true
				break
			}
			b.WriteByte(c)
			i++
		}
		if !closed {
			return nil, 0, fmt.Errorf("unterminated value for label %q", name)
		}
		labels[name] = b.String()
	}
	return nil, 0, errors.New("unterminated label block")
}

// isNameChar matches the Prometheus name grammar: [a-zA-Z0-9_:].
// The leading-digit case is rejected upstream by requiring at least one char.
func isNameChar(c byte) bool {
	return c == '_' || c == ':' ||
		(c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9')
}

var (
	scrapedAtRe = regexp.MustCompile(`scraped at (\S+?)(?:[,\s]|$)`)
	ageRe       = regexp.MustCompile(`\bage (\d+)s\b`)
)

// parseCroudngMeta extracts metadata from croudng's leading "# croudng: ..."
// line. Unknown or absent tokens leave the corresponding fields zero-valued.
func parseCroudngMeta(line string) (ScrapeMeta, bool) {
	if !strings.HasPrefix(line, "# croudng:") {
		return ScrapeMeta{}, false
	}
	m := ScrapeMeta{
		Cached:      strings.Contains(line, "served from cache"),
		RateLimited: strings.Contains(line, "rate limited"),
	}
	if match := scrapedAtRe.FindStringSubmatch(line); match != nil {
		if t, err := time.Parse(time.RFC3339, strings.TrimRight(match[1], ",")); err == nil {
			m.ScrapedAt = t
		}
	}
	if match := ageRe.FindStringSubmatch(line); match != nil {
		if n, err := strconv.Atoi(match[1]); err == nil {
			m.UpstreamAge = time.Duration(n) * time.Second
		}
	}
	return m, true
}
