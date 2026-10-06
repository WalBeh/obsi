package tui

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// applySetting runs SET GLOBAL for the settings editor. A TRANSIENT value
// wins over a PERSISTENT one until the next full restart, and sys.cluster
// only shows the value in effect, so a PERSISTENT set can succeed and change
// nothing (seen with a transient max_bytes_per_sec left by the recovery
// throttle form). When the value in effect doesn't move although a different
// one was asked for, the same value is set TRANSIENT as well.
func applySetting(ctx context.Context, reg *cratedb.Registry, msg SetSettingMsg) SetSettingResultMsg {
	res := SetSettingResultMsg{SlotIndex: msg.SlotIndex}
	set := func(mode string) error {
		_, err := reg.Query(ctx, fmt.Sprintf(`SET GLOBAL %s "%s" = ?`, mode, msg.SettingPath)+cratedb.ChangeTag, msg.Value)
		return err
	}
	if !msg.Persistent {
		if err := set("TRANSIENT"); err != nil {
			res.Error = err.Error()
		}
		return res
	}

	before, beforeErr := effectiveSetting(ctx, reg, msg.SettingPath)
	if err := set("PERSISTENT"); err != nil {
		res.Error = err.Error()
		return res
	}
	if beforeErr != nil || strings.EqualFold(strings.TrimSpace(msg.Value), before) {
		return res
	}
	after, err := effectiveSetting(ctx, reg, msg.SettingPath)
	if err != nil || after != before {
		return res
	}
	if err := set("TRANSIENT"); err != nil {
		res.Error = fmt.Sprintf("set PERSISTENT, but a TRANSIENT %s still wins until restart: %v", before, err)
		return res
	}
	if now, err := effectiveSetting(ctx, reg, msg.SettingPath); err == nil && now == before {
		// Same value in another spelling (1g vs 1gb): nothing was overridden,
		// the TRANSIENT copy is redundant.
		return res
	}
	res.Note = fmt.Sprintf("a TRANSIENT %s was overriding it, so %s is set TRANSIENT too", before, msg.Value)
	return res
}

// effectiveSetting reads the value in effect for a dotted setting path.
func effectiveSetting(ctx context.Context, reg *cratedb.Registry, path string) (string, error) {
	var b strings.Builder
	b.WriteString("SELECT settings")
	for _, p := range strings.Split(path, ".") {
		b.WriteString("['" + strings.ReplaceAll(p, "'", "''") + "']")
	}
	b.WriteString(" FROM sys.cluster")
	resp, err := reg.Query(ctx, b.String()+cratedb.QueryTag)
	if err != nil {
		return "", err
	}
	if len(resp.Rows) == 0 || len(resp.Rows[0]) == 0 {
		return "", fmt.Errorf("no value for %s", path)
	}
	switch v := resp.Rows[0][0].(type) {
	case string:
		return v, nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	default:
		return fmt.Sprint(v), nil
	}
}
