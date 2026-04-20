package tui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// SetSettingMsg requests the App to execute a SET GLOBAL statement.
type SetSettingMsg struct {
	SettingPath string
	Value       string
	SlotIndex   int
	Persistent  bool
}

// SetSettingResultMsg carries the result back.
type SetSettingResultMsg struct {
	SlotIndex int
	Error     string
}

// slotKind determines how a slot is edited.
type slotKind int

const (
	slotFreeText slotKind = iota
	slotPicker
)

// Slot index constants for type-safe access.
const (
	slotWMLow          = 0
	slotWMHigh         = 1
	slotWMFlood        = 2
	slotAlloc          = 3
	slotRebalance      = 4
	slotMaxShards      = 5
	slotRecoveryBytes  = 6
	slotRecoveryNode   = 7
	slotRecoveryClust  = 8
)

// editSlot defines one editable cluster setting.
type editSlot struct {
	settingPath string
	kind        slotKind
	options     []string // for slotPicker
	value       string   // current value synced from ClusterSettings
}

// settingsEditor manages the inline edit-mode state machine.
type settingsEditor struct {
	slots  []editSlot
	cursor int
	active bool // edit mode entered

	// Input state (when editing a slot)
	inputActive bool
	inputBuf    string
	pickerIdx   int

	// Feedback
	successSlot int
	successAt   time.Time
	errorSlot   int
	errorMsg    string
	errorAt     time.Time

	// Config
	persistent bool // SET GLOBAL PERSISTENT vs TRANSIENT

	keyMap KeyMap
}

func newSettingsEditor(persistent bool) settingsEditor {
	slots := make([]editSlot, 9)
	slots[slotWMLow] = editSlot{settingPath: "cluster.routing.allocation.disk.watermark.low", kind: slotFreeText}
	slots[slotWMHigh] = editSlot{settingPath: "cluster.routing.allocation.disk.watermark.high", kind: slotFreeText}
	slots[slotWMFlood] = editSlot{settingPath: "cluster.routing.allocation.disk.watermark.flood_stage", kind: slotFreeText}
	slots[slotAlloc] = editSlot{settingPath: "cluster.routing.allocation.enable", kind: slotPicker, options: []string{"all", "new_primaries", "primaries", "none"}}
	slots[slotRebalance] = editSlot{settingPath: "cluster.routing.rebalance.enable", kind: slotPicker, options: []string{"all", "primaries", "replicas", "none"}}
	slots[slotMaxShards] = editSlot{settingPath: "cluster.max_shards_per_node", kind: slotFreeText}
	slots[slotRecoveryBytes] = editSlot{settingPath: "indices.recovery.max_bytes_per_sec", kind: slotFreeText}
	slots[slotRecoveryNode] = editSlot{settingPath: "cluster.routing.allocation.node_concurrent_recoveries", kind: slotFreeText}
	slots[slotRecoveryClust] = editSlot{settingPath: "cluster.routing.allocation.cluster_concurrent_rebalance", kind: slotFreeText}

	return settingsEditor{
		slots:       slots,
		successSlot: -1,
		errorSlot:   -1,
		persistent:  persistent,
		keyMap:      DefaultKeyMap(),
	}
}

// syncFromSettings updates slot values from current cluster settings.
// Skips the slot currently being edited to avoid overwriting user input.
func (e *settingsEditor) syncFromSettings(cs cratedb.ClusterSettings) {
	set := func(idx int, val string) {
		if e.inputActive && e.cursor == idx {
			return
		}
		e.slots[idx].value = val
	}
	set(slotWMLow, cs.DiskWatermarkLow)
	set(slotWMHigh, cs.DiskWatermarkHigh)
	set(slotWMFlood, cs.DiskWatermarkFlood)
	set(slotAlloc, cs.AllocationEnable)
	set(slotRebalance, cs.RebalanceEnable)
	set(slotMaxShards, fmt.Sprintf("%d", cs.MaxShardsPerNode))
	set(slotRecoveryBytes, cs.RecoveryMaxBytesPerSec)
	set(slotRecoveryNode, fmt.Sprintf("%d", cs.NodeConcurrentRecoveries))
	set(slotRecoveryClust, fmt.Sprintf("%d", cs.ClusterConcurrentRebalance))

	// Expire stale feedback
	if e.successSlot >= 0 && time.Since(e.successAt) > 3*time.Second {
		e.successSlot = -1
	}
	if e.errorSlot >= 0 && time.Since(e.errorAt) > 5*time.Second {
		e.errorSlot = -1
		e.errorMsg = ""
	}
}

// isInputMode returns true when the editor is consuming text/picker input.
func (e *settingsEditor) isInputMode() bool {
	return e.active && e.inputActive
}

// handleKey processes key events when the editor is active.
// Returns the editor state, a command (if any), and whether the key was consumed.
func (e settingsEditor) handleKey(msg tea.KeyMsg) (settingsEditor, tea.Cmd, bool) {
	if !e.active {
		if key.Matches(msg, e.keyMap.Edit) {
			e.active = true
			e.cursor = 0
			return e, nil, true
		}
		return e, nil, false
	}

	// Editing a specific slot
	if e.inputActive {
		return e.handleInputKey(msg)
	}

	// Navigating between slots
	switch {
	case key.Matches(msg, e.keyMap.Escape):
		e.active = false
		return e, nil, true
	case msg.Type == tea.KeyTab:
		e.cursor = (e.cursor + 1) % len(e.slots)
		return e, nil, true
	case msg.Type == tea.KeyShiftTab:
		e.cursor = (e.cursor - 1 + len(e.slots)) % len(e.slots)
		return e, nil, true
	case msg.Type == tea.KeyEnter:
		e.activateInput()
		return e, nil, true
	case key.Matches(msg, e.keyMap.Up):
		e.cursor = (e.cursor - 1 + len(e.slots)) % len(e.slots)
		return e, nil, true
	case key.Matches(msg, e.keyMap.Down):
		e.cursor = (e.cursor + 1) % len(e.slots)
		return e, nil, true
	}

	return e, nil, true // consume all keys in edit mode
}

func (e *settingsEditor) activateInput() {
	slot := e.slots[e.cursor]
	e.inputActive = true
	if slot.kind == slotPicker {
		e.pickerIdx = 0
		for i, opt := range slot.options {
			if opt == slot.value {
				e.pickerIdx = i
				break
			}
		}
	} else {
		e.inputBuf = slot.value
	}
}

func (e settingsEditor) handleInputKey(msg tea.KeyMsg) (settingsEditor, tea.Cmd, bool) {
	slot := e.slots[e.cursor]

	if key.Matches(msg, e.keyMap.Escape) {
		e.inputActive = false
		e.inputBuf = ""
		return e, nil, true
	}

	if slot.kind == slotPicker {
		return e.handlePickerKey(msg)
	}
	return e.handleFreeTextKey(msg)
}

func (e settingsEditor) handlePickerKey(msg tea.KeyMsg) (settingsEditor, tea.Cmd, bool) {
	slot := e.slots[e.cursor]
	switch {
	case key.Matches(msg, e.keyMap.Up):
		if e.pickerIdx > 0 {
			e.pickerIdx--
		}
		return e, nil, true
	case key.Matches(msg, e.keyMap.Down):
		if e.pickerIdx < len(slot.options)-1 {
			e.pickerIdx++
		}
		return e, nil, true
	case msg.Type == tea.KeyEnter:
		selected := slot.options[e.pickerIdx]
		if selected == slot.value {
			e.inputActive = false
			return e, nil, true
		}
		e.inputActive = false
		cmd := e.setCmd(slot.settingPath, selected, e.cursor)
		return e, cmd, true
	}
	return e, nil, true
}

func (e settingsEditor) handleFreeTextKey(msg tea.KeyMsg) (settingsEditor, tea.Cmd, bool) {
	slot := e.slots[e.cursor]
	switch msg.Type {
	case tea.KeyEnter:
		val := strings.TrimSpace(e.inputBuf)
		if val == "" || val == slot.value {
			e.inputActive = false
			e.inputBuf = ""
			return e, nil, true
		}
		e.inputActive = false
		e.inputBuf = ""
		cmd := e.setCmd(slot.settingPath, val, e.cursor)
		return e, cmd, true
	case tea.KeyBackspace:
		if len(e.inputBuf) > 0 {
			e.inputBuf = e.inputBuf[:len(e.inputBuf)-1]
		}
		return e, nil, true
	case tea.KeyRunes:
		e.inputBuf += string(msg.Runes)
		return e, nil, true
	}
	return e, nil, true
}

// setCmd builds a tea.Cmd that emits a SetSettingMsg.
func (e *settingsEditor) setCmd(settingPath, value string, slotIdx int) tea.Cmd {
	persistent := e.persistent
	return func() tea.Msg {
		return SetSettingMsg{
			SettingPath: settingPath,
			Value:       value,
			SlotIndex:   slotIdx,
			Persistent:  persistent,
		}
	}
}

// handleResult processes the result of a SET GLOBAL command.
func (e *settingsEditor) handleResult(msg SetSettingResultMsg) {
	if msg.Error != "" {
		e.errorSlot = msg.SlotIndex
		e.errorMsg = msg.Error
		e.errorAt = time.Now()
		e.successSlot = -1
	} else {
		e.successSlot = msg.SlotIndex
		e.successAt = time.Now()
		e.errorSlot = -1
		e.errorMsg = ""
	}
}

// renderValue renders a single slot value with appropriate styling for edit mode.
func (e *settingsEditor) renderValue(slotIdx int, displayValue string) string {
	if !e.active {
		return displayValue
	}

	// Success indicator
	if e.successSlot == slotIdx {
		return styleHealthGreen.Render(displayValue + " ✓")
	}
	// Error indicator
	if e.errorSlot == slotIdx {
		return styleHealthRed.Render(displayValue + " ✗")
	}

	if e.cursor != slotIdx {
		return displayValue
	}

	// This is the focused slot
	if e.inputActive {
		slot := e.slots[slotIdx]
		if slot.kind == slotPicker {
			return styleEditCursor.Render(slot.options[e.pickerIdx])
		}
		return styleEditInput.Render(e.inputBuf + "▏")
	}

	return styleEditCursor.Render(displayValue)
}

// renderPicker renders the vertical picker dropdown if active on the given slot.
func (e *settingsEditor) renderPicker(slotIdx int, indent string) string {
	if !e.active || !e.inputActive || e.cursor != slotIdx {
		return ""
	}
	slot := e.slots[slotIdx]
	if slot.kind != slotPicker {
		return ""
	}

	var lines []string
	for i, opt := range slot.options {
		if i == e.pickerIdx {
			lines = append(lines, indent+stylePickerActive.Render(" "+opt+" "))
		} else {
			lines = append(lines, indent+stylePickerItem.Render(" "+opt+" "))
		}
	}
	return strings.Join(lines, "\n")
}

// renderEditHint returns a hint line shown when edit mode is active.
func (e *settingsEditor) renderEditHint() string {
	if !e.active {
		return ""
	}
	if e.inputActive {
		return styleDim.Render("  [Enter] confirm  [Esc] cancel")
	}
	mode := "PERSISTENT"
	if !e.persistent {
		mode = "TRANSIENT"
	}
	return styleDim.Render(fmt.Sprintf("  [Tab/↑↓] navigate  [Enter] edit  [Esc] exit  (%s)", mode))
}

// renderError returns the error message if one is active.
func (e *settingsEditor) renderError() string {
	if e.errorSlot < 0 {
		return ""
	}
	return "  " + styleHealthRed.Render("Error: "+e.errorMsg)
}
