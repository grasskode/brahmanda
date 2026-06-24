package agentui

import "github.com/charmbracelet/lipgloss"

// Color palette. Foreground codes are 256-color ANSI; lipgloss falls
// back to the closest 16-color cell on terminals that don't speak 256.
// Keep the palette tight — three "load-bearing" hues (good/bad/busy)
// plus dim for background data. Anything else dilutes the signal.
var (
	colorOK      = lipgloss.Color("10")  // bright green: succeeded, healthy, prune-ok
	colorErr     = lipgloss.Color("9")   // bright red: failed, dead, unhealthy
	colorBusy    = lipgloss.Color("11")  // bright yellow: in-progress, attention, stale
	colorInfo    = lipgloss.Color("39")  // cyan-blue: PR open, accent labels
	colorDim     = lipgloss.Color("241") // grey: tertiary text, idle data
	colorMagenta = lipgloss.Color("13")  // bright magenta: timed_out (distinct from failed)
)

var (
	styleOK      = lipgloss.NewStyle().Foreground(colorOK).Bold(true)
	styleErr     = lipgloss.NewStyle().Foreground(colorErr).Bold(true)
	styleBusy    = lipgloss.NewStyle().Foreground(colorBusy).Bold(true)
	styleInfo    = lipgloss.NewStyle().Foreground(colorInfo).Bold(true)
	styleDim     = lipgloss.NewStyle().Foreground(colorDim)
	styleMagenta = lipgloss.NewStyle().Foreground(colorMagenta).Bold(true)
	styleTitle   = lipgloss.NewStyle().Bold(true).Foreground(colorInfo)
	styleSubtle  = lipgloss.NewStyle().Foreground(colorDim).Italic(true)
)
