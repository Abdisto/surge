package tui

import "github.com/charmbracelet/bubbles/key"

type InputKeyMap struct {
	Tab   key.Binding
	Enter key.Binding
	Esc   key.Binding
}

type FilePickerKeyMap struct {
	UseDir   key.Binding
	GotoHome key.Binding
	Open     key.Binding
	Cancel   key.Binding
}

var InputKeys = InputKeyMap{
	Enter: key.NewBinding(
		key.WithKeys("enter"),
		key.WithHelp("enter", "start"),
	),
	Esc: key.NewBinding(
		key.WithKeys("esc"),
		key.WithHelp("esc", "cancel"),
	),
}

var FilePickerKeys = FilePickerKeyMap{
	UseDir: key.NewBinding(
		key.WithKeys("."),
		key.WithHelp(".", "use directory"),
	),
	GotoHome: key.NewBinding(
		key.WithKeys("h", "H"),
		key.WithHelp("h", "downloads"),
	),
	Open: key.NewBinding(
		key.WithKeys("enter"),
		key.WithHelp("enter", "open"),
	),
	Cancel: key.NewBinding(
		key.WithKeys("esc"),
		key.WithHelp("esc", "cancel"),
	),
}

// ShortHelp returns keybindings to show in the mini help view
func (k InputKeyMap) ShortHelp() []key.Binding {
	return []key.Binding{k.Tab, k.Enter, k.Esc}
}

// FullHelp returns keybindings for the expanded help view
func (k InputKeyMap) FullHelp() [][]key.Binding {
	return [][]key.Binding{{k.Tab, k.Enter, k.Esc}}
}

// ShortHelp returns keybindings to show in the mini help view
func (k FilePickerKeyMap) ShortHelp() []key.Binding {
	return []key.Binding{k.UseDir, k.GotoHome, k.Open, k.Cancel}
}

// FullHelp returns keybindings for the expanded help view
func (k FilePickerKeyMap) FullHelp() [][]key.Binding {
	return [][]key.Binding{{k.UseDir, k.GotoHome, k.Open, k.Cancel}}
}
