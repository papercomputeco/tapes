package deckcmder

import (
	"github.com/charmbracelet/bubbles/key"

	"github.com/papercomputeco/tapes/pkg/deck"
)

var (
	sortOrder        = []string{sortKeyCost, "date", "tokens", "duration"}
	sortDirOptions   = []string{"asc", sortDirDesc}
	messageSortOrder = []string{"time", "tokens", sortKeyCost, "delta"}
	statusFilters    = []string{"", deck.StatusCompleted, deck.StatusFailed, deck.StatusAbandoned}
)

type deckKeyMap struct {
	Up     key.Binding
	Down   key.Binding
	Enter  key.Binding
	Back   key.Binding
	Sort   key.Binding
	Filter key.Binding
	Search key.Binding
	Period key.Binding
	Replay key.Binding
	Quit   key.Binding
}

func (k deckKeyMap) ShortHelp() []key.Binding {
	return []key.Binding{k.Down, k.Up, k.Enter, k.Back, k.Sort, k.Filter, k.Search, k.Period, k.Replay, k.Quit}
}

func (k deckKeyMap) FullHelp() [][]key.Binding {
	return [][]key.Binding{{k.Down, k.Up, k.Enter, k.Back}, {k.Sort, k.Filter, k.Search, k.Period, k.Replay, k.Quit}}
}

func defaultKeyMap() deckKeyMap {
	return deckKeyMap{
		Up:     key.NewBinding(key.WithKeys("k", "up"), key.WithHelp("k", "up")),
		Down:   key.NewBinding(key.WithKeys("j", "down"), key.WithHelp("j", "down")),
		Enter:  key.NewBinding(key.WithKeys("enter", "l"), key.WithHelp("enter", "drill")),
		Back:   key.NewBinding(key.WithKeys("h", "esc"), key.WithHelp("h", "back")),
		Sort:   key.NewBinding(key.WithKeys("s"), key.WithHelp("s", "sort")),
		Filter: key.NewBinding(key.WithKeys("f"), key.WithHelp("f", "status")),
		Search: key.NewBinding(key.WithKeys("/"), key.WithHelp("/", "search")),
		Period: key.NewBinding(key.WithKeys("p"), key.WithHelp("p", "period")),
		Replay: key.NewBinding(key.WithKeys("r"), key.WithHelp("r", "replay")),
		Quit:   key.NewBinding(key.WithKeys("q", "ctrl+c"), key.WithHelp("q", "quit")),
	}
}
