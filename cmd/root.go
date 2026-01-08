package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"surge/internal/tui"

	tea "github.com/charmbracelet/bubbletea"
)

// Version information - set via ldflags during build
var (
	Version   = "dev"
	BuildTime = "unknown"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:     "surge",
	Short:   "An open-source download manager written in Go",
	Long:    `Surge is a fast, concurrent download manager with pause/resume support.`,
	Version: Version,
	Run: func(cmd *cobra.Command, args []string) {
		p := tea.NewProgram(tui.InitialRootModel(), tea.WithAltScreen())
		if _, err := p.Run(); err != nil {
			fmt.Printf("Alas, there's been an error: %v", err)
			os.Exit(1)
		}
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(getCmd)

	// Set version template
	rootCmd.SetVersionTemplate("Surge version {{.Version}}\n")
}
