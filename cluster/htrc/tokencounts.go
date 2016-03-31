package htrc

import (
	"github.com/jawher/mow.cli"
)

// Get counts, dump JSON.
func TokenCountsCmd(cmd *cli.Cmd) {

	var htrcPath = cmd.StringArg(
		"HTRC_PATH",
		"path/to/htrc",
		"The HTRC basic features root",
	)

	cmd.Action = func() {
		extractTokenCounts(*htrcPath)
	}

}

// Accumulate token counts.
func extractTokenCounts(path string) map[string]int {

	counts := make(map[string]int)

	// TODO
	println(path)

	return counts

}
