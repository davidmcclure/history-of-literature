package htrc

import (
	"github.com/jawher/mow.cli"
	"github.com/stretchr/powerwalk"
	"os"
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

	powerwalk.Walk(path, func(path string, info os.FileInfo, _ error) error {

		if info.IsDir() {
			return nil
		}

		vol, err := NewVolumeFromPath(path)
		if err != nil {
			return err
		}

		// TODO
		println(vol.Id())

		return nil

	})

	return counts

}
