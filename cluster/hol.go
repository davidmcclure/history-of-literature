package main

import (
	"github.com/jawher/mow.cli"
	"os"
	"path/filepath"
)

func main() {

	app := cli.App("hol", "History of 'literature'")

	app.Command("counts", "Extract token counts", func(cmd *cli.Cmd) {

		var corpus = cmd.StringArg(
			"CORPUS",
			"path/to/htrc",
			"HTRC root",
		)

		cmd.Action = func() {
			extractCounts(*corpus)
		}

	})

	app.Run(os.Args)

}

func extractCounts(path string) {
	filepath.Walk(path, walkVolume)
}

func walkVolume(path string, info os.FileInfo, err error) error {

	if !info.IsDir() {
		println(path)
	}

	return nil

}
