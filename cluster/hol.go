package main

import (
	"github.com/jawher/mow.cli"
	"os"
)

func main() {

	app := cli.App("hol", "History of 'literature'")

	app.Command(
		"counts",
		"Extract per-year token counts",
		func(cmd *cli.Cmd) {

			var corpus = cmd.StringArg(
				"CORPUS",
				"path/to/htrc",
				"The HTRC corpus root",
			)

			cmd.Action = func() {
				println(*corpus)
			}

		},
	)

	app.Run(os.Args)

}
