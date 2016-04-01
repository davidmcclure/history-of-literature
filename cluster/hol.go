package main

import (
	"encoding/json"
	"fmt"
	"github.com/davidmcclure/hol/htrc"
	"github.com/jawher/mow.cli"
	"io/ioutil"
	"os"
	"runtime"
)

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	app := cli.App("hol", "The history of 'literature.'")

	app.Command(
		"yearCounts",
		"Get token counts for each year",
		yearCountsCmd,
	)

	app.Command(
		"tokenCounts",
		"Get per-year counts for each token",
		htrc.TokenCountsCmd,
	)

	app.Run(os.Args)

}

// Pull year counts, dump JSON.
func yearCountsCmd(cmd *cli.Cmd) {

	var htrcPath = cmd.StringArg(
		"HTRC_PATH",
		"path/to/htrc",
		"The HTRC basic features root",
	)

	var outPath = cmd.StringArg(
		"OUT_PATH",
		"path/to/json",
		"The final JSON output path",
	)

	cmd.Action = func() {

		// Build the counts.
		corpus := htrc.Corpus{*htrcPath}
		counts := corpus.YearCounts()

		data, err := json.Marshal(counts)
		if err != nil {
			fmt.Println(err)
		}

		// Dump JSON to file.
		ioutil.WriteFile(*outPath, data, 0644)

	}

}
