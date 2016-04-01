package htrc

import (
	"encoding/json"
	"fmt"
	"github.com/jawher/mow.cli"
	"io/ioutil"
)

// Get counts, dump JSON.
func YearCountsCmd(cmd *cli.Cmd) {

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
		corpus := Corpus{path: *htrcPath}
		counts := corpus.YearCounts()
		writeYearCounts(&counts, *outPath)
	}

}

// Dump year counts to JSON.
func writeYearCounts(counts *map[string]int, path string) {

	data, err := json.Marshal(counts)
	if err != nil {
		fmt.Println(err)
	}

	ioutil.WriteFile(path, data, 0644)

}
