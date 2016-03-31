package main

import (
	"encoding/json"
	"github.com/davidmcclure/hol/htrc"
	"github.com/jawher/mow.cli"
	"github.com/stretchr/powerwalk"
	"io/ioutil"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
)

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	app := cli.App("hol", "The history of 'literature.'")

	app.Command(
		"yearCounts",
		"Extract per-year total counts",
		func(cmd *cli.Cmd) {

			var corpus = cmd.StringArg(
				"CORPUS",
				"path/to/htrc",
				"HTRC root",
			)

			cmd.Action = func() {
				extractYearCounts(*corpus)
			}

		},
	)

	app.Run(os.Args)

}

// Accumulate per-year counts.
func extractYearCounts(path string) {

	var mutex = &sync.Mutex{}

	counts := make(map[string]int)

	var ops int64 = 0

	powerwalk.Walk(path, func(path string, info os.FileInfo, _ error) error {

		if !info.IsDir() {

			vol, err := htrc.NewVolumeFromPath(path)
			if err != nil {
				return err
			}

			// Increment the year count.
			mutex.Lock()
			counts[vol.YearString()] += vol.TokenCount()
			mutex.Unlock()

			// Log progress.
			atomic.AddInt64(&ops, 1)
			if ops%100 == 0 {
				println(ops)
			}

		}

		return nil

	})

	data, _ := json.Marshal(counts)
	ioutil.WriteFile("test.json", data, 0644)

}
