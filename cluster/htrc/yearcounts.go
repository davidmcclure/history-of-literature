package htrc

import (
	"encoding/json"
	"fmt"
	"github.com/jawher/mow.cli"
	"github.com/stretchr/powerwalk"
	"io/ioutil"
	"os"
	"sync"
	"sync/atomic"
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
		counts := extractYearCounts(*htrcPath)
		writeYearCounts(&counts, *outPath)
	}

}

// Accumulate per-year counts.
func extractYearCounts(path string) map[string]int {

	var mutex = &sync.Mutex{}

	counts := make(map[string]int)

	var ops int64 = 0

	powerwalk.Walk(path, func(path string, info os.FileInfo, _ error) error {

		if info.IsDir() {
			return nil
		}

		vol, err := NewVolumeFromPath(path)
		if err != nil {
			return err
		}

		// Increment the year count.
		mutex.Lock()
		counts[vol.YearString()] += vol.TotalTokenCount()
		mutex.Unlock()

		// Log progress.
		atomic.AddInt64(&ops, 1)
		if ops%100 == 0 {
			println(ops)
		}

		return nil

	})

	return counts

}

// Dump year counts to JSON.
func writeYearCounts(counts *map[string]int, path string) {

	data, err := json.Marshal(counts)
	if err != nil {
		fmt.Println(err)
	}

	ioutil.WriteFile(path, data, 0644)

}
