package main

import (
	"github.com/davidmcclure/hol/htrc"
	"github.com/jawher/mow.cli"
	"os"
	"runtime"
)

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	app := cli.App("hol", "The history of 'literature.'")

	app.Command(
		"yearCounts",
		"Extract per-year total counts",
		htrc.YearCountsCmd,
	)

	app.Run(os.Args)

}
