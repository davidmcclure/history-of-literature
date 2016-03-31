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
		"Get total token counts for each year",
		htrc.YearCountsCmd,
	)

	app.Command(
		"tokenCounts",
		"Get individual token counts for each year",
		htrc.TokenCountsCmd,
	)

	app.Run(os.Args)

}
