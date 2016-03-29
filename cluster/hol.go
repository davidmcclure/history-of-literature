package main

import (
	"github.com/codegangsta/cli"
	"os"
)

func main() {

	app := cli.NewApp()
	app.Name = "hol"

	app.Commands = []cli.Command{
		{
			Name: "counts",
			Action: func(c *cli.Context) {
				println(c.Args().First())
			},
		},
	}

	app.Run(os.Args)

}
