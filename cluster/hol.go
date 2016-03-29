package main

import (
	"compress/bzip2"
	"github.com/bitly/go-simplejson"
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

func walkVolume(path string, info os.FileInfo, _ error) error {

	if !info.IsDir() {

		vol, err := openVolume(path)
		if err != nil {
			return err
		}

		println(vol.Id())

	}

	return nil

}

// Given a path for a .bz2 JSON file in the HTRC corpus, decode the file and
// parse the JSON into a Volume.
func openVolume(path string) (v *Volume, err error) {

	compressed, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	inflated := bzip2.NewReader(compressed)

	parsed, err := simplejson.NewFromReader(inflated)
	if err != nil {
		return nil, err
	}

	return &Volume{json: parsed}, nil

}

// An individual HTRC volume.
type Volume struct {
	json *simplejson.Json
}

// Get the HTRC id.
func (v *Volume) Id() string {
	return v.json.Get("id").MustString()
}
