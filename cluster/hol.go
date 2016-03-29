package main

import (
	"compress/bzip2"
	"github.com/Jeffail/gabs"
	"github.com/jawher/mow.cli"
	"io/ioutil"
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

	content, err := ioutil.ReadAll(inflated)
	if err != nil {
		return nil, err
	}

	parsed, err := gabs.ParseJSON(content)
	if err != nil {
		return nil, err
	}

	return &Volume{json: parsed}, nil

}

// An individual HTRC volume.
type Volume struct {
	json *gabs.Container
}

// Get the HTRC id.
func (v *Volume) Id() string {
	return v.json.Path("id").Data().(string)
}
