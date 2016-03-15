package main

import (
	"compress/bzip2"
	"fmt"
	"github.com/Jeffail/gabs"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
)

func walkVolume(path string, info os.FileInfo, err error) error {

	if !info.IsDir() {

		vol, err := openVolume(path)
		if err != nil {
			return err
		}

		fmt.Println(vol.Year(), vol.Id())

	}

	return nil

}

func openVolume(path string) (v *Volume, err error) {

	// Open the file.
	raw, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	// Decompress the JSON.
	file := bzip2.NewReader(raw)

	// Read the bytes.
	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}

	// Parse the JSON.
	parsed, err := gabs.ParseJSON(bytes)
	if err != nil {
		return nil, err
	}

	return &Volume{json: parsed}, nil

}

type Volume struct {
	json *gabs.Container
}

func (v *Volume) Id() string {
	return v.json.Path("id").Data().(string)
}

func (v *Volume) Year() int {

	raw := v.json.Path("metadata.pubDate").Data().(string)
	year, _ := strconv.Atoi(raw)

	return year

}

func main() {
	filepath.Walk("data", walkVolume)
}
