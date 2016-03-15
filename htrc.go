package main

import (
	"compress/bzip2"
	"github.com/Jeffail/gabs"
	"github.com/davecgh/go-spew/spew"
	"io/ioutil"
	"os"
	"path/filepath"
)

func walkVolume(path string, info os.FileInfo, err error) error {

	if !info.IsDir() {

		vol, err := openVolume(path)
		if err != nil {
			return err
		}

		spew.Dump(vol)

	}

	return nil

}

func openVolume(path string) (g *gabs.Container, err error) {

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

	return gabs.ParseJSON(bytes)

}

func main() {
	filepath.Walk("data", walkVolume)
}
