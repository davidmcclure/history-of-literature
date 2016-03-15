package main

import (
	"compress/bzip2"
	"encoding/json"
	"github.com/davecgh/go-spew/spew"
	"os"
	"path/filepath"
)

type Volume struct {
	Id       string   `json:"id"`
	Metadata Metadata `json:"metadata"`
}

type Metadata struct {
	Year  int    `json:"pubDate,string"`
	Title string `json:"title"`
}

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

func openVolume(path string) (v *Volume, err error) {

	// Open the file.
	raw, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	// Decompress the JSON.
	file := bzip2.NewReader(raw)

	// Decode into a Volume.
	vol := new(Volume)
	json.NewDecoder(file).Decode(vol)

	return vol, nil

}

func main() {
	filepath.Walk("data", walkVolume)
}

// combined openVolume function
