package main

import (
	"compress/bzip2"
	"encoding/json"
	"fmt"
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

		// Open the .bz2 file.
		raw, err := os.Open(path)
		if err != nil {
			return err
		}

		// Decompress the .bz2.
		decompressed := bzip2.NewReader(raw)

		// Decode into a Volume.
		vol := new(Volume)
		json.NewDecoder(decompressed).Decode(vol)

		fmt.Println(vol.Metadata.Year)

	}

	return nil

}

func main() {
	filepath.Walk("data", walkVolume)
}
