package main

import (
	"compress/bzip2"
	"encoding/json"
	"fmt"
	"io"
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

		file, err := loadBzip2JSON(path)
		if err != nil {
			return err
		}

		// Decode into a Volume.
		vol := new(Volume)
		json.NewDecoder(file).Decode(vol)

		fmt.Println(vol.Metadata.Year)

	}

	return nil

}

func loadBzip2JSON(path string) (f io.Reader, err error) {

	raw, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return bzip2.NewReader(raw), nil

}

func main() {
	filepath.Walk("data", walkVolume)
}
