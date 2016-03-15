package main

import (
	"compress/bzip2"
	"fmt"
	"github.com/Jeffail/gabs"
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

		for _, page := range vol.Pages() {
			fmt.Println(page.TokenCount())
		}

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

func (v *Volume) Pages() []Page {

	children, _ := v.json.Search("features", "pages").Children()

	var pages []Page
	for _, child := range children {
		pages = append(pages, Page{json: child})
	}

	return pages

}

type Page struct {
	json *gabs.Container
}

func (p *Page) TokenCount() int {
	return int(p.json.Path("tokenCount").Data().(float64))
}

func main() {
	filepath.Walk("data/basic", walkVolume)
}
