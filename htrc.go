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

		fmt.Println(vol.TokenCount())

	}

	return nil

}

// Given a path for a .bz2 JSON file in the HTRC corpus, decode the file and
// parse the JSON into a Volume.
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

// An individual HTRC volume.
type Volume struct {
	json *gabs.Container
}

// Get the HTRC id.
func (v *Volume) Id() string {
	return v.json.Path("id").Data().(string)
}

// Gather a slice of Page instances.
func (v *Volume) Pages() []Page {

	children, _ := v.json.Search("features", "pages").Children()

	var pages []Page
	for _, child := range children {
		pages = append(pages, Page{json: child})
	}

	return pages

}

// Get the total token count for all pages.
func (v *Volume) TokenCount() int {

	count := 0
	for _, page := range v.Pages() {
		count += page.TokenCount()
	}

	return count

}

// An individual page in a volume.
type Page struct {
	json *gabs.Container
}

// Get the token count for the page body.
func (p *Page) TokenCount() int {
	return int(p.json.Path("body.tokenCount").Data().(float64))
}

func main() {
	filepath.Walk("data/basic", walkVolume)
}
