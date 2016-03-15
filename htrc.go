package main

import (
	"compress/bzip2"
	"fmt"
	"github.com/Jeffail/gabs"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
)

func walkVolume(path string, info os.FileInfo, err error) error {

	if !info.IsDir() {

		vol, err := openVolume(path)
		if err != nil {
			return err
		}

		for _, page := range vol.Pages() {
			page.Edges()
		}

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

// Map (token1, token2) pairs to weights.
type EdgeList struct {
	weights map[[2]string]int
}

// Add N units of weight to the edge between two tokens.
func (e *EdgeList) AddWeight(token1 string, token2 string, weight int) {

	// Sort the tokens.
	tokens := []string{token1, token2}
	sort.Strings(tokens)

	// Flatten the slice into an array.
	var key [2]string
	copy(key[:], tokens[:2])

	e.weights[key] += weight

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

// Generate an edge list from the body tokens.
func (p *Page) Edges() *EdgeList {

	var edges EdgeList

	children, _ := p.json.Search("body", "tokenPosCount").ChildrenMap()

	for token, counts := range children {
		fmt.Println(token, counts)
	}

	return &edges

}

func main() {
	filepath.Walk("data/basic", walkVolume)
}
