package htrc

import (
	"compress/bzip2"
	"github.com/bitly/go-simplejson"
	"os"
	"strconv"
)

// An individual HTRC volume.
type Volume struct {
	json *simplejson.Json
}

// Given a path for a .bz2 JSON file in the HTRC corpus, decode the file and
// parse the JSON into a Volume.
func NewVolumeFromPath(path string) (*Volume, error) {

	deflated, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	defer deflated.Close()

	inflated := bzip2.NewReader(deflated)

	parsed, err := simplejson.NewFromReader(inflated)
	if err != nil {
		return nil, err
	}

	return &Volume{json: parsed}, nil

}

// Get the HTRC id.
func (v *Volume) Id() string {
	return v.json.Get("id").MustString()
}

// Get the year as a string.
func (v *Volume) YearString() string {
	return v.json.GetPath("metadata", "pubDate").MustString()
}

// Get the year as an int, 0 if parse fails.
func (v *Volume) YearInt() int {

	ystr := v.YearString()

	yint, err := strconv.Atoi(ystr)
	if err != nil {
		return 0
	}

	return yint

}

// Make page instances.
func (v *Volume) Pages() []*Page {

	key := v.json.GetPath("features", "pages")

	var pages []*Page

	for i, _ := range key.MustArray() {
		json := key.GetIndex(i)
		pages = append(pages, &Page{json: json})
	}

	return pages

}

// Get the total token count for all pages.
func (v *Volume) TotalTokenCount() int {

	count := 0
	for _, page := range v.Pages() {
		count += page.TotalTokenCount()
	}

	return count

}
