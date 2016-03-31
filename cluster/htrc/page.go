package htrc

import (
	"github.com/bitly/go-simplejson"
)

// An individual page.
type Page struct {
	json *simplejson.Json
}

// Get the token count for the page body.
func (p *Page) TokenCount() int {
	return p.json.GetPath("body", "tokenCount").MustInt()
}
