package htrc

import (
	"fmt"
	"github.com/bitly/go-simplejson"
)

// An individual page.
type Page struct {
	json *simplejson.Json
}

// Get the token count for the page body.
func (p *Page) TotalTokenCount() int {
	return p.json.GetPath("body", "tokenCount").MustInt()
}

// Get a map of (token -> count) for each a-z token.
func (p *Page) CleanedTokenCounts() map[string]int {

	counts := make(map[string]int)

	tpc := p.json.GetPath("body", "tokenPosCount")

	for token, posCount := range tpc.MustMap() {
		fmt.Println(token)
		fmt.Println(posCount)
	}

	// iterate through token -> posCount pairs
	// filter out non a-z's
	// downcase the token
	// sum up the pos counts
	// update the count

	return counts

}
