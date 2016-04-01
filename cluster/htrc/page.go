package htrc

import (
	"github.com/bitly/go-simplejson"
	"regexp"
	"strings"
)

// An individual page.
type Page struct {
	json *simplejson.Json
}

// Get the token count for the page body.
func (p *Page) TotalTokenCount() int {
	return p.json.GetPath("body", "tokenCount").MustInt()
}

// Get a map of (token -> count) for each [a-z] token.
func (p *Page) CleanedTokenCounts() map[string]int {

	counts := make(map[string]int)

	tps := p.json.GetPath("body", "tokenPosCount")

	tokenRegex, _ := regexp.Compile("^[a-z]+$")

	for token, _ := range tps.MustMap() {

		token := strings.ToLower(token)

		// Ignore irregular tokens.
		if !tokenRegex.MatchString(token) {
			continue
		}

		// Merge part-of-speech counts.
		count := 0
		for pos, _ := range tps.Get(token).MustMap() {
			count += tps.GetPath(token, pos).MustInt()
		}

		counts[token] = count

	}

	return counts

}
