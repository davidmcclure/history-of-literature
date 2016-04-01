package htrc

import (
	"fmt"
	"github.com/stretchr/powerwalk"
	"os"
)

type Corpus struct {
	Path string
}

// Generage English volumes concurrently.
func (c *Corpus) WalkEnglishVolumes() <-chan *Volume {

	volumes := make(chan *Volume)

	// Spawn a goroutine for the tree walk.
	go func() {

		powerwalk.Walk(c.Path, func(path string, info os.FileInfo, _ error) error {

			if info.IsDir() {
				return nil
			}

			vol, err := NewVolumeFromPath(path)
			if err != nil {
				return err
			}

			if vol.IsEnglish() {
				volumes <- vol
			}

			return nil

		},
		)

		close(volumes)

	}()

	return volumes

}

// Get token counts for each year.
func (c *Corpus) YearCounts() map[string]int {

	volumes := c.WalkEnglishVolumes()

	counts := make(map[string]int)

	for vol := range volumes {
		counts[vol.YearString()] += vol.TotalTokenCount()
	}

	return counts

}

// Get per-year counts for each token.
func (c *Corpus) TokenCounts() {

	volumes := c.WalkEnglishVolumes()

	for vol := range volumes {
		fmt.Println(vol.CleanedTokenCounts()["the"])
	}

}
