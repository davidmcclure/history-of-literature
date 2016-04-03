package htrc

import (
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

		})

		close(volumes)

	}()

	return volumes

}

type YearCount struct {
	year  string
	count int
}

// Get token counts for each year.
func (c *Corpus) YearCounts() map[string]int {

	counts := make(map[string]int)

	paths := make(chan string)
	results := make(chan YearCount)

	for w := 1; w <= 100; w++ {
		go worker(paths, results)
	}

	go func() {

		powerwalk.Walk(c.Path, func(path string, info os.FileInfo, _ error) error {

			if !info.IsDir() {
				paths <- path
			}

			return nil

		})

		close(paths)

	}()

	var ops int = 0
	for yc := range results {

		counts[yc.year] += yc.count

		ops++
		if ops%100 == 0 {
			println(ops)
		}

	}

	return counts

}

func worker(paths <-chan string, results chan<- YearCount) {
	for path := range paths {

		vol, _ := NewVolumeFromPath(path)
		//if err != nil {
		//return err
		//}

		if vol.IsEnglish() {
			results <- YearCount{
				year:  vol.YearString(),
				count: vol.TotalTokenCount(),
			}
		}

	}
}

// Get per-year counts for each token.
func (c *Corpus) TokenCounts() map[string]map[string]int {

	counts := make(map[string]map[string]int)

	volumes := c.WalkEnglishVolumes()

	var ops int = 0

	for vol := range volumes {

		year := vol.YearString()

		// Initialize the year map, if missing.
		_, hasYear := counts[year]
		if !hasYear {
			counts[year] = make(map[string]int)
		}

		// Update the year/token count.
		for token, count := range vol.CleanedTokenCounts() {
			counts[year][token] += count
		}

		// Log Progress.
		ops++
		if ops%1000 == 0 {
			println(ops)
		}

	}

	return counts

}
