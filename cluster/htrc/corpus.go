package htrc

import (
	"github.com/stretchr/powerwalk"
	"os"
)

type Corpus struct {
	path string
}

// Generage English volumes concurrently.
func (c *Corpus) WalkEnglishVolumes() <-chan *Volume {

	volumes := make(chan *Volume)

	// Spawn a goroutine for the tree walk.
	go func() {

		powerwalk.Walk(c.path, func(path string, info os.FileInfo, _ error) error {

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
