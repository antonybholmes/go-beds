package beddbcache

import (
	"sync"

	"github.com/antonybholmes/go-bed"
	"github.com/antonybholmes/go-beds"
)

var instance *beds.BedsDB
var once sync.Once

func InitCache(dir string) *beds.BedsDB {
	once.Do(func() {
		instance = beds.NewBedsDB(dir)
	})

	return instance
}

func GetInstance() *beds.BedsDB {
	return instance
}

func Dir() string {
	return instance.Dir()
}

func Platforms() []string {
	return instance.Platforms()
}

func Genomes(platform string) ([]string, error) {
	return instance.Genomes(platform)
}

func Tracks(platform string, genome string) ([]bed.TrackInfo, error) {
	return instance.Tracks(platform, genome)
}

func AllTracks() (*bed.AllTracks, error) {
	return instance.AllTracks()
}

func ReaderFromTrackId(publicId string, binWidth uint) (*bed.TrackReader, error) {
	return instance.ReaderFromTrackId(publicId, binWidth)
}
