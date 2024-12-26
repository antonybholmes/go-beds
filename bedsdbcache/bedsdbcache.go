package bedsdbcache

import (
	"sync"

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

func Genomes() ([]string, error) {
	return instance.Genomes()
}

func Platforms(genome string) ([]string, error) {
	return instance.Platforms(genome)
}

func Search(genome string, query string) ([]beds.BedTrack, error) {
	return instance.Search(genome, query)
}

func ReaderFromId(publicId string) (*beds.BedReader, error) {
	return instance.ReaderFromId(publicId)
}
