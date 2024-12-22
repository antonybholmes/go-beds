package beddbcache

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

func Platforms() ([]string, error) {
	return instance.Platforms()
}

func Genomes(platform string) ([]string, error) {
	return instance.Genomes(platform)
}

func AllBeds() (*beds.AllBeds, error) {
	return instance.AllTracks()
}

func ReaderFromId(publicId string) (*beds.BedReader, error) {
	return instance.ReaderFromId(publicId)
}
