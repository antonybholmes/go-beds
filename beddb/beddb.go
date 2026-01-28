package beddb

import (
	"sync"

	"github.com/antonybholmes/go-beds"
	"github.com/antonybholmes/go-seqs"
)

var instance *beds.BedsDB
var once sync.Once

func InitBedDB(dir string) *beds.BedsDB {
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

// func Genomes() ([]string, error) {
// 	return instance.Genomes()
// }

func Platforms(assembly string, isAdmin bool, permissions []string) ([]*seqs.Platform, error) {
	return instance.Platforms(assembly, isAdmin, permissions)
}

func Search(query string, assembly string, isAdmin bool, permissions []string) ([]*beds.BedSample, error) {
	return instance.Search(query, assembly, isAdmin, permissions)
}

func ReaderFromId(uuid string) (*beds.BedReader, error) {
	return instance.ReaderFromId(uuid)
}

func CanViewSample(sampleId string, isAdmin bool, permissions []string) error {
	return instance.CanViewSample(sampleId, isAdmin, permissions)
}
