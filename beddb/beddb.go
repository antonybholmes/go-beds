package beddb

import (
	"sync"

	"github.com/antonybholmes/go-beds"
)

var instance *beds.BedsDB
var once sync.Once

func InitBedDB(path string) *beds.BedsDB {
	once.Do(func() {
		instance = beds.NewBedsDB(path)
	})

	return instance
}

func Instance() *beds.BedsDB {
	return instance
}
