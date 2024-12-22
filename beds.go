package beds

import (
	"database/sql"
	"path/filepath"

	"github.com/antonybholmes/go-dna"
	"github.com/antonybholmes/go-sys"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog/log"
)

// const MAGIC_NUMBER_OFFSET_BYTES = 0
// const BIN_SIZE_OFFSET_BYTES = MAGIC_NUMBER_OFFSET_BYTES + 4
// const BIN_WIDTH_OFFSET_BYTES = BIN_SIZE_OFFSET_BYTES + 4
// const N_BINS_OFFSET_BYTES = BIN_WIDTH_OFFSET_BYTES + 4
// const BINS_OFFSET_BYTES = N_BINS_OFFSET_BYTES + 4

const PLATFORMS_SQL = `SELECT DISTINCT platform FROM beds ORDER BY platform`
const GENOMES_SQL = `SELECT DISTINCT genome FROM beds WHERE platform = ?1 ORDER BY genome`
const BEDS_SQL = `SELECT id, public_id, platform, genome, name, file FROM beds WHERE platform = ?1 AND genome = ?2 ORDER BY name`

const FIND_BED_SQL = `SELECT id, public_id, platform, genome, name, file FROM beds WHERE public_id = ?1`

const BED_SQL = `SELECT chr, start, end, score, name, tags 
	FROM bed
 	WHERE chr = ?1 AND ((start >= ?2 AND start <= ?3) OR (end >= ?2 AND end <= ?3) OR (start <= ?2 AND end >= ?3))
	ORDER BY chr, start`

type BedFeature struct {
	Location *dna.Location `json:"location"`
	Name     string        `json:"name,omitempty"`
	Score    float64       `json:"score"`
	Tags     string        `json:"tags,omitempty"`
}

type Track struct {
	Platform string `json:"platform"`
	Genome   string `json:"genome"`
	Name     string `json:"name"`
}

type BedInfo struct {
	PublicId string `json:"publicId"`
	Platform string `json:"platform"`
	Genome   string `json:"genome"`
	Name     string `json:"name"`
	File     string `json:"-"`
}

type BedGenome struct {
	Name string    `json:"name"`
	Beds []BedInfo `json:"beds"`
}

type BedPlaform struct {
	Name    string      `json:"name"`
	Genomes []BedGenome `json:"genomes"`
}

type AllBeds struct {
	Name      string       `json:"name"`
	Platforms []BedPlaform `json:"platforms"`
}

type BedReader struct {
	file string
}

func NewBedReader(file string) (*BedReader, error) {

	return &BedReader{file: file}, nil
}

func (reader *BedReader) BedFeatures(location *dna.Location) ([]BedFeature, error) {

	db, err := sql.Open("sqlite3", reader.file)

	if err != nil {
		log.Debug().Msgf("bin sql err %s", err)
		return nil, err
	}

	defer db.Close()

	rows, err := db.Query(BED_SQL,
		location.Chr,
		location.Start,
		location.End)

	if err != nil {
		return nil, err
	}

	var chr string
	var start uint
	var end uint
	var score float64
	var name string
	var tags string

	ret := make([]BedFeature, 0, 10)

	for rows.Next() {
		err := rows.Scan(&chr, &start, &end, &score, &name, &tags)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		ret = append(ret, BedFeature{Location: dna.NewLocation(chr, start, end), Score: score, Name: name, Tags: tags})
	}

	return ret, nil

}

// func (reader *TracksReader) ReadsUint8(location *dna.Location) (*BinCounts, error) {
// 	s := location.Start - 1
// 	e := location.End - 1

// 	bs := s / reader.BinWidth
// 	be := e / reader.BinWidth
// 	bl := be - bs + 1

// 	file := reader.getPath(location)

// 	f, err := os.Open(file)

// 	if err != nil {
// 		return nil, err
// 	}

// 	defer f.Close()

// 	//var magic uint32
// 	//binary.Read(f, binary.LittleEndian, &magic)

// 	f.Seek(9, 0)

// 	offset := BINS_OFFSET_BYTES + bs
// 	log.Debug().Msgf("offset %d %d", offset, bs)

// 	data := make([]uint8, bl)
// 	f.Seek(int64(offset), 0)
// 	binary.Read(f, binary.LittleEndian, &data)

// 	reads := make([]uint32, bl)

// 	for i, c := range data {
// 		reads[i] = uint32(c)
// 	}

// 	return reader.Results(location, bs, reads)
// }

// func (reader *TracksReader) ReadsUint16(location *dna.Location) (*BinCounts, error) {
// 	s := location.Start - 1
// 	e := location.End - 1

// 	bs := s / reader.BinWidth
// 	be := e / reader.BinWidth
// 	bl := be - bs + 1

// 	file := reader.getPath(location)

// 	f, err := os.Open(file)

// 	if err != nil {
// 		return nil, err
// 	}

// 	defer f.Close()

// 	f.Seek(9, 0)

// 	data := make([]uint16, bl)
// 	f.Seek(int64(BINS_OFFSET_BYTES+bs*2), 0)
// 	binary.Read(f, binary.LittleEndian, &data)

// 	reads := make([]uint32, bl)

// 	for i, c := range data {
// 		reads[i] = uint32(c)
// 	}

// 	return reader.Results(location, bs, reads)
// }

// func (reader *TracksReader) ReadsUint32(location *dna.Location) (*BinCounts, error) {
// 	s := location.Start - 1
// 	e := location.End - 1

// 	bs := s / reader.BinWidth
// 	be := e / reader.BinWidth
// 	bl := be - bs + 1

// 	file := reader.getPath(location)

// 	f, err := os.Open(file)

// 	if err != nil {
// 		return nil, err
// 	}

// 	defer f.Close()

// 	f.Seek(9, 0)

// 	reads := make([]uint32, bl)
// 	f.Seek(int64(BINS_OFFSET_BYTES+bs*4), 0)
// 	binary.Read(f, binary.LittleEndian, &reads)

// 	return reader.Results(location, bs, reads)
// }

// func (reader *TracksReader) Results(location *dna.Location, bs uint, reads []uint32) (*BinCounts, error) {

// 	return &BinCounts{
// 		Location: location,
// 		Start:    bs*reader.BinWidth + 1,
// 		Reads:    reads,
// 		ReadN:    reader.ReadN,
// 	}, nil
// }

type BedsDB struct {
	db  *sql.DB
	dir string
}

func (tracksDb *BedsDB) Dir() string {
	return tracksDb.dir
}

func NewBedsDB(dir string) *BedsDB {

	db := sys.Must(sql.Open("sqlite3", filepath.Join(dir, "beds.db")))

	return &BedsDB{dir: dir, db: db}
}

func (bedsDb *BedsDB) Platforms() ([]string, error) {
	rows, err := bedsDb.db.Query(PLATFORMS_SQL)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

	ret := make([]string, 0, 10)

	defer rows.Close()

	var platform string

	for rows.Next() {
		err := rows.Scan(&platform)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		ret = append(ret, platform)
	}

	return ret, nil
}

func (bedsDb *BedsDB) Genomes(platform string) ([]string, error) {
	rows, err := bedsDb.db.Query(GENOMES_SQL, platform)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

	ret := make([]string, 0, 10)

	defer rows.Close()

	var genome string

	for rows.Next() {
		err := rows.Scan(&genome)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		ret = append(ret, genome)
	}

	return ret, nil
}

func (bedsDb *BedsDB) Beds(platform string, genome string) ([]BedInfo, error) {
	rows, err := bedsDb.db.Query(BEDS_SQL, platform, genome)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

	ret := make([]BedInfo, 0, 10)

	defer rows.Close()

	var id uint
	var publicId string
	var name string
	var file string

	for rows.Next() {
		err := rows.Scan(&id, &publicId, &platform, &genome, &name, &file)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		ret = append(ret, BedInfo{PublicId: publicId, Platform: platform, Genome: genome, Name: name, File: file})
	}

	return ret, nil
}

func (bedsDb *BedsDB) AllTracks() (*AllBeds, error) {
	platforms, err := bedsDb.Platforms()

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

	ret := AllBeds{Name: "Beds Database", Platforms: make([]BedPlaform, 0, len(platforms))}

	for _, platform := range platforms {

		genomes, err := bedsDb.Genomes(platform)

		if err != nil {
			return nil, err
		}

		bedPlaform := BedPlaform{Name: platform, Genomes: make([]BedGenome, 0, len(genomes))}

		for _, genome := range genomes {
			beds, err := bedsDb.Beds(platform, genome)

			if err != nil {
				return nil, err
			}

			bedGenome := BedGenome{Name: genome, Beds: beds}

			bedPlaform.Genomes = append(bedPlaform.Genomes, bedGenome)

		}

		ret.Platforms = append(ret.Platforms, bedPlaform)

	}

	return &ret, nil
}

func (tracksDb *BedsDB) ReaderFromId(publicId string) (*BedReader, error) {

	var platform string
	var genome string
	var name string
	var id uint

	var file string
	//const FIND_TRACK_SQL = `SELECT platform, genome, name, reads, stat_mode, dir FROM tracks WHERE tracks.publicId = ?1`

	err := tracksDb.db.QueryRow(FIND_BED_SQL, publicId).Scan(&id, &publicId, &platform, &genome, &name, &file)

	if err != nil {
		return nil, err
	}

	return NewBedReader(file)
}
