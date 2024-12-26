package beds

import (
	"database/sql"
	"fmt"
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

const GENOMES_SQL = `SELECT DISTINCT genome FROM tracks ORDER BY genome`
const PLATFORMS_SQL = `SELECT DISTINCT platform FROM tracks WHERE genome = ?1 ORDER BY platform`

const BEDS_SQL = `SELECT id, public_id, genome, platform, name, regions, file 
	FROM tracks 
	WHERE genome = ?1 AND platform = ?2 
	ORDER BY name`

const ALL_BEDS_SQL = `SELECT id, public_id, genome, platform, name, regions, file 
	FROM tracks WHERE genome = ?1 
	ORDER BY genome, platform, name`

const SEARCH_BED_SQL = `SELECT id, public_id, genome, platform, name, regions, file 
	FROM tracks 
	WHERE genome = ?1 AND (public_id = ?1 OR platform = ?1 OR name LIKE ?2)
	ORDER BY genome, platform, name`

const BED_FROM_ID_SQL = `SELECT id, public_id, genome, platform, name, regions, file 
	FROM tracks WHERE public_id = ?1
	ORDER BY genome, platform, name`

const REGIONS_SQL = `SELECT chr, start, end, score, name, tags 
	FROM regions
 	WHERE chr = ?1 AND (start <= ?3 AND end >= ?2)
	ORDER BY chr, start`

type BedRegion struct {
	Location *dna.Location `json:"loc"`
	Name     string        `json:"name,omitempty"`
	Tags     string        `json:"tags,omitempty"`
	Score    float64       `json:"score"`
}

type BedTrack struct {
	PublicId string `json:"publicId"`
	Platform string `json:"platform"`
	Genome   string `json:"genome"`
	Name     string `json:"name"`
	Regions  uint   `json:"regions"`
	File     string `json:"-"`
}

type BedReader struct {
	file string
}

func NewBedReader(file string) (*BedReader, error) {
	return &BedReader{file: file}, nil
}

func (reader *BedReader) BedRegions(location *dna.Location) ([]BedRegion, error) {
	ret := make([]BedRegion, 0, 10)

	db, err := sql.Open("sqlite3", reader.file)

	if err != nil {
		log.Debug().Msgf("bin sql err %s", err)
		return ret, err
	}

	defer db.Close()

	rows, err := db.Query(REGIONS_SQL,
		location.Chr,
		location.Start,
		location.End)

	if err != nil {
		return ret, err
	}

	var chr string
	var start uint
	var end uint
	var score float64
	var name string
	var tags string

	for rows.Next() {
		err := rows.Scan(&chr, &start, &end, &score, &name, &tags)

		if err != nil {
			return ret, err //fmt.Errorf("there was an error with the database records")
		}

		ret = append(ret, BedRegion{Location: dna.NewLocation(chr, start, end), Score: score, Name: name, Tags: tags})
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
	db             *sql.DB
	stmtAllBeds    *sql.Stmt
	stmtSearchBeds *sql.Stmt
	stmtBedFromId  *sql.Stmt
	dir            string
}

func (tracksDb *BedsDB) Dir() string {
	return tracksDb.dir
}

func NewBedsDB(dir string) *BedsDB {

	db := sys.Must(sql.Open("sqlite3", filepath.Join(dir, "tracks.db?mode=ro")))

	stmtAllBeds := sys.Must(db.Prepare(ALL_BEDS_SQL))
	stmtSearchBeds := sys.Must(db.Prepare(SEARCH_BED_SQL))
	stmtBedFromId := sys.Must(db.Prepare(BED_FROM_ID_SQL))

	return &BedsDB{dir: dir, db: db, stmtAllBeds: stmtAllBeds, stmtSearchBeds: stmtSearchBeds, stmtBedFromId: stmtBedFromId}
}

func (bedsDb *BedsDB) Genomes() ([]string, error) {
	rows, err := bedsDb.db.Query(GENOMES_SQL)

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

func (bedsDb *BedsDB) Platforms(genome string) ([]string, error) {
	rows, err := bedsDb.db.Query(PLATFORMS_SQL, genome)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

	defer rows.Close()

	ret := make([]string, 0, 10)

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

func (bedsDb *BedsDB) Beds(genome string, platform string) ([]BedTrack, error) {
	rows, err := bedsDb.db.Query(BEDS_SQL, genome, platform)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

	defer rows.Close()

	ret := make([]BedTrack, 0, 10)

	var id uint
	var publicId string
	var name string
	var file string

	for rows.Next() {
		err := rows.Scan(&id, &publicId, &genome, &platform, &name, &file)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		ret = append(ret, BedTrack{PublicId: publicId, Genome: genome, Platform: platform, Name: name, File: file})
	}

	return ret, nil
}

func (bedsDb *BedsDB) Search(genome string, query string) ([]BedTrack, error) {
	var rows *sql.Rows
	var err error

	if query != "" {
		rows, err = bedsDb.stmtSearchBeds.Query(genome, query, fmt.Sprintf("%%%s%%", query))
	} else {
		rows, err = bedsDb.stmtAllBeds.Query(genome)
	}

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

	defer rows.Close()

	ret := make([]BedTrack, 0, 10)

	var id uint
	var publicId string
	var platform string
	var name string
	var regions uint
	var file string

	for rows.Next() {
		err := rows.Scan(&id, &publicId, &genome, &platform, &name, &regions, &file)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		ret = append(ret, BedTrack{PublicId: publicId, Genome: genome, Platform: platform, Name: name, Regions: regions, File: file})
	}

	return ret, nil
}

func (bedsDb *BedsDB) ReaderFromId(publicId string) (*BedReader, error) {

	var platform string
	var genome string
	var name string
	var regions uint
	var id uint

	var file string

	err := bedsDb.stmtBedFromId.QueryRow(publicId).Scan(&id, &publicId, &platform, &genome, &name, &regions, &file)

	if err != nil {
		return nil, err
	}

	file = filepath.Join(bedsDb.dir, fmt.Sprintf("%s?mode=ro", file))

	return NewBedReader(file)
}
