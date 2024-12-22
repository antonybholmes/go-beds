package beds

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"

	basemath "github.com/antonybholmes/go-basemath"
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
const BEDS_SQL = `SELECT DISTINCT genome FROM beds WHERE platform = ?1 ORDER BY genome`

const FIND_TRACK_SQL = `SELECT platform, genome, name, reads, stat_mode, dir FROM tracks WHERE tracks.public_id = ?1`

const BIN_SQL = `SELECT start, end, reads 
	FROM bins
 	WHERE start >= ?1 AND end < ?2
	ORDER BY start`

type BinCounts struct {
	Track    Track         `json:"track"`
	Location *dna.Location `json:"location"`
	Bins     []uint        `json:"bins"`
	Start    uint          `json:"start"`
	BinWidth uint          `json:"binWidth"`
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

type TrackReader struct {
	Dir      string
	Stat     string
	Track    Track
	BinWidth uint
	Reads    uint
}

func NewTrackReader(dir string, track Track, binWidth uint) (*TrackReader, error) {

	dir = filepath.Join(dir, track.Platform, track.Genome, track.Name)

	path := filepath.Join(dir, "track.db")

	db, err := sql.Open("sqlite3", path)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	var reads uint
	var name string
	var publicId string
	var stat string
	err = db.QueryRow(TRACK_SQL).Scan(&publicId, &name, &reads, &stat)

	if err != nil {
		return nil, err
	}

	// if err != nil {
	// 	return nil, fmt.Errorf("error opening %s", file)
	// }

	// defer file.Close()
	// // Create a scanner
	// scanner := bufio.NewScanner(file)
	// scanner.Scan()

	// count, err := strconv.Atoi(scanner.Text())

	// if err != nil {
	// 	return nil, fmt.Errorf("could not count reads")
	// }

	return &TrackReader{Dir: dir,
		Stat:     stat,
		BinWidth: binWidth,
		Reads:    reads,
		Track:    track}, nil
}

func (reader *TrackReader) getPath(location *dna.Location) string {
	return filepath.Join(reader.Dir, fmt.Sprintf("%s_bw%d_%s.db", strings.ToLower(location.Chr), reader.BinWidth, reader.Track.Genome))

}

func (reader *TrackReader) BinCounts(location *dna.Location) (*BinCounts, error) {

	path := reader.getPath(location)

	log.Debug().Msgf("track path %s", path)

	db, err := sql.Open("sqlite3", path)

	if err != nil {
		log.Debug().Msgf("bin sql err %s", err)
		return nil, err
	}

	defer db.Close()

	startBin := (location.Start - 1) / reader.BinWidth
	endBin := (location.End - 1) / reader.BinWidth

	rows, err := db.Query(BIN_SQL,
		startBin,
		endBin)

	if err != nil {
		return nil, err
	}

	var readBlockStart uint
	var readBlockEnd uint
	var count uint
	reads := make([]uint, endBin-startBin+1)
	index := 0

	for rows.Next() {
		err := rows.Scan(&readBlockStart, &readBlockEnd, &count)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		// we don't want to load bin data that goes outside our coordinates
		// of interest. A long gapped bin, may end beyond the blocks we are
		// interested in, so we need to stop the loop short if so.
		endBin := basemath.UintMin(startBin+uint(len(reads)), readBlockEnd)

		for bin := readBlockStart; bin < endBin; bin++ {
			reads[bin-startBin] = count
		}

		index++
	}

	return &BinCounts{
		Track:    reader.Track,
		Location: location,
		Start:    startBin*reader.BinWidth + 1,
		Bins:     reads,

		BinWidth: reader.BinWidth,
	}, nil

	// var magic uint32
	// binary.Read(f, binary.LittleEndian, &magic)
	// var binSizeBytes byte
	// binary.Read(f, binary.LittleEndian, &binSizeBytes)

	// switch binSizeBytes {
	// case 1:
	// 	return reader.ReadsUint8(location)
	// case 2:
	// 	return reader.ReadsUint16(location)
	// default:
	// 	return reader.ReadsUint32(location)
	// }
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
	rows, err := bedsDb.db.Query(BEDS_SQL, platform)

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

	geneRows, err := bedsDb.db.Query(OVERLAPPING_GENES_FROM_LOCATION_SQL,
		location.Chr,
		location.Start,
		location.End)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

	return &ret, nil
}

func (tracksDb *BedsDB) ReaderFromTrackId(publicId string, binWidth uint) (*TrackReader, error) {

	var platform string
	var genome string
	var name string
	var reads uint
	var stat string
	var dir string
	//const FIND_TRACK_SQL = `SELECT platform, genome, name, reads, stat_mode, dir FROM tracks WHERE tracks.publicId = ?1`

	err := tracksDb.db.QueryRow(FIND_TRACK_SQL, publicId).Scan(&platform, &genome, &name, &reads, &stat, &dir)

	if err != nil {
		return nil, err
	}

	track := Track{Platform: platform, Genome: genome, Name: name}

	return NewTrackReader(tracksDb.dir, track, binWidth)
}
