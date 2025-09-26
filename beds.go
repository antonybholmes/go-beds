package beds

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/antonybholmes/go-dna"
	"github.com/antonybholmes/go-sys"
	_ "github.com/mattn/go-sqlite3"
)

// const MAGIC_NUMBER_OFFSET_BYTES = 0
// const BIN_SIZE_OFFSET_BYTES = MAGIC_NUMBER_OFFSET_BYTES + 4
// const BIN_WIDTH_OFFSET_BYTES = BIN_SIZE_OFFSET_BYTES + 4
// const N_BINS_OFFSET_BYTES = BIN_WIDTH_OFFSET_BYTES + 4
// const BINS_OFFSET_BYTES = N_BINS_OFFSET_BYTES + 4

const GENOMES_SQL = `SELECT DISTINCT genome FROM tracks ORDER BY genome`
const PLATFORMS_SQL = `SELECT DISTINCT platform FROM tracks WHERE genome = ?1 ORDER BY platform`

const SELECT_BED_SQL = `SELECT id, public_id, genome, platform, dataset, name, track_type, regions, url, tags `

const BEDS_SQL = SELECT_BED_SQL +
	`FROM tracks 
	WHERE genome = ?1 AND platform = ?2 
	ORDER BY name`

const ALL_BEDS_SQL = SELECT_BED_SQL +
	`FROM tracks WHERE genome = ?1 
	ORDER BY genome, platform, name`

const SEARCH_BED_SQL = SELECT_BED_SQL +
	`FROM tracks 
	WHERE genome = ?1 AND (public_id = ?1 OR platform = ?1 OR name LIKE ?2)
	ORDER BY genome, platform, name`

const BED_FROM_ID_SQL = SELECT_BED_SQL +
	`FROM tracks WHERE public_id = ?1
	ORDER BY genome, platform, name`

const OVERLAPPING_REGIONS_SQL = `SELECT chr, start, end, score, name, tags 
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
	PublicId  string   `json:"publicId"`
	Platform  string   `json:"platform"`
	Genome    string   `json:"genome"`
	Dataset   string   `json:"dataset"`
	Name      string   `json:"name"`
	TrackType string   `json:"trackType"`
	Url       string   `json:"url"`
	Tags      []string `json:"tags"`
	Regions   uint     `json:"regions"`
}

type BedReader struct {
	file string
}

func NewBedReader(file string) (*BedReader, error) {
	return &BedReader{file: file}, nil
}

func (reader *BedReader) OverlappingRegions(location *dna.Location) ([]*BedRegion, error) {
	ret := make([]*BedRegion, 0, 10)

	//log.Debug().Msgf("hmm %s", reader.file)

	db, err := sql.Open("sqlite3", reader.file)

	if err != nil {
		return ret, err
	}

	defer db.Close()

	rows, err := db.Query(OVERLAPPING_REGIONS_SQL,
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

		ret = append(ret, &BedRegion{Location: dna.NewLocation(chr, start, end), Score: score, Name: name, Tags: tags})
	}

	return ret, nil

}

type BedsDB struct {
	db *sql.DB
	//stmtAllBeds    *sql.Stmt
	//stmtSearchBeds *sql.Stmt
	//stmtBedFromId  *sql.Stmt
	dir string
}

func (tracksDb *BedsDB) Dir() string {
	return tracksDb.dir
}

func NewBedsDB(dir string) *BedsDB {
	db := sys.Must(sql.Open("sqlite3", filepath.Join(dir, "tracks.db?mode=ro")))

	//stmtAllBeds := sys.Must(db.Prepare(ALL_BEDS_SQL))
	//stmtSearchBeds := sys.Must(db.Prepare(SEARCH_BED_SQL))
	//stmtBedFromId := sys.Must(db.Prepare(BED_FROM_ID_SQL))

	return &BedsDB{dir: dir,
		db: db,
		//stmtAllBeds:    stmtAllBeds,
		//stmtSearchBeds: stmtSearchBeds,
		//stmtBedFromId:  stmtBedFromId
	}
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
	var dataset string
	var name string
	var trackType string
	var url string
	var tags string

	for rows.Next() {
		err := rows.Scan(&id, &publicId, &genome, &platform, &dataset, &name, &trackType, &url, &tags)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		tagList := strings.Split(tags, ",")
		sort.Strings(tagList)

		track := BedTrack{PublicId: publicId,
			Genome:    genome,
			Platform:  platform,
			Dataset:   dataset,
			Name:      name,
			TrackType: trackType,
			Url:       url,
			Tags:      tagList}

		if track.TrackType == "Remote BigBed" {
			track.Url = url
		}

		ret = append(ret, track)
	}

	return ret, nil
}

func (bedsDb *BedsDB) Search(genome string, query string) ([]BedTrack, error) {
	var rows *sql.Rows
	var err error

	if query != "" {
		rows, err = bedsDb.db.Query(SEARCH_BED_SQL, genome, query, fmt.Sprintf("%%%s%%", query))
	} else {
		rows, err = bedsDb.db.Query(ALL_BEDS_SQL, genome)
	}

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

	defer rows.Close()

	ret := make([]BedTrack, 0, 10)

	var id uint
	var publicId string
	var platform string
	var dataset string
	var name string
	var trackType string
	var regions uint
	var url string
	var tags string

	for rows.Next() {
		err := rows.Scan(&id, &publicId, &genome, &platform, &dataset, &name, &trackType, &regions, &url, &tags)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		tagList := strings.Split(tags, ",")
		sort.Strings(tagList)

		track := BedTrack{PublicId: publicId,
			Genome:    genome,
			Platform:  platform,
			Dataset:   dataset,
			Name:      name,
			Regions:   regions,
			TrackType: trackType,
			Tags:      tagList}

		if track.TrackType == "Remote BigBed" {
			track.Url = url
		}

		ret = append(ret, track)
	}

	return ret, nil
}

func (bedsDb *BedsDB) ReaderFromId(publicId string) (*BedReader, error) {

	var platform string
	var genome string
	var dataset string
	var name string
	var trackType string
	var regions uint
	var id uint
	var url string
	var tags string

	err := bedsDb.db.QueryRow(BED_FROM_ID_SQL, publicId).Scan(&id,
		&publicId,
		&genome,
		&platform,
		&dataset,
		&name,
		&trackType,
		&regions,
		&url,
		&tags)

	if err != nil {
		return nil, err
	}

	url = filepath.Join(bedsDb.dir, fmt.Sprintf("%s?mode=ro", url))

	return NewBedReader(url)
}
