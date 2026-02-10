package beds

import (
	"database/sql"
	"fmt"
	"path/filepath"

	"github.com/antonybholmes/go-dna"
	"github.com/antonybholmes/go-seqs"
	"github.com/antonybholmes/go-sys"
	"github.com/antonybholmes/go-sys/log"
	"github.com/antonybholmes/go-web/auth/sqlite"
	_ "github.com/mattn/go-sqlite3"
)

// const MAGIC_NUMBER_OFFSET_BYTES = 0
// const BIN_SIZE_OFFSET_BYTES = MAGIC_NUMBER_OFFSET_BYTES + 4
// const BIN_WIDTH_OFFSET_BYTES = BIN_SIZE_OFFSET_BYTES + 4
// const N_BINS_OFFSET_BYTES = BIN_WIDTH_OFFSET_BYTES + 4
// const BINS_OFFSET_BYTES = N_BINS_OFFSET_BYTES + 4

type (
	BedRegion struct {
		Location *dna.Location `json:"loc"`
		Name     string        `json:"name,omitempty"`
		Tags     string        `json:"tags,omitempty"`
		Score    float64       `json:"score"`
	}

	BedSample struct {
		Id       string   `json:"id"`
		Platform string   `json:"platform"`
		Genome   string   `json:"genome"`
		Assembly string   `json:"assembly"`
		Dataset  string   `json:"dataset"`
		Name     string   `json:"name"`
		Type     string   `json:"type"`
		Url      string   `json:"url"`
		Tags     []string `json:"tags"`
		Regions  int      `json:"regions"`
	}

	BedReader struct {
		file string
	}
)

const (
	SelectBedSql = `SELECT DISTINCT 
		s.id, 
		d.genome, 
		d.assembly, 
		d.platform, 
		d.name AS dataset_name, 
		s.name AS sample_name, 
		s.type, 
		s.regions, 
		s.url, 
		s.tags
	FROM samples s
	JOIN datasets d ON s.dataset_id = d.id
	JOIN dataset_permissions dp ON d.id = dp.dataset_id
	JOIN permissions p ON dp.permission_id = p.id
	WHERE 
		<<PERMISSIONS>>`

	BedsSql = SelectBedSql +
		` AND assembly = :assembly AND platform = :platform 
		ORDER BY name`

	BedFromIdSql = SelectBedSql +
		` AND s.id = :id
		ORDER BY d.genome, d.platform, s.name`

	BaseSearchSamplesSql = SelectBedSql +
		` AND d.assembly = :assembly`

	AllBedsSql = BaseSearchSamplesSql +
		` ORDER BY 
			d.platform, 
			d.name, 
			s.name`

	SearchBedSql = BaseSearchSamplesSql +
		` AND (s.id = :id OR d.id = :id OR d.platform = :id OR d.name LIKE :q OR s.name LIKE :q)
		ORDER BY 
			d.platform, 
			d.name, 
			s.name`

	OverlappingRegionsSql = `SELECT c.name, r.start, r.end, r.score, r.name, r.tags 
		FROM regions r
		JOIN chromosomes c ON r.chr_id = c.id
		WHERE c.name = :chr AND (r.start <= :end AND r.end >= :start)
		ORDER BY c.name, r.start`
)

func NewBedReader(file string) (*BedReader, error) {
	return &BedReader{file: file}, nil
}

func (reader *BedReader) OverlappingRegions(location *dna.Location) ([]*BedRegion, error) {
	ret := make([]*BedRegion, 0, 10)

	//log.Debug().Msgf("hmm %s", reader.file)

	db, err := sql.Open(sys.Sqlite3DB, reader.file)

	if err != nil {
		return ret, err
	}

	defer db.Close()

	rows, err := db.Query(OverlappingRegionsSql,
		sql.Named("chr", location.Chr()),
		sql.Named("start", location.Start()),
		sql.Named("end", location.End()))

	//log.Debug().Msgf("query done %s", OverlappingRegionsSql, location.Chr(), location.Start(), location.End())

	if err != nil {
		return ret, err
	}

	var chr string
	var start int
	var end int
	var score float64
	var name string
	var tags string

	for rows.Next() {
		err := rows.Scan(&chr, &start, &end, &score, &name, &tags)

		if err != nil {
			return ret, err //fmt.Errorf("there was an error with the database records")
		}

		location, err := dna.NewLocation(chr, start, end)

		if err != nil {
			return ret, err
		}

		ret = append(ret, &BedRegion{Location: location, Score: score, Name: name, Tags: tags})
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

func (bedb *BedsDB) Dir() string {
	return bedb.dir
}

func NewBedsDB(dir string) *BedsDB {
	db := sys.Must(sql.Open(sys.Sqlite3DB, filepath.Join(dir, "samples.db?mode=ro")))

	//stmtAllBeds := sys.Must(db.Prepare(ALL_BEDS_SQL))
	//stmtSearchBeds := sys.Must(db.Prepare(SEARCH_BED_SQL))
	//stmtBedFromId := sys.Must(db.Prepare(BED_FROM_ID_SQL))

	return &BedsDB{dir: dir,
		db: db,
	}
}

// func (bedsDb *BedsDB) Genomes() ([]string, error) {
// 	rows, err := bedsDb.db.Query(GenomesSql)

// 	if err != nil {
// 		return nil, err //fmt.Errorf("there was an error with the database query")
// 	}

// 	ret := make([]string, 0, 10)

// 	defer rows.Close()

// 	var genome string

// 	for rows.Next() {
// 		err := rows.Scan(&genome)

// 		if err != nil {
// 			return nil, err //fmt.Errorf("there was an error with the database records")
// 		}

// 		ret = append(ret, genome)
// 	}

// 	return ret, nil
// }

func (bedb *BedsDB) CanViewSample(sampleId string, isAdmin bool, permissions []string) error {
	namedArgs := []any{sql.Named("id", sampleId)}

	query := sqlite.MakePermissionsSql(seqs.CanViewSampleSql, isAdmin, permissions, &namedArgs)

	var id string
	err := bedb.db.QueryRow(query, namedArgs...).Scan(&id)

	// no rows means no permission
	if err != nil {
		return err
	}

	// sanity
	if id != sampleId {
		return fmt.Errorf("permission denied to view sample %s", sampleId)
	}

	return nil
}

func (bedb *BedsDB) Platforms(assembly string, isAdmin bool, permissions []string) ([]*seqs.Platform, error) {
	namedArgs := []any{sql.Named("assembly", assembly)}

	query := sqlite.MakePermissionsSql(seqs.PlatformsSql, isAdmin, permissions, &namedArgs)

	rows, err := bedb.db.Query(query, namedArgs...)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

	defer rows.Close()

	ret := make([]*seqs.Platform, 0, 10)

	for rows.Next() {
		var platform seqs.Platform

		err := rows.Scan(&platform.Genome,
			&platform.Assembly,
			&platform.Platform)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		ret = append(ret, &platform)
	}

	return ret, nil
}

func (bedb *BedsDB) Datasets(assembly string, isAdmin bool, permissions []string) ([]*seqs.Dataset, error) {
	// build sql.Named args
	namedArgs := []any{sql.Named("assembly", assembly)}

	query := sqlite.MakePermissionsSql(seqs.DatasetsSql, isAdmin, permissions, &namedArgs)

	// execute query

	rows, err := bedb.db.Query(query, namedArgs...)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

	defer rows.Close()

	ret := make([]*seqs.Dataset, 0, 10)

	for rows.Next() {
		var dataset seqs.Dataset

		err := rows.Scan(&dataset.Id,
			&dataset.Assembly,
			&dataset.Platform,
			&dataset.Name)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		ret = append(ret, &dataset)
	}

	return ret, nil
}

func (bedb *BedsDB) PlatformDatasets(platform string, assembly string, isAdmin bool, permissions []string) ([]*seqs.Dataset, error) {
	// build sql.Named args

	namedArgs := []any{sql.Named("assembly", assembly), sql.Named("platform", platform)}

	query := sqlite.MakePermissionsSql(seqs.PlatformDatasetsSql, isAdmin, permissions, &namedArgs)

	// execute query

	rows, err := bedb.db.Query(query, namedArgs...)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

	defer rows.Close()

	ret := make([]*seqs.Dataset, 0, 10)

	for rows.Next() {
		var dataset seqs.Dataset

		err := rows.Scan(&dataset.Id,
			&dataset.Assembly,
			&dataset.Platform,
			&dataset.Name)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		ret = append(ret, &dataset)
	}

	return ret, nil
}

func (bedb *BedsDB) Beds(genome string, platform string) ([]*BedSample, error) {
	rows, err := bedb.db.Query(BedsSql, genome, platform)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

	defer rows.Close()

	ret := make([]*BedSample, 0, 10)

	var tags string

	for rows.Next() {
		sample, err := rowsToSample(rows)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		sample.Tags = seqs.TagsToList(tags)

		ret = append(ret, sample)
	}

	return ret, nil
}

func (bedb *BedsDB) Search(q string, assembly string, isAdmin bool, permissions []string) ([]*BedSample, error) {
	var rows *sql.Rows
	var err error

	// if platform != "" {
	// 	// platform specific search
	// 	rows, err = sdb.db.Query(SearchPlatformSamplesSql,
	// 		sql.Named("assembly", assembly),
	// 		sql.Named("platform", platform),
	// 		sql.Named("id", query),
	// 		sql.Named("q", fmt.Sprintf("%%%s%%", query)))

	// } else {
	//search all platforms within assembly

	if q != "" {
		namedArgs := []any{sql.Named("assembly", assembly),
			sql.Named("id", q),
			sql.Named("name", fmt.Sprintf("%%%s%%", q))}

		query := sqlite.MakePermissionsSql(SearchBedSql, isAdmin, permissions, &namedArgs)

		log.Debug().Msgf("searching beds for query '%s' assembly '%s'", q, query)

		rows, err = bedb.db.Query(query, namedArgs...)
	} else {
		namedArgs := []any{sql.Named("assembly", assembly)}

		query := sqlite.MakePermissionsSql(AllBedsSql, isAdmin, permissions, &namedArgs)

		log.Debug().Msgf("searching beds for query '%s' assembly '%s'", query, AllBedsSql)

		rows, err = bedb.db.Query(query, namedArgs...)
	}

	if err != nil {
		log.Error().Msgf("error querying beds: %v", err)
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

	defer rows.Close()

	ret := make([]*BedSample, 0, 10)

	var tags string

	for rows.Next() {
		sample, err := rowsToSample(rows)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		sample.Tags = seqs.TagsToList(tags)

		ret = append(ret, sample)
	}

	return ret, nil
}

func (bedb *BedsDB) ReaderFromId(sampleId string, isAdmin bool, permissions []string) (*BedReader, error) {
	namedArgs := []any{sql.Named("id", sampleId)}

	query := sqlite.MakePermissionsSql(BedFromIdSql, isAdmin, permissions, &namedArgs)

	row := bedb.db.QueryRow(query, namedArgs...)

	sample, err := rowToSample(row)

	if err != nil {
		return nil, err
	}

	url := filepath.Join(bedb.dir, sample.Url+sys.SqliteReadOnlySuffix)

	return NewBedReader(url)
}

func rowToSample(rows *sql.Row) (*BedSample, error) {
	var sample BedSample
	var tags string

	err := rows.Scan(&sample.Id,
		&sample.Genome,
		&sample.Assembly,
		&sample.Platform,
		&sample.Dataset,
		&sample.Name,
		&sample.Type,
		&sample.Regions,
		&sample.Url,
		&tags)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database records")
	}

	sample.Tags = seqs.TagsToList(tags)

	return &sample, nil
}

func rowsToSample(rows *sql.Rows) (*BedSample, error) {
	var sample BedSample
	var tags string

	err := rows.Scan(&sample.Id,
		&sample.Genome,
		&sample.Assembly,
		&sample.Platform,
		&sample.Dataset,
		&sample.Name,
		&sample.Type,
		&sample.Regions,
		&sample.Url,
		&tags)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database records")
	}

	sample.Tags = seqs.TagsToList(tags)

	return &sample, nil
}
