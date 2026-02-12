package beds

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/antonybholmes/go-dna"
	"github.com/antonybholmes/go-seqs"
	"github.com/antonybholmes/go-sys"
	"github.com/antonybholmes/go-sys/log"
	"github.com/antonybholmes/go-web"
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
		Tags     []string      `json:"tags,omitempty"`
		Score    float64       `json:"score,omitempty"`
	}

	SampleBedRegions struct {
		Sample  string       `json:"sample"`
		Regions []*BedRegion `json:"regions"`
	}

	BedSample struct {
		Id         string   `json:"id"`
		Technology string   `json:"technology"`
		Genome     string   `json:"genome"`
		Assembly   string   `json:"assembly"`
		Dataset    string   `json:"dataset"`
		Name       string   `json:"name"`
		Type       string   `json:"type"`
		Url        string   `json:"url"`
		Tags       []string `json:"tags,omitempty"`
		Regions    int      `json:"regions"`
	}

	// BedReader struct {
	// 	file string
	// }

	BedsDB struct {
		db *sql.DB
		//stmtAllBeds    *sql.Stmt
		//stmtSearchBeds *sql.Stmt
		//stmtBedFromId  *sql.Stmt
		dir string
	}
)

const (
	SelectBedSql = `SELECT DISTINCT 
		s.public_id, 
		g.name AS genome, 
		a.name AS assembly, 
		t.name AS technology, 
		d.name AS dataset_name, 
		s.name AS sample_name, 
		st.name AS type, 
		s.regions, 
		s.url, 
		s.tags
	FROM samples s
	JOIN technologies t ON s.technology_id = t.id
	JOIN sample_types st ON s.type_id = st.id
	JOIN datasets d ON s.dataset_id = d.id
	JOIN assemblies a ON d.assembly_id = a.id
	JOIN genomes g ON a.genome_id = g.id
	JOIN dataset_permissions dp ON d.id = dp.dataset_id
	JOIN permissions p ON dp.permission_id = p.id
	WHERE 
		<<PERMISSIONS>>
		AND (a.public_id =:assembly OR LOWER(a.name) = :assembly)`

	BedsSql = SelectBedSql +
		` ORDER BY
			t.name,
			d.name,
			s.name`

	BedFromIdSql = SelectBedSql +
		` AND s.id = :id
		ORDER BY
			t.name,
			d.name,
			s.name`

	AllBedsSql = SelectBedSql +
		` ORDER BY 
			t.name, 
			d.name, 
			s.name`

	SearchBedSql = SelectBedSql +
		` AND (
			d.public_id = :id 
			OR s.public_id = :id
			OR t.name LIKE :q
			OR d.name LIKE :q 
			OR s.name LIKE :q)
		ORDER BY 
			t.name, 
			d.name, 
			s.name`

	// we sort by c.id to ensure chromosome numerical order rather than lexicographical order
	OverlappingRegionsSql = `SELECT
		s.public_id,
		c.name, 
		r.start, 
		r.end, 
		r.name,
		r.score,
		r.tags 
		FROM regions r
		JOIN chromosomes c ON r.chr_id = c.id
		JOIN samples s ON r.sample_id = s.id
		JOIN datasets d ON s.dataset_id = d.id
		JOIN dataset_permissions dp ON d.id = dp.dataset_id
		JOIN permissions p ON dp.permission_id = p.id
		WHERE
			<<PERMISSIONS>>
			AND <<SAMPLES>>
			AND c.name = :chr 
			AND (r.start <= :end AND r.end >= :start)
		ORDER BY 
			s.public_id,
			c.id, 
			r.start`
)

func (bdb *BedsDB) Dir() string {
	return bdb.dir
}

func NewBedsDB(dir string) *BedsDB {
	db := sys.Must(sql.Open(sys.Sqlite3DB, filepath.Join(dir, "beds.db"+sys.SqliteReadOnlySuffix)))

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

func (bdb *BedsDB) CanViewSample(sampleId string, isAdmin bool, permissions []string) error {
	namedArgs := []any{sql.Named("id", sampleId)}

	query := sqlite.MakePermissionsSql(seqs.CanViewSampleSql, isAdmin, permissions, &namedArgs)

	var id string
	err := bdb.db.QueryRow(query, namedArgs...).Scan(&id)

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

// func (bdb *BedsDB) Platforms(assembly string, isAdmin bool, permissions []string) ([]*seqs.Platform, error) {
// 	namedArgs := []any{sql.Named("assembly", assembly)}

// 	query := sqlite.MakePermissionsSql(seqs.PlatformsSql, isAdmin, permissions, &namedArgs)

// 	rows, err := bdb.db.Query(query, namedArgs...)

// 	if err != nil {
// 		return nil, err //fmt.Errorf("there was an error with the database query")
// 	}

// 	defer rows.Close()

// 	ret := make([]*seqs.Platform, 0, 10)

// 	for rows.Next() {
// 		var platform seqs.Platform

// 		err := rows.Scan(&platform.Genome,
// 			&platform.Assembly,
// 			&platform.Platform)

// 		if err != nil {
// 			return nil, err //fmt.Errorf("there was an error with the database records")
// 		}

// 		ret = append(ret, &platform)
// 	}

// 	return ret, nil
// }

func (bdb *BedsDB) Datasets(assembly string, isAdmin bool, permissions []string) ([]*seqs.Dataset, error) {
	// build sql.Named args
	namedArgs := []any{sql.Named("assembly", assembly)}

	query := sqlite.MakePermissionsSql(seqs.DatasetsSql, isAdmin, permissions, &namedArgs)

	// execute query

	rows, err := bdb.db.Query(query, namedArgs...)

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

// func (bdb *BedsDB) PlatformDatasets(platform string, assembly string, isAdmin bool, permissions []string) ([]*seqs.Dataset, error) {
// 	// build sql.Named args

// 	namedArgs := []any{sql.Named("assembly", assembly), sql.Named("platform", platform)}

// 	query := sqlite.MakePermissionsSql(seqs.PlatformDatasetsSql, isAdmin, permissions, &namedArgs)

// 	// execute query

// 	rows, err := bdb.db.Query(query, namedArgs...)

// 	if err != nil {
// 		return nil, err //fmt.Errorf("there was an error with the database query")
// 	}

// 	defer rows.Close()

// 	ret := make([]*seqs.Dataset, 0, 10)

// 	for rows.Next() {
// 		var dataset seqs.Dataset

// 		err := rows.Scan(&dataset.Id,
// 			&dataset.Assembly,
// 			&dataset.Platform,
// 			&dataset.Name)

// 		if err != nil {
// 			return nil, err //fmt.Errorf("there was an error with the database records")
// 		}

// 		ret = append(ret, &dataset)
// 	}

// 	return ret, nil
// }

func (bdb *BedsDB) Beds(genome string, platform string) ([]*BedSample, error) {
	rows, err := bdb.db.Query(BedsSql, genome, platform)

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

func (bdb *BedsDB) Search(q string,
	assembly string,
	isAdmin bool,
	permissions []string) ([]*BedSample, error) {
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
		namedArgs := []any{sql.Named("assembly", web.FormatParam(assembly)),
			sql.Named("id", q),
			sql.Named("q", fmt.Sprintf("%%%s%%", q))}

		query := sqlite.MakePermissionsSql(SearchBedSql, isAdmin, permissions, &namedArgs)

		log.Debug().Msgf("searching beds for query '%s' assembly '%s'", q, query)

		rows, err = bdb.db.Query(query, namedArgs...)
	} else {
		namedArgs := []any{sql.Named("assembly", web.FormatParam(assembly))}

		query := sqlite.MakePermissionsSql(AllBedsSql, isAdmin, permissions, &namedArgs)

		log.Debug().Msgf("searching beds for query '%s' assembly '%s'", query, AllBedsSql)

		rows, err = bdb.db.Query(query, namedArgs...)
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

// func NewBedReader(file string) (*BedReader, error) {
// 	return &BedReader{file: file}, nil
// }

func (bdb *BedsDB) Regions(sampleIds []string, location *dna.Location, isAdmin bool, permissions []string) ([]*SampleBedRegions, error) {

	namedArgs := []any{sql.Named("chr", location.Chr()),
		sql.Named("start", location.Start()),
		sql.Named("end", location.End())}

	query := sqlite.MakePermissionsSql(OverlappingRegionsSql, isAdmin, permissions, &namedArgs)
	query = makeInSamplesSql(query, sampleIds, &namedArgs)

	rows, err := bdb.db.Query(query, namedArgs...)

	if err != nil {
		return nil, err
	}

	var sampleId string
	var chr string
	var start int
	var end int
	var score float64
	var name string
	var tags string
	var currentSampleBedRegion *SampleBedRegions

	ret := make([]*SampleBedRegions, 0, len(sampleIds))

	for rows.Next() {
		err := rows.Scan(&sampleId, &chr, &start, &end, &name, &score, &tags)

		if err != nil {
			return ret, err //fmt.Errorf("there was an error with the database records")
		}

		if currentSampleBedRegion == nil || currentSampleBedRegion.Sample != sampleId {
			currentSampleBedRegion = &SampleBedRegions{Sample: sampleId, Regions: make([]*BedRegion, 0, 10)}
			ret = append(ret, currentSampleBedRegion)
		}

		location, err := dna.NewLocation(chr, start, end)

		if err != nil {
			return ret, err
		}

		currentSampleBedRegion.Regions = append(currentSampleBedRegion.Regions,
			&BedRegion{Location: location, Name: name, Score: score, Tags: seqs.TagsToList(tags)})
	}

	return ret, nil

}

// func (bdb *BedsDB) SampleReader(sampleId string, isAdmin bool, permissions []string) (*BedReader, error) {
// 	namedArgs := []any{sql.Named("id", sampleId)}

// 	query := sqlite.MakePermissionsSql(BedFromIdSql, isAdmin, permissions, &namedArgs)

// 	row := bdb.db.QueryRow(query, namedArgs...)

// 	sample, err := rowToSample(row)

// 	if err != nil {
// 		return nil, err
// 	}

// 	url := filepath.Join(bdb.dir, sample.Url+sys.SqliteReadOnlySuffix)

// 	return NewBedReader(url)
// }

// func rowToSample(rows *sql.Row) (*BedSample, error) {
// 	var sample BedSample
// 	var tags string

// 	err := rows.Scan(&sample.Id,
// 		&sample.Genome,
// 		&sample.Assembly,
// 		&sample.Technology,
// 		&sample.Dataset,
// 		&sample.Name,
// 		&sample.Type,
// 		&sample.Regions,
// 		&sample.Url,
// 		&tags)

// 	if err != nil {
// 		return nil, err //fmt.Errorf("there was an error with the database records")
// 	}

// 	sample.Tags = seqs.TagsToList(tags)

// 	return &sample, nil
// }

func rowsToSample(rows *sql.Rows) (*BedSample, error) {
	var sample BedSample
	var tags string

	err := rows.Scan(&sample.Id,
		&sample.Genome,
		&sample.Assembly,
		&sample.Technology,
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

func makeInSamplesSql(query string, sampleIds []string, namedArgs *[]any) string {

	inPlaceholders := make([]string, len(sampleIds))

	for i, sid := range sampleIds {
		ph := fmt.Sprintf("s%d", i+1)
		inPlaceholders[i] = ":" + ph
		*namedArgs = append(*namedArgs, sql.Named(ph, sid))
	}

	clause := "s.public_id IN (" + strings.Join(inPlaceholders, ", ") + ")"

	return strings.Replace(query, "<<SAMPLES>>", clause, 1)

}
