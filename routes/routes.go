package routes

import (
	"errors"

	"github.com/gin-gonic/gin"

	"github.com/antonybholmes/go-beds"
	"github.com/antonybholmes/go-beds/beddb"
	"github.com/antonybholmes/go-dna"
	"github.com/antonybholmes/go-web"
)

var (
	ErrNoBedsSupplied = errors.New("at least 1 bed id must be supplied")
)

type (
	ReqBedsParams struct {
		Location string   `json:"location"`
		Beds     []string `json:"beds"`
	}

	BedsParams struct {
		Location *dna.Location `json:"location"`
		Beds     []string      `json:"beds"`
	}
)

func ParseBedParamsFromPost(c *gin.Context) (*BedsParams, error) {

	var params ReqBedsParams

	err := c.Bind(&params)

	if err != nil {
		return nil, err
	}

	location, err := dna.ParseLocation(params.Location)

	if err != nil {
		return nil, err
	}

	return &BedsParams{Location: location, Beds: params.Beds}, nil
}

func GenomeRoute(c *gin.Context) {
	platforms, err := beddb.Genomes()

	if err != nil {
		c.Error(err)
		return
	}

	web.MakeDataResp(c, "", platforms)
}

func PlatformRoute(c *gin.Context) {
	genome := c.Param("assembly")

	platforms, err := beddb.Platforms(genome)

	if err != nil {
		c.Error(err)
		return
	}

	web.MakeDataResp(c, "", platforms)
}

func SearchBedsRoute(c *gin.Context) {
	genome := c.Param("assembly")

	if genome == "" {
		web.BadReqResp(c, ErrNoBedsSupplied)
	}

	query := c.Query("search")

	tracks, err := beddb.Search(genome, query)

	if err != nil {
		c.Error(err)
		return
	}

	web.MakeDataResp(c, "", tracks)
}

func BedRegionsRoute(c *gin.Context) {

	params, err := ParseBedParamsFromPost(c)

	if err != nil {
		c.Error(err)
		return
	}

	if len(params.Beds) == 0 {
		web.BadReqResp(c, ErrNoBedsSupplied)
	}

	ret := make([][]*beds.BedRegion, 0, len(params.Beds))

	for _, bed := range params.Beds {

		//log.Debug().Msgf("bed id %s", bed)

		reader, err := beddb.ReaderFromId(bed)

		if err != nil {
			c.Error(err)
			return
		}

		features, _ := reader.OverlappingRegions(params.Location)

		ret = append(ret, features)
	}

	web.MakeDataResp(c, "", ret)
}
