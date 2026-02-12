package routes

import (
	"errors"

	"github.com/gin-gonic/gin"

	"github.com/antonybholmes/go-beds/beddb"
	"github.com/antonybholmes/go-dna"
	"github.com/antonybholmes/go-web"
	"github.com/antonybholmes/go-web/auth"
	"github.com/antonybholmes/go-web/middleware"
)

var (
	ErrNoBedsSupplied = errors.New("at least 1 bed id must be supplied")
)

type (
	ReqBedsParams struct {
		Location string   `json:"location"`
		Samples  []string `json:"samples"`
	}

	BedsParams struct {
		Location *dna.Location `json:"location"`
		Samples  []string      `json:"samples"`
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

	return &BedsParams{Location: location, Samples: params.Samples}, nil
}

// func GenomeRoute(c *gin.Context) {
// 	platforms, err := beddb.Genomes()

// 	if err != nil {
// 		c.Error(err)
// 		return
// 	}

// 	web.MakeDataResp(c, "", platforms)
// }

// func PlatformsRoute(c *gin.Context) {
// 	middleware.JwtUserWithPermissionsRoute(c, func(c *gin.Context, isAdmin bool, user *auth.AuthUserJwtClaims) {

// 		assembly := c.Param("assembly")

// 		platforms, err := beddb.Platforms(assembly, isAdmin, user.Permissions)

// 		if err != nil {
// 			c.Error(err)
// 			return
// 		}

// 		web.MakeDataResp(c, "", platforms)
// 	})
// }

func SearchSamplesRoute(c *gin.Context) {
	middleware.JwtUserWithPermissionsRoute(c, func(c *gin.Context, isAdmin bool, user *auth.AuthUserJwtClaims) {
		assembly := c.Param("assembly")

		if assembly == "" {
			web.BadReqResp(c, ErrNoBedsSupplied)
			return
		}

		query := c.Query("search")

		samples, err := beddb.Search(query, assembly, isAdmin, user.Permissions)

		if err != nil {
			c.Error(err)
			return
		}

		web.MakeDataResp(c, "", samples)
	})
}

func BedRegionsRoute(c *gin.Context) {
	middleware.JwtUserWithPermissionsRoute(c, func(c *gin.Context, isAdmin bool, user *auth.AuthUserJwtClaims) {
		params, err := ParseBedParamsFromPost(c)

		if err != nil {
			c.Error(err)
			return
		}

		if len(params.Samples) == 0 {
			web.BadReqResp(c, ErrNoBedsSupplied)
			return
		}

		features, err := beddb.Regions(params.Samples, params.Location, isAdmin, user.Permissions)

		if err != nil {
			c.Error(err)
			return
		}

		web.MakeDataResp(c, "", features)
	})
}
