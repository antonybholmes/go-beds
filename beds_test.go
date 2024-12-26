package beds

import (
	"testing"

	"github.com/antonybholmes/go-dna"
	"github.com/rs/zerolog/log"
)

func TestWithin(t *testing.T) {

	reader, err := NewBedReader("/home/antony/development/data/modules/beds/ChIP-seq/hg19/Peaks_CB4_BCL6_RK040_vs_Input_RK063_p12.db")

	if err != nil {
		t.Fatalf(`err %s`, err)
	}

	location, err := dna.ParseLocation("chr3:187441954-187466041")

	if err != nil {
		t.Fatalf(`err %s`, err)
	}

	features, err := reader.BedRegions(location)

	if err != nil {
		t.Fatalf(`err %s`, err)
	}

	log.Debug().Msgf("%v", features)
}
