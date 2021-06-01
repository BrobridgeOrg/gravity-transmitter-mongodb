package app

import (
	"github.com/BrobridgeOrg/gravity-transmitter-mongodb/pkg/database"
)

type App interface {
	GetWriter() database.Writer
}
