package app

import (
	"github.com/BrobridgeOrg/gravity-transmitter-mongodb/pkg/database"
	"github.com/BrobridgeOrg/gravity-transmitter-mongodb/pkg/grpc_server"
	"github.com/BrobridgeOrg/gravity-transmitter-mongodb/pkg/mux_manager"
)

type App interface {
	GetGRPCServer() grpc_server.Server
	GetMuxManager() mux_manager.Manager
	GetWriter() database.Writer
}
