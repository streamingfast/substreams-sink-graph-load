package postgres

import (
	"github.com/streamingfast/cli"
	"github.com/streamingfast/logging"
)

var zlog, tracer = logging.PackageLogger("graph-load-postgres", "github.com/streamingfast/substreams-graph-load/postgres")

func init() {
	cli.SetLogger(zlog, tracer)
}
