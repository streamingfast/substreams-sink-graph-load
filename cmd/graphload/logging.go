package main

import (
	"github.com/streamingfast/cli"
	"github.com/streamingfast/logging"
)

var zlog, tracer = logging.RootLogger("sink-graphcsv", "github.com/streamingfast/substreams-graph-load/cmd/graphload")

func init() {
	cli.SetLogger(zlog, tracer)
}
