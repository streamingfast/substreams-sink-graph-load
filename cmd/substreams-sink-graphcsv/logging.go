package main

import (
	"github.com/streamingfast/cli"
	"github.com/streamingfast/logging"
)

var zlog, tracer = logging.RootLogger("sink-graphcsv", "github.com/streamingfast/substreams-sink-graphcsv/cmd/substreams-sink-graphcsv")

func init() {
	cli.SetLogger(zlog, tracer)
}
