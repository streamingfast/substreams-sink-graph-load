package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/streamingfast/cli"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-graphcsv/sinker"
	"github.com/streamingfast/substreams/manifest"
	"go.uber.org/zap"
)

var SinkRunCmd = Command(sinkRunE,
	"run (--entities|--subgraph-manifest) <destination-folder> <endpoint> <manifest> <module> <stopBlock>",
	"Runs substreams sinker to CSV files",
	ExactArgs(5),
	Flags(func(flags *pflag.FlagSet) {
		sink.AddFlagsToSet(flags)
		flags.Uint64("bundle-size", 1000, "Size of output bundle, in blocks")
		flags.Uint64("buffer-size", 1024*1024*1024, "Size of output buffer, in bytes")
		flags.String("entities", "", "Comma-separated list of entities to process (alternative to providing the subgraph manifest)")
		flags.String("subgraph-manifest", "", "Path to subgraph manifest file to read the list of entities automatically (alternative to setting 'entities' value)")
		flags.String("working-dir", "./workdir", "Path to local folder used as working directory")
	}),
)

func sinkRunE(cmd *cobra.Command, args []string) error {
	app := shutter.New()

	ctx, cancelApp := context.WithCancel(cmd.Context())
	app.OnTerminating(func(_ error) {
		cancelApp()
	})

	sink.RegisterMetrics()
	sinker.RegisterMetrics()

	destFolder := args[0]
	endpoint := args[1]
	manifestPath := args[2]
	outputModuleName := args[3]
	stopBlock, err := strconv.ParseUint(args[4], 10, 64)
	if err != nil {
		return fmt.Errorf("stopBlock must be a uint64")
	}

	// FIXME: in a function, just to get startBlock, then parse destination-folder
	pkg, err := manifest.NewReader(manifestPath).Read()
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	graph, err := manifest.NewModuleGraph(pkg.Modules.Modules)
	if err != nil {
		return fmt.Errorf("create substreams module graph: %w", err)
	}
	module, err := graph.Module(outputModuleName)
	if err != nil {
		return fmt.Errorf("create substreams module graph: %w", err)
	}
	blockRange := fmt.Sprintf("%d:%d", module.InitialBlock, stopBlock)
	////////////////////////////3

	sink, err := sink.NewFromViper(
		cmd,
		"proto:substreams.entity.v1.EntityChanges",
		endpoint, manifestPath, outputModuleName, blockRange,
		zlog,
		tracer,
	)
	if err != nil {
		return fmt.Errorf("unable to setup sinker: %w", err)
	}

	bundleSize := sflags.MustGetUint64(cmd, "bundle-size")
	bufferSize := sflags.MustGetUint64(cmd, "buffer-size")
	workingDir := sflags.MustGetString(cmd, "working-dir")
	entities := strings.Split(sflags.MustGetString(cmd, "entities"), ",")
	if len(entities) == 0 || (len(entities) == 1 && len(entities[0]) == 0) {
		return fmt.Errorf("you must have at least one entity, set by --entities or --subgraph-manifest")
	}
	// FIXME: get entities from subgraph-manifest if present, fail if both flags are set

	entitySink, err := sinker.New(sink, destFolder, workingDir, entities, bundleSize, bufferSize, zlog, tracer)
	if err != nil {
		return fmt.Errorf("unable to setup csv sinker: %w", err)
	}

	entitySink.OnTerminating(app.Shutdown)
	app.OnTerminating(func(err error) {
		entitySink.Shutdown(err)
	})

	go func() {
		entitySink.Run(ctx)
	}()

	zlog.Info("ready, waiting for signal to quit")

	signalHandler, isSignaled, _ := cli.SetupSignalHandler(0*time.Second, zlog)
	select {
	case <-signalHandler:
		go app.Shutdown(nil)
		break
	case <-app.Terminating():
		zlog.Info("run terminating", zap.Bool("from_signal", isSignaled.Load()), zap.Bool("with_error", app.Err() != nil))
		break
	}

	zlog.Info("waiting for run termination")
	select {
	case <-app.Terminated():
	case <-time.After(30 * time.Second):
		zlog.Warn("application did not terminate within 30s")
	}

	if err := app.Err(); err != nil {
		return err
	}

	zlog.Info("run terminated gracefully")
	return nil
}
