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
	"github.com/streamingfast/substreams-sink-graphcsv/schema"
	"github.com/streamingfast/substreams-sink-graphcsv/sinker"
	"github.com/streamingfast/substreams/manifest"
	"go.uber.org/zap"
)

var SinkRunCmd = Command(sinkRunE,
	"run (--entities|--graphql-schema) <destination-folder> <endpoint> <manifest> <module> <stopBlock>",
	"Runs substreams sinker to CSV files",
	ExactArgs(5),
	Flags(func(flags *pflag.FlagSet) {
		// FIXME: this adds FinalBlockOnly, etc. which is ignored here
		sink.AddFlagsToSet(flags)
		flags.Uint64("bundle-size", 1000, "Size of output bundle, in blocks")
		flags.String("entities", "", "Comma-separated list of entities to process (alternative to providing the subgraph manifest)")
		flags.String("graphql-schema", "", "Path to graphql schema to read the list of entities automatically (alternative to setting 'entities' value)")
		flags.String("working-dir", "./workdir", "Path to local folder used as working directory")
		flags.String("chain-id", "ethereum/mainnet", "ID of the chain to appear in POI table")
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

	startBlock, err := getStartBlock(manifestPath, outputModuleName)
	if err != nil {
		return fmt.Errorf("getting startblock from substreams manifest: %w", err)
	}
	blockRange := fmt.Sprintf("%d:%d", startBlock, stopBlock)

	sink, err := sink.NewFromViper(
		cmd,
		"proto:substreams.entity.v1.EntityChanges",
		endpoint, manifestPath, outputModuleName, blockRange,
		zlog,
		tracer,
		sink.WithFinalBlocksOnly(), // always set this to true
	)
	if err != nil {
		return fmt.Errorf("unable to setup sinker: %w", err)
	}

	bundleSize := sflags.MustGetUint64(cmd, "bundle-size")
	bufferSize := uint64(10 * 1024) // too high, this wrecks havoc
	workingDir := sflags.MustGetString(cmd, "working-dir")
	chainID := sflags.MustGetString(cmd, "chain-id")

	graphqlSchemaFilename := sflags.MustGetString(cmd, "graphql-schema")

	var entities []string
	entitiesList := sflags.MustGetString(cmd, "entities")
	if entitiesList != "" {
		if graphqlSchemaFilename != "" {
			return fmt.Errorf("you must only use one of these flags: '--entities' or '--graphql-schema'")
		}
		entities = strings.Split(entitiesList, ",")
	} else {
		if graphqlSchemaFilename == "" {
			return fmt.Errorf("you must set one of these flags: '--entities' or '--graphql-schema'")
		}
		entities, err = schema.GetEntityNamesFromSchema(graphqlSchemaFilename)
		if err != nil {
			return err
		}
	}

	entitySink, err := sinker.New(sink, destFolder, workingDir, entities, bundleSize, bufferSize, stopBlock, chainID, zlog, tracer)
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

	<-entitySink.Terminated()
	zlog.Info("run terminated gracefully")
	return nil
}

func getStartBlock(manifestPath, outputModuleName string) (uint64, error) {
	pkg, err := manifest.NewReader(manifestPath).Read()
	if err != nil {
		return 0, fmt.Errorf("read manifest: %w", err)
	}
	graph, err := manifest.NewModuleGraph(pkg.Modules.Modules)
	if err != nil {
		return 0, fmt.Errorf("create substreams module graph: %w", err)
	}
	module, err := graph.Module(outputModuleName)
	if err != nil {
		return 0, fmt.Errorf("create substreams module graph: %w", err)
	}

	return module.InitialBlock, nil
}
