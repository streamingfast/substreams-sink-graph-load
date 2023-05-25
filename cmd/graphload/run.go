package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/streamingfast/cli"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	"github.com/streamingfast/shutter"
	"github.com/streamingfast/substreams-graph-load/schema"
	"github.com/streamingfast/substreams-graph-load/sinker"
	sink "github.com/streamingfast/substreams-sink"
	"go.uber.org/zap"
)

const (
	SUPPORTED_MODULE_TYPE = "sf.substreams.entity.v1.EntityChanges"
	LEGACY_MODULE_TYPE    = "substreams.entity.v1.EntityChanges"
)

var SinkRunCmd = Command(sinkRunE,
	"run (--entities|--graphql-schema) <destination-folder> <endpoint> <manifest> <module> <stopBlock>",
	"Runs substreams sinker to CSV files",
	ExactArgs(5),
	Flags(func(flags *pflag.FlagSet) {

		flags.BoolP("insecure", "k", false, "Skip certificate validation on GRPC connection")
		flags.BoolP("plaintext", "p", false, "Establish GRPC connection in plaintext")
		flags.Bool("development-mode", false, "Enable development mode, use it for testing purpose only, should not be used for production workload")
		flags.Bool("infinite-retry", false, "Default behavior is to retry 15 times spanning approximatively 5m before exiting with an error, activating this flag will retry forever")

		// so we can use sinker.NewFromViper
		flags.Int("undo-buffer-size", 0, "DO NOT TOUCH THIS FLAG")
		flags.Duration("live-block-time-delta", 300*time.Second, "DO NOT TOUCH THIS FLAG")
		flags.Bool("final-blocks-only", true, "DO NOT TOUCH THIS FLAG")
		// Deprecated flags from sinker.NewFromViper
		flags.Bool("irreversible-only", false, "DO NOT TOUCH THIS FLAG")
		flags.Lookup("irreversible-only").Deprecated = "Renamed to --final-blocks-only"

		flags.Uint64("bundle-size", 1000, "Size of output bundle, in blocks")
		flags.String("start-block", "", "Start processing at this block instead of the substreams initial block")
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
	stopBlock := args[4]

	startBlock := sflags.MustGetString(cmd, "start-block") // empty string by default makes valid ':endBlock' range

	blockRange := startBlock + ":" + stopBlock

	sink, err := sink.NewFromViper(
		cmd,
		sink.IgnoreOutputModuleType,
		endpoint, manifestPath, outputModuleName, blockRange,
		zlog,
		tracer,
		sink.WithFinalBlocksOnly(),
	)
	if err != nil {
		return fmt.Errorf("unable to setup sinker: %w", err)
	}

	outputModuleType := sink.OutputModuleTypeUnprefixed()

	if outputModuleType != SUPPORTED_MODULE_TYPE && outputModuleType != LEGACY_MODULE_TYPE {
		return fmt.Errorf("sink only supports map module with output type %q (or %q) but selected module %q output type is %q", SUPPORTED_MODULE_TYPE, LEGACY_MODULE_TYPE, outputModuleName, outputModuleType)
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

	entitySink, err := sinker.New(sink, destFolder, workingDir, entities, bundleSize, bufferSize, chainID, zlog, tracer)
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
