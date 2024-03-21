package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/streamingfast/cli"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	"github.com/streamingfast/shutter"
	"github.com/streamingfast/substreams-graph-load/csvprocessor"
	"github.com/streamingfast/substreams-graph-load/sinker"
	sink "github.com/streamingfast/substreams-sink"
	"go.uber.org/zap"
)

var toCSVCmd = Command(toCSVE,
	"tocsv <source_folder> <destination_folder> <entity> <stop_block>",
	"Create CSV files ready for insertion into postgresql",
	Description(`
		Process <source_folder>/<entity> to create CSV files ready for insertion into PostgreSQL to
		<destination_folder>/<entity>.

		Arguments:
		- <source_folder>: Folder containing one folder per entity with jsonl files, created with 'run' command.
		- <destination_folder>: Folder where CSV files will be created (a subfolder named as the entity will be automatically appended)
		- <entity>: Name of the entity (ex: 'transfers') that will be processed. You need to run one instance of 'tocsv' per instance.
		- <stop_block>: Where you want to stop creating CSV (usually, very close to chain HEAD)
	`),
	ExactArgs(4),
	Flags(func(flags *pflag.FlagSet) {
		sink.AddFlagsToSet(flags)
		flags.Uint64("bundle-size", 1000, "Size of output bundle, in blocks")
		flags.String("graphql-schema", "schema.graphql", "Path to graphql schema")
	}),
)

func toCSVE(cmd *cobra.Command, args []string) error {
	app := shutter.New()

	ctx, cancelApp := context.WithCancel(cmd.Context())
	app.OnTerminating(func(_ error) {
		cancelApp()
	})

	sink.RegisterMetrics()
	sinker.RegisterMetrics()

	sourceFolder := args[0]
	destFolder := args[1]
	entity := args[2]

	stopBlock, err := strconv.ParseUint(args[3], 10, 64)
	if err != nil {
		return fmt.Errorf("stopBlock must be a uint64, got %q", args[3])
	}

	bundleSize := sflags.MustGetUint64(cmd, "bundle-size")
	graphqlSchemaFilename := sflags.MustGetString(cmd, "graphql-schema")

	csvProc, err := csvprocessor.New(
		sourceFolder,
		destFolder,
		entity,
		stopBlock,
		bundleSize,
		graphqlSchemaFilename,
		zlog,
		tracer,
	)

	if err != nil {
		return err
	}

	csvProc.OnTerminating(app.Shutdown)
	app.OnTerminating(func(err error) {
		csvProc.Shutdown(err)
	})

	go csvProc.Run(ctx)
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
		zlog.Error("unsuccessful termination", zap.Error(err))
		return err
	}

	zlog.Info("run terminated gracefully")
	return nil
}
