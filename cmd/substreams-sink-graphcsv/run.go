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
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-graphcsv/sinker"
	"go.uber.org/zap"
)

var SinkRunCmd = Command(sinkRunE,
	"run <destination-folder> <endpoint> <manifest> <module> <stopBlock>",
	"Runs substreams sinker to CSV files",
	ExactArgs(5),
	Flags(func(flags *pflag.FlagSet) {
		sink.AddFlagsToSet(flags)
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
	if strings.Contains(stopBlock, ":") || strings.Contains(stopBlock, "-") {
		return fmt.Errorf("stopBlock must be a uint64")
	}

	sink, err := sink.NewFromViper(
		"substreams.entity.v1.EntityChanges",
		endpoint, manifestPath, outputModuleName, stopBlock,
		"run",
		zlog,
		tracer,
	)
	if err != nil {
		return fmt.Errorf("unable to setup sinker: %w", err)
	}

	csvSinker, err := sinker.New(sink, destFolder, zlog, tracer)
	if err != nil {
		return fmt.Errorf("unable to setup csv sinker: %w", err)
	}

	csvSinker.OnTerminating(app.Shutdown)
	app.OnTerminating(func(err error) {
		csvSinker.Shutdown(err)
	})

	go func() {
		csvSinker.Run(ctx)
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
