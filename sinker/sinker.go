package sinker

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-graphcsv/bundler"
	"github.com/streamingfast/substreams-sink-graphcsv/bundler/writer"
	pbentity "github.com/streamingfast/substreams-sink-graphcsv/pb/entity/v1"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type EntitiesSink struct {
	*shutter.Shutter
	*sink.Sinker
	destFolder string

	fileBundlers map[string]*bundler.Bundler

	logger *zap.Logger
	tracer logging.Tracer

	stats *Stats
}

func New(
	sink *sink.Sinker,
	destFolder string,
	workingDir string,
	entities []string,
	bundleSize uint64,
	bufferSize uint64,
	logger *zap.Logger,
	tracer logging.Tracer) (*EntitiesSink, error) {
	s := &EntitiesSink{
		Shutter: shutter.New(),
		Sinker:  sink,

		fileBundlers: make(map[string]*bundler.Bundler),
		destFolder:   destFolder,
		logger:       logger,
		tracer:       tracer,

		stats: NewStats(logger),
	}

	baseOutputStore, err := dstore.NewJSONLStore(destFolder)
	if err != nil {
		return nil, err
	}

	for _, entity := range entities {
		boundaryWriter := writer.NewBufferedIO(
			bufferSize,
			filepath.Join(workingDir, entity),
			writer.FileTypeJSONL,
			logger.With(zap.String("entity_name", entity)),
		)
		subStore, err := baseOutputStore.SubStore(entity)
		if err != nil {
			return nil, err
		}

		fb, err := bundler.New(bundleSize, boundaryWriter, subStore, logger)
		if err != nil {
			return nil, err
		}
		s.fileBundlers[entity] = fb
		fb.Start(s.Sinker.BlockRange().StartBlock())
	}

	return s, nil
}

func (s *EntitiesSink) Run(ctx context.Context) {
	s.Sinker.OnTerminating(s.Shutdown)
	s.OnTerminating(func(err error) {
		s.stats.LogNow()
		s.logger.Info("csv sinker terminating", zap.Uint64("last_block_written", s.stats.lastBlock))
		s.Sinker.Shutdown(err)
	})

	s.OnTerminating(func(_ error) { s.stats.Close() })
	s.stats.OnTerminated(func(err error) { s.Shutdown(err) })

	logEach := 15 * time.Second
	if s.logger.Core().Enabled(zap.DebugLevel) {
		logEach = 5 * time.Second
	}

	s.stats.Start(logEach)

	for _, fb := range s.fileBundlers {
		fb.Launch(context.Background())
	}
	s.Sinker.Run(ctx, nil, sink.NewSinkerHandlers(s.handleBlockScopedData, s.handleBlockUndoSignal))
}

func (s *EntitiesSink) handleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	output := data.Output

	if output.Name != s.OutputModuleName() {
		return fmt.Errorf("received data from wrong output module, expected to received from %q but got module's output for %q", s.OutputModuleName(), output.Name)
	}

	if data.Output == nil || data.Output.MapOutput == nil || len(data.Output.MapOutput.Value) == 0 {
		s.logger.Info("getting empty block", zap.Stringer("block", data.Clock))
		return nil
	}

	entityChanges := &pbentity.EntityChanges{}
	err := proto.Unmarshal(output.GetMapOutput().GetValue(), entityChanges)
	if err != nil {
		return fmt.Errorf("unmarshal entity changes: %w", err)
	}

	s.logger.Info("entity changes", zap.Any("entity_changes", entityChanges))
	for _, change := range entityChanges.EntityChanges {
		jsonlChange, err := bundler.JSONLEncode(change)
		if err != nil {
			return err
		}
		entityBundler, ok := s.fileBundlers[change.Entity]
		if !ok {
			return fmt.Errorf("cannot get bundler writer for entity %s", change.Entity)
		}
		entityBundler.Writer().Write(jsonlChange)
	}

	for _, entityBundler := range s.fileBundlers {
		entityBundler.Roll(ctx, data.Clock.Number)
	}
	//err = s.applyDatabaseChanges(ctx, dataAsBlockRef(data), dbChanges)
	//if err != nil {
	//	return fmt.Errorf("apply database changes: %w", err)
	//}

	s.stats.RecordBlock(cursor.Block().Num())

	return nil
}

func (s *EntitiesSink) handleBlockUndoSignal(ctx context.Context, data *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) error {
	return fmt.Errorf("received undo signal but there is no handling of undo, this is because you used `--undo-buffer-size=0` which is invalid right now")
}

func dataAsBlockRef(blockData *pbsubstreamsrpc.BlockScopedData) bstream.BlockRef {
	return clockAsBlockRef(blockData.Clock)
}

func clockAsBlockRef(clock *pbsubstreams.Clock) bstream.BlockRef {
	return bstream.NewBlockRef(clock.Id, clock.Number)
}
