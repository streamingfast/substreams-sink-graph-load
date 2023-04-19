package sinker

import (
	"context"
	"fmt"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"go.uber.org/zap"
)

type CSVSinker struct {
	*shutter.Shutter
	*sink.Sinker
	destFolder string

	logger *zap.Logger
	tracer logging.Tracer

	stats      *Stats
	lastCursor *sink.Cursor
}

func New(sink *sink.Sinker, destFolder string, logger *zap.Logger, tracer logging.Tracer) (*CSVSinker, error) {
	s := &CSVSinker{
		Shutter: shutter.New(),
		Sinker:  sink,

		destFolder: destFolder,
		logger:     logger,
		tracer:     tracer,

		stats: NewStats(logger),
	}

	s.OnTerminating(func(err error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s.writeLastCursor(ctx, err)
	})

	return s, nil
}

func (s *CSVSinker) writeLastCursor(ctx context.Context, err error) {
	if s.lastCursor == nil || err != nil {
		return
	}
	// FIXME: write cursor to file

	//_ = s.WriteCursor(ctx, s.OutputModuleHash(), s.lastCursor)
}

func (s *CSVSinker) Run(ctx context.Context) {

	cursor := sink.MustNewCursor("")
	// FIXME: get cursor from files
	//cursor, err := s.GetCursor(ctx, s.OutputModuleHash())
	//if err != nil && !errors.Is(err, ErrCursorNotFound) {
	//	s.Shutdown(fmt.Errorf("unable to retrieve cursor: %w", err))
	//	return
	//}

	s.Sinker.OnTerminating(s.Shutdown)
	s.OnTerminating(func(err error) {
		s.stats.LogNow()
		s.logger.Info("csv sinker terminating", zap.Stringer("last_block_written", s.stats.lastBlock))
		s.Sinker.Shutdown(err)
	})

	s.OnTerminating(func(_ error) { s.stats.Close() })
	s.stats.OnTerminated(func(err error) { s.Shutdown(err) })

	logEach := 15 * time.Second
	if s.logger.Core().Enabled(zap.DebugLevel) {
		logEach = 5 * time.Second
	}

	s.stats.Start(logEach, cursor)

	//s.logger.Info("starting graphcsv sink", zap.Duration("stats_refresh_each", logEach), zap.Stringer("restarting_at", cursor.Block))
	fmt.Println("about to run sinker with", cursor.String())
	s.Sinker.Run(ctx, cursor, sink.NewSinkerHandlers(s.handleBlockScopedData, s.handleBlockUndoSignal))
}

func (s *CSVSinker) handleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	output := data.Output

	if output.Name != s.OutputModuleName() {
		return fmt.Errorf("received data from wrong output module, expected to received from %q but got module's output for %q", s.OutputModuleName(), output.Name)
	}

	if data.Output != nil && data.Output != nil && len(data.Output.MapOutput.Value) != 0 {
		s.logger.Info("getting data from block", zap.Stringer("block", data.Clock))
	} else {
		s.logger.Info("getting empty block", zap.Stringer("block", data.Clock))
	}
	//	entityChanges := &pbentity
	//err := proto.Unmarshal(output.GetMapOutput().GetValue(), dbChanges)
	//if err != nil {
	//	return fmt.Errorf("unmarshal database changes: %w", err)
	//}

	//err = s.applyDatabaseChanges(ctx, dataAsBlockRef(data), dbChanges)
	//if err != nil {
	//	return fmt.Errorf("apply database changes: %w", err)
	//}

	s.lastCursor = cursor

	return nil
}

func (s *CSVSinker) handleBlockUndoSignal(ctx context.Context, data *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) error {
	return fmt.Errorf("received undo signal but there is no handling of undo, this is because you used `--undo-buffer-size=0` which is invalid right now")
}

func dataAsBlockRef(blockData *pbsubstreamsrpc.BlockScopedData) bstream.BlockRef {
	return clockAsBlockRef(blockData.Clock)
}

func clockAsBlockRef(clock *pbsubstreams.Clock) bstream.BlockRef {
	return bstream.NewBlockRef(clock.Id, clock.Number)
}
