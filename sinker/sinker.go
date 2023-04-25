package sinker

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-graphcsv/bundler"
	"github.com/streamingfast/substreams-sink-graphcsv/bundler/writer"
	pbentity "github.com/streamingfast/substreams-sink-graphcsv/pb/entity/v1"
	"github.com/streamingfast/substreams-sink-graphcsv/schema"
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
	poiBundler   *bundler.Bundler
	chainID      string

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
	chainID string,
	logger *zap.Logger,
	tracer logging.Tracer) (*EntitiesSink, error) {
	s := &EntitiesSink{
		Shutter: shutter.New(),
		Sinker:  sink,

		fileBundlers: make(map[string]*bundler.Bundler),
		destFolder:   destFolder,
		logger:       logger,
		tracer:       tracer,
		chainID:      chainID,

		stats: NewStats(logger),
	}

	baseOutputStore, err := dstore.NewJSONLStore(destFolder)
	if err != nil {
		return nil, err
	}

	for _, entity := range entities {
		fb, err := getBundler(entity, s.Sinker.BlockRange().StartBlock(), bundleSize, bufferSize, baseOutputStore, workingDir, logger)
		if err != nil {
			return nil, err
		}
		s.fileBundlers[entity] = fb
	}

	poiBundler, err := getBundler(schema.PoiEntityName, s.Sinker.BlockRange().StartBlock(), bundleSize, bufferSize, baseOutputStore, workingDir, logger)
	if err != nil {
		return nil, err
	}
	s.poiBundler = poiBundler

	return s, nil
}

func getBundler(entity string, startBlock, bundleSize, bufferSize uint64, baseOutputStore dstore.Store, workingDir string, logger *zap.Logger) (*bundler.Bundler, error) {
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
	fb.Start(startBlock)
	return fb, nil

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

	uploadContext := context.Background()
	for _, fb := range s.fileBundlers {
		fb.Launch(uploadContext)
	}
	s.poiBundler.Launch(uploadContext)

	s.Sinker.Run(ctx, nil, sink.NewSinkerHandlers(s.handleBlockScopedData, s.handleBlockUndoSignal))
}

func (s *EntitiesSink) handleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	output := data.Output

	if output.Name != s.OutputModuleName() {
		return fmt.Errorf("received data from wrong output module, expected to received from %q but got module's output for %q", s.OutputModuleName(), output.Name)
	}

	digest := sha256.New()
	blockHash, err := hex.DecodeString(data.Clock.Id)
	if err != nil {
		return fmt.Errorf("invalid clock received: %w", err)
	}
	digest.Write(blockHash)

	entityChanges := &pbentity.EntityChanges{}
	err = proto.Unmarshal(output.GetMapOutput().GetValue(), entityChanges)
	if err != nil {
		return fmt.Errorf("unmarshal entity changes: %w", err)
	}

	if data.Output == nil || data.Output.MapOutput == nil || len(data.Output.MapOutput.Value) == 0 {
		s.logger.Info("getting empty block", zap.Stringer("block", data.Clock))
	} else {
		s.logger.Info("entity changes", zap.Any("entity_changes", entityChanges))
	}

	for _, entityBundler := range s.fileBundlers {
		entityBundler.Roll(ctx, data.Clock.Number)
	}

	for _, change := range entityChanges.EntityChanges {
		jsonlChange, err := bundler.JSONLEncode(&pbentity.EntityChangeAtBlockNum{
			EntityChange: change,
			BlockNum:     data.Clock.Number,
		})
		if err != nil {
			return err
		}
		digest.Write(jsonlChange)
		entity := strings.ToLower(change.Entity)
		entityBundler, ok := s.fileBundlers[entity]
		if !ok {
			return fmt.Errorf("cannot get bundler writer for entity %s", entity)
		}
		entityBundler.Writer().Write(jsonlChange)
	}

	s.poiBundler.Roll(ctx, data.Clock.Number)
	poiEntity := getPOIEntity(digest.Sum(nil), s.chainID, data.Clock.Number)
	jsonlPOI, err := bundler.JSONLEncode(poiEntity)
	if err != nil {
		return err
	}
	s.poiBundler.Writer().Write(jsonlPOI)

	s.stats.RecordBlock(cursor.Block().Num())

	return nil
}

func (s *EntitiesSink) handleBlockUndoSignal(ctx context.Context, data *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) error {
	return fmt.Errorf("received undo signal: should not happen, substreams connection should be 'final-blocks-only' ")
}

func dataAsBlockRef(blockData *pbsubstreamsrpc.BlockScopedData) bstream.BlockRef {
	return clockAsBlockRef(blockData.Clock)
}

func clockAsBlockRef(clock *pbsubstreams.Clock) bstream.BlockRef {
	return bstream.NewBlockRef(clock.Id, clock.Number)
}

func getPOIEntity(digest []byte, chainID string, blockNum uint64) *pbentity.EntityChangeAtBlockNum {
	return &pbentity.EntityChangeAtBlockNum{
		BlockNum: blockNum,
		EntityChange: &pbentity.EntityChange{
			Entity: schema.PoiEntityName,
			Id:     chainID,
			// Ordinal
			Operation: pbentity.EntityChange_UPDATE,
			Fields: []*pbentity.Field{
				{
					Name: "digest",
					NewValue: &pbentity.Value{
						Typed: &pbentity.Value_Bytes{
							Bytes: base64.StdEncoding.EncodeToString(digest),
						},
					},
				},
			},
		},
	}
}
