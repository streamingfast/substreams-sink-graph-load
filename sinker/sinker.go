package sinker

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/streamingfast/dstore"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-graphcsv/bundler"
	"github.com/streamingfast/substreams-sink-graphcsv/bundler/writer"
	pbentity "github.com/streamingfast/substreams-sink-graphcsv/pb/entity/v1"
	"github.com/streamingfast/substreams-sink-graphcsv/poi"
	"github.com/streamingfast/substreams-sink-graphcsv/schema"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type EntitiesSink struct {
	*shutter.Shutter
	*sink.Sinker
	destFolder string

	fileBundlers map[string]*bundler.Bundler
	poiBundler   *bundler.Bundler
	stopBlock    uint64
	chainID      string
	lastPOI      []byte

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
	stopBlock uint64,
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
		stopBlock:    stopBlock,

		stats: NewStats(logger),
	}

	baseOutputStore, err := dstore.NewJSONLStore(destFolder)
	if err != nil {
		return nil, err
	}

	for _, entity := range entities {
		fb, err := getBundler(entity, s.Sinker.BlockRange().StartBlock(), stopBlock, bundleSize, bufferSize, baseOutputStore, workingDir, logger)
		if err != nil {
			return nil, err
		}
		s.fileBundlers[entity] = fb
	}

	poiBundler, err := getBundler(schema.PoiEntityName, s.Sinker.BlockRange().StartBlock(), stopBlock, bundleSize, bufferSize, baseOutputStore, workingDir, logger)
	if err != nil {
		return nil, err
	}
	s.poiBundler = poiBundler

	return s, nil
}

func getBundler(entity string, startBlock, stopBlock, bundleSize, bufferSize uint64, baseOutputStore dstore.Store, workingDir string, logger *zap.Logger) (*bundler.Bundler, error) {
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

	fb, err := bundler.New(bundleSize, stopBlock, boundaryWriter, subStore, logger)
	if err != nil {
		return nil, err
	}
	fb.Start(startBlock)
	return fb, nil

}

func (s *EntitiesSink) CloseAllFileBundlers(err error) {
	var wg sync.WaitGroup
	for _, fb := range s.fileBundlers {
		wg.Add(1)
		f := fb
		go func() {
			f.Shutdown(err)
			<-f.Terminated()
			wg.Done()
		}()
	}
	s.poiBundler.Shutdown(err)
	<-s.poiBundler.Terminated()
	wg.Wait()
}

func (s *EntitiesSink) Run(ctx context.Context) {
	s.Sinker.OnTerminating(s.Shutdown)
	s.OnTerminating(func(err error) {
		s.stats.LogNow()
		s.logger.Info("csv sinker terminating", zap.Uint64("last_block_written", s.stats.lastBlock))
		if err == nil {
			s.handleStopBlockReached(ctx)
		}
		s.CloseAllFileBundlers(err)
		s.stats.Close()
		s.Sinker.Shutdown(err)
	})

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

func (s *EntitiesSink) handleStopBlockReached(ctx context.Context) error {
	s.rollAllBundlers(ctx, s.stopBlock)

	store, err := dstore.NewSimpleStore(s.destFolder)
	if err != nil {
		return fmt.Errorf("failed to initialize store at path %s: %w", s.destFolder, err)
	}
	lastBlockAndCursor := fmt.Sprintf("%d:%s\n", s.stats.lastBlock, s.stats.lastBlockHash)
	if err := store.WriteObject(context.Background(), "last_block.txt", bytes.NewReader([]byte(lastBlockAndCursor))); err != nil {
		s.logger.Warn("could not write last block")
	}

	return nil
}

func (s *EntitiesSink) rollAllBundlers(ctx context.Context, blockNum uint64) {
	var wg sync.WaitGroup
	for _, entityBundler := range s.fileBundlers {
		wg.Add(1)
		eb := entityBundler
		go func() {
			if err := eb.Roll(ctx, blockNum); err != nil {
				// no worries, Shutdown can and will be called multiple times
				if errors.Is(err, bundler.ErrStopBlockReached) {
					err = nil
				}
				s.Shutdown(err)
			}
			wg.Done()
		}()
	}

	s.poiBundler.Roll(ctx, blockNum)
	wg.Wait()
}

func (s *EntitiesSink) handleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, _ *bool, cursor *sink.Cursor) error {
	if s.IsTerminating() {
		return nil
	}
	output := data.Output

	if output.Name != s.OutputModuleName() {
		return fmt.Errorf("received data from wrong output module, expected to received from %q but got module's output for %q", s.OutputModuleName(), output.Name)
	}

	entityChanges := &pbentity.EntityChanges{}
	err := proto.Unmarshal(output.GetMapOutput().GetValue(), entityChanges)
	if err != nil {
		return fmt.Errorf("unmarshal entity changes: %w", err)
	}

	if s.tracer.Enabled() {
		if data.Output == nil || data.Output.MapOutput == nil || len(data.Output.MapOutput.Value) == 0 {
			s.logger.Debug("getting empty block", zap.Stringer("block", data.Clock))
		} else {
			s.logger.Debug("entity changes", zap.Any("entity_changes", entityChanges))
		}
	}

	s.rollAllBundlers(ctx, data.Clock.Number)
	if s.IsTerminating() {
		return nil
	}

	proofOfIndexing := poi.NewProofOfIndexing(data.Clock.Number, poi.VersionFast)

	for _, change := range entityChanges.EntityChanges {
		jsonlChange, err := bundler.JSONLEncode(&pbentity.EntityChangeAtBlockNum{
			EntityChange: change,
			BlockNum:     data.Clock.Number,
		})
		if err != nil {
			return err
		}

		entity := schema.NormalizeField(change.Entity)
		entityBundler, ok := s.fileBundlers[entity]
		if !ok {
			return fmt.Errorf("cannot get bundler writer for entity %s", entity)
		}
		entityBundler.Writer().Write(jsonlChange)

		if err := addEntityChangeToPOI(proofOfIndexing, change); err != nil {
			return fmt.Errorf("entity change POI: %w", err)
		}
	}

	poi, err := proofOfIndexing.Pause(s.lastPOI)
	if err != nil {
		return fmt.Errorf("pause proof of indexing: %w", err)
	}

	poiEntity := getPOIEntity(poi, s.chainID, data.Clock.Number)
	jsonlPOI, err := bundler.JSONLEncode(poiEntity)
	if err != nil {
		return err
	}
	s.poiBundler.Writer().Write(jsonlPOI)

	s.lastPOI = poi
	s.stats.RecordBlock(cursor.Block().Num())
	s.stats.RecordLastBlockHash(cursor.Block().ID())

	return nil
}

func addEntityChangeToPOI(proofOfIndexing *poi.ProofOfIndexing, change *pbentity.EntityChange) error {
	switch change.Operation {
	case pbentity.EntityChange_CREATE, pbentity.EntityChange_UPDATE, pbentity.EntityChange_FINAL:
		proofOfIndexing.SetEntity(change)

	case pbentity.EntityChange_DELETE:
		proofOfIndexing.RemoveEntity(change)

	case pbentity.EntityChange_UNSET:
		return fmt.Errorf("received %q operation which is should never be sent", change.Operation)
	}

	return nil
}

func (s *EntitiesSink) handleBlockUndoSignal(ctx context.Context, data *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) error {
	return fmt.Errorf("received undo signal: should not happen, substreams connection should be 'final-blocks-only'")
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
