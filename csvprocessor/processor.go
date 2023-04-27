package csvprocessor

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/streamingfast/dstore"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	pbentity "github.com/streamingfast/substreams-sink-graphcsv/pb/entity/v1"
	"github.com/streamingfast/substreams-sink-graphcsv/schema"
	"go.uber.org/zap"
)

type Processor struct {
	*shutter.Shutter

	inputStore dstore.Store
	csvOutput  *WriterManager

	entityDesc *schema.EntityDesc

	entities map[string]*Entity

	stopBlock  uint64
	bundleSize uint64

	logger *zap.Logger
	tracer logging.Tracer
}

func New(
	srcFolder string,
	destFolder string,
	entity string,
	stopBlock uint64,
	bundleSize uint64,
	schemaFilename string,
	logger *zap.Logger,
	tracer logging.Tracer) (*Processor, error) {
	p := &Processor{
		Shutter:    shutter.New(),
		entities:   make(map[string]*Entity),
		stopBlock:  stopBlock,
		bundleSize: bundleSize,
		logger:     logger,
		tracer:     tracer,
	}

	inputStore, err := dstore.NewJSONLStore(filepath.Join(srcFolder, entity))
	if err != nil {
		return nil, err
	}

	outputStore, err := dstore.NewStore(filepath.Join(destFolder, entity), "csv", "none", false)
	if err != nil {
		return nil, err
	}

	p.inputStore = inputStore

	p.csvOutput = NewWriterManager(bundleSize, outputStore)

	entities, err := schema.GetEntitiesFromSchema(schemaFilename)
	if err != nil {
		return nil, err
	}

	for _, ent := range entities {
		if ent.Name == entity {
			p.entityDesc = ent
			break
		}
	}
	if p.entityDesc == nil {
		return nil, fmt.Errorf("cannot find entity %q in schema %q", entity, schemaFilename)
	}

	return p, nil
}

func (p *Processor) Run(ctx context.Context) {
	p.Shutdown(p.run(ctx))
}

func (p *Processor) run(ctx context.Context) error {

	entitiesToLoad := []string{}
	var endRange uint64
	p.logger.Info("retrieving relevant entity files")
	fileCount := 0
	err := p.inputStore.Walk(context.Background(), "", func(filename string) (err error) {
		fileCount++
		startBlockNum, endBlockNum, err := getBlockRange(filename)
		if err != nil {
			return fmt.Errorf("fail reading block range in %q: %w", filename, err)
		}

		if p.stopBlock != 0 && startBlockNum >= p.stopBlock {
			return dstore.StopIteration
		}

		if endRange == 0 {
			endRange = endBlockNum
		} else {
			if startBlockNum != (endRange + 1) {
				return fmt.Errorf("broken file contiguity at %q (previous range end was %d)", filename, endRange)
			}
			endRange = endBlockNum
		}

		entitiesToLoad = append(entitiesToLoad, filename)

		return nil
	})
	if err != nil {
		return fmt.Errorf("unable to walk entity files: %w", err)
	}

	p.logger.Info("found entities file to export",
		zap.Int("entity_file_seen_count", fileCount),
		zap.Int("entity_file_to_load", len(entitiesToLoad)),
	)

	for idx, filename := range entitiesToLoad {
		if err := p.processEntityFile(ctx, filename); err != nil {
			return fmt.Errorf("error processing file: %w", err)
		}

		if idx%10 == 0 {
			p.logger.Info("entity file completed (1/10)",
				zap.String("filename", filename),
				//				zap.Uint64("block_count", ts.metrics.blockCount),
				//				zap.Uint64("entity_count", ts.metrics.entityCount),
				zap.Int("file_count", idx),
			)
		}

	}
	if endRange > p.stopBlock {
		endRange = p.stopBlock
	}

	// ensure we create the last file
	if err := p.csvOutput.Roll(ctx, endRange); err != nil {
		return err
	}
	p.csvOutput.current.Close()

	return nil
}

func (p *Processor) processEntityFile(ctx context.Context, filename string) error {
	//ts.metrics.fileCount++
	p.logger.Debug("processing entity file", zap.String("filename", filename))

	reader, err := p.inputStore.OpenObject(ctx, filename)
	if err != nil {
		return fmt.Errorf("unable to load entitis file %q: %w", filename, err)
	}
	bufReader := bufio.NewReader(reader)

	for {
		//	ts.metrics.blockCount++
		line, err := bufReader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("unable to read newline: %w", err)
		}

		ch := &EntityChangeAtBlockNum{}
		if err := json.Unmarshal(line, ch); err != nil {
			return err
		}

		if p.stopBlock != 0 && ch.BlockNum > p.stopBlock {
			p.logger.Info("passed stopBlock", zap.Uint64("change block_num", ch.BlockNum), zap.Uint64("stop_block", p.stopBlock))
			if err := p.csvOutput.Close(); err != nil {
				return err
			}
			return nil
		}

		if err := p.csvOutput.Roll(ctx, ch.BlockNum); err != nil {
			return err
		}

		newEnt, err := newEntity(ch, p.entityDesc)
		if err != nil {
			return err
		}

		prev, found := p.entities[ch.EntityChange.ID]

		switch ch.EntityChange.Operation {
		case pbentity.EntityChange_CREATE:
			if found {
				return fmt.Errorf("got CREATE on entity %q but it already exists", ch.EntityChange.ID)
			}

			if err := newEnt.ValidateFields(p.entityDesc); err != nil {
				return fmt.Errorf("during CREATE: %w", err)
			}

			if p.entityDesc.Immutable {
				if err := p.csvOutput.Write(newEnt, p.entityDesc, 0); err != nil {
					return err
				}
				continue
			}
			p.entities[ch.EntityChange.ID] = newEnt

		case pbentity.EntityChange_UPDATE:
			if p.entityDesc.Immutable {
				if err := newEnt.ValidateFields(p.entityDesc); err != nil {
					return fmt.Errorf("during UPDATE to an immutable entity: %w", err)
				}
				if err := p.csvOutput.Write(newEnt, p.entityDesc, 0); err != nil {
					return err
				}
				continue
				// FIXME: enforce this at some point
				// return fmt.Errorf("entity %q got updated but should be immutable", ch.EntityChange.ID)
			}
			if !found {
				if err := newEnt.ValidateFields(p.entityDesc); err != nil {
					return fmt.Errorf("during UPDATE to an unseen entity: %w", err)
				}
				p.entities[ch.EntityChange.ID] = newEnt
				continue
				// FIXME: enforce this at some point
				//return fmt.Errorf("entity %q got updated but previous value not found", ch.EntityChange.ID)
			}
			if err := prev.ValidateFields(p.entityDesc); err != nil {
				return fmt.Errorf("during UPDATE to an existing entity: %w", err)
			}
			if err := p.csvOutput.Write(prev, p.entityDesc, ch.BlockNum); err != nil {
				return err
			}
			prev.Update(newEnt)
			p.entities[ch.EntityChange.ID] = prev

		case pbentity.EntityChange_DELETE:
			if p.entityDesc.Immutable {
				return fmt.Errorf("entity %q got deleted but should be immutable", ch.EntityChange.ID)
			}
			if !found {
				return fmt.Errorf("entity %q got updated but previous value not found", ch.EntityChange.ID)
			}

			if err := p.csvOutput.Write(prev, p.entityDesc, ch.BlockNum); err != nil {
				return err
			}
			delete(p.entities, ch.EntityChange.ID)

		case pbentity.EntityChange_FINAL:
			if p.entityDesc.Immutable {
				continue
			}

			if err := p.csvOutput.Write(prev, p.entityDesc, 0); err != nil {
				return err
			}
			delete(p.entities, ch.EntityChange.ID)
		}

	}
	//	if ts.metrics.shouldPurge() {
	//		for id, ent := range ts.entities {
	//			if purgeableEntity, ok := ent.(entity.Finalizable); ok {
	//				if purgeableEntity.IsFinal(currentBlock.BlockNum, currentBlock.BlockTimestamp) {
	//					if ent != nil {
	//						if err := ts.writeEntity(currentBlock.BlockNum, ent); err != nil {
	//							return fmt.Errorf("write csv encoded: %w", err)
	//						}
	//					}
	//					delete(ts.entities, id)
	//				}
	//			} else {
	//				break
	//			}
	//		}
	//	}
	//	if ts.metrics.showProgress() {
	//		zlog.Info("entities progress",
	//			zap.Uint64("last_block_num", currentBlock.BlockNum),
	//			zap.String("table_name", ts.tableName),
	//			zap.Uint64("entity_count", ts.metrics.entityCount),
	//			zap.Duration("elasped_time", time.Since(ts.metrics.startedAt)),
	//			zap.Int("entities_map_size", len(ts.entities)),
	//			zap.String("table_name", ts.tableName),
	//		)
	//	}
	//}
	return nil
}

func getBlockRange(filename string) (uint64, uint64, error) {
	match := blockRangeRegex.FindStringSubmatch(filename)
	if match == nil {
		return 0, 0, fmt.Errorf("no block range in filename: %s", filename)
	}

	startBlock, _ := strconv.ParseUint(match[1], 10, 64)
	stopBlock, _ := strconv.ParseUint(match[2], 10, 64)
	return startBlock, stopBlock, nil
}

var blockRangeRegex = regexp.MustCompile(`(\d{10})-(\d{10})`)
