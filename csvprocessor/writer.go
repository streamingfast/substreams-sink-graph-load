package csvprocessor

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/substreams-sink-graphcsv/schema"
)

type WriterManager struct {
	current    *Writer
	stopBlock  uint64
	bundleSize uint64
	store      dstore.Store
}

func NewWriterManager(bundleSize uint64, store dstore.Store) *WriterManager {
	return &WriterManager{
		bundleSize: bundleSize,
		store:      store,
	}
}

func (wm *WriterManager) setNewWriter(ctx context.Context, blockNum uint64) error {
	r, err := bstream.NewRangeContaining(blockNum, wm.bundleSize)
	if err != nil {
		return err
	}

	writer, err := NewWriter(ctx, wm.store, fileNameFromRange(r))
	if err != nil {
		return err
	}

	wm.current = writer
	return nil
}

func (wm *WriterManager) Roll(ctx context.Context, blockNum uint64) error {
	if wm.current == nil {
		return wm.setNewWriter(ctx, blockNum)
	}
	if blockNum > wm.stopBlock {

		if err := wm.current.Close(); err != nil {
			return err
		}
		return wm.setNewWriter(ctx, blockNum)
	}
	return nil
}

func (wm *WriterManager) Close() error {
	return wm.current.Close()
}

func (wm *WriterManager) Write(e *Entity, desc *schema.EntityDesc, stopBlock uint64) error {
	return wm.current.Write(e, desc, stopBlock)
}

type Writer struct {
	writer    *io.PipeWriter
	done      chan struct{}
	csvWriter *csv.Writer
	filename  string
	StopBlock uint64
}

func NewWriter(ctx context.Context, store dstore.Store, filename string) (*Writer, error) {
	reader, writer := io.Pipe()
	csvWriter := csv.NewWriter(writer)

	ce := &Writer{
		filename:  filename,
		csvWriter: csvWriter,
		writer:    writer,
		done:      make(chan struct{}),
	}

	go func() {
		err := store.WriteObject(ctx, filename, reader)
		if err != nil {
			// todo: better handle error
			panic(fmt.Errorf("failed writting object in file object %q: %w", filename, err))
		}
		close(ce.done)
	}()

	return ce, nil
}

func (c *Writer) Write(e *Entity, desc *schema.EntityDesc, stopBlock uint64) error {
	records := []string{
		formatField(e.Fields["id"], schema.FieldTypeID),
		blockRange(e.StartBlock, stopBlock),
	}

	for _, f := range desc.OrderedFields() {
		records = append(records, formatField(e.Fields[f.Name], f.Type))
	}

	if err := c.csvWriter.Write(records); err != nil {
		return err
	}
	return nil
}

func formatField(f interface{}, t schema.FieldType) string {
	switch t {
	case schema.FieldTypeID, schema.FieldTypeString:
		return fmt.Sprintf("%s", f)
	case schema.FieldTypeBytes:
		return fmt.Sprintf("%s", f)
	case schema.FieldTypeBigInt:
		return fmt.Sprintf("%s", f)
	case schema.FieldTypeBigDecimal:
		return fmt.Sprintf("%s", f)
	case schema.FieldTypeInt:
		return fmt.Sprintf("%d", f)
	case schema.FieldTypeFloat:
		return fmt.Sprintf("%f", f)
	case schema.FieldTypeBoolean:
		return fmt.Sprintf("%t", f)
	default:
		panic(fmt.Errorf("invalid field type: %q", t))
	}
}

func (c *Writer) Close() error {
	c.csvWriter.Flush()
	if err := c.csvWriter.Error(); err != nil {
		return fmt.Errorf("error flushing csv encoder: %w", err)
	}

	if err := c.writer.Close(); err != nil {
		return fmt.Errorf("closing csv writer: %w", err)
	}
	<-c.done
	return nil
}

func fileNameFromRange(r *bstream.Range) string {
	return fmt.Sprintf("%d-%d", r.StartBlock(), *r.EndBlock()-1) // endBlock should always be set in those ranges
}
