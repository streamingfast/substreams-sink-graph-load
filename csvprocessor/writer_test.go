package csvprocessor

import (
	"testing"

	"github.com/streamingfast/substreams-sink-graphcsv/schema"
	"github.com/stretchr/testify/assert"
)

func TestFormatField(t *testing.T) {
	var in interface{}
	in = "hFgqh8ZmyJrv2UhHF3t/r0l20y8PBf2mK+yFdQAAAAA="
	expected := "84582a87c666c89aefd94847177b7faf4976d32f0f05fda62bec857500000000"
	got := formatField(
		in,
		schema.FieldTypeBytes,
		false,
		false,
	)
	assert.Equal(t, expected, got)
}
