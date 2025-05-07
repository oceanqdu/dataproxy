// Copyright 2025 Ant Group Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dataproxy

import (
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/secretflow/dataproxy/dataproxy_sdk/go/pkg/flightcli"
)

type DataProxyStreamReader interface {
	Get() (arrow.Record, error)
	GetByIndex(i int) (arrow.Record, error)
	Schema() *arrow.Schema
	GetSize() int
	Close()
}

type baseStreamReader struct {
	readers []*flightcli.FlightReader
}

func (b *baseStreamReader) GetSize() int {
	return len(b.readers)
}

func (b *baseStreamReader) Close() {
	for _, r := range b.readers {
		r.Close()
	}
}

func (b *baseStreamReader) Schema() *arrow.Schema {
	if len(b.readers) == 0 {
		return nil
	}
	return b.readers[0].Schema()
}

type serialStreamReader struct {
	baseStreamReader
	index int
}

func NewSerialStreamReader(readers []*flightcli.FlightReader) DataProxyStreamReader {
	return &serialStreamReader{
		baseStreamReader: baseStreamReader{readers: readers},
	}
}

func (c *serialStreamReader) Get() (arrow.Record, error) {
	for c.index < len(c.readers) {
		switch {
		case c.readers[c.index].Next():
			return c.readers[c.index].Record(), nil
		default:
			c.index++
		}
	}
	return nil, nil
}

func (c *serialStreamReader) GetByIndex(i int) (arrow.Record, error) {
	return nil, fmt.Errorf("GetByIndex not support")
}

type parallelStreamReader struct {
	baseStreamReader
}

func NewParallelStreamReader(readers []*flightcli.FlightReader) DataProxyStreamReader {
	return &parallelStreamReader{
		baseStreamReader: baseStreamReader{readers: readers},
	}
}

func (c *parallelStreamReader) Get() (arrow.Record, error) {
	return nil, fmt.Errorf("Get not support")
}

func (c *parallelStreamReader) GetByIndex(i int) (arrow.Record, error) {
	if i < 0 || i >= len(c.readers) {
		return nil, fmt.Errorf("GetByIndex index out of range")
	}

	record, err := c.readers[i].Read()
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("GetByIndex error: %w", err)
	}
	return record, nil
}
