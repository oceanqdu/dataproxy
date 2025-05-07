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

package files

import (
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/secretflow/dataproxy/dataproxy_sdk/go/pkg/protos"
)

type Options struct {
	Schema         *arrow.Schema
	WithHeader     bool
	ChunkSize      int
	Comma          rune
	IncludeColumns []string
	ColumnTypes    map[string]arrow.DataType
}

func DefaultOptions() *Options {
	return &Options{
		Schema:     nil,
		WithHeader: true,
		ChunkSize:  1 << 20,
		Comma:      ',',
	}
}

type Reader interface {
	Read() (arrow.Record, error)
	Schema() *arrow.Schema
	Close() error
}

func NewReader(fileName string, fileFormat protos.FileFormat, opt *Options) (Reader, error) {
	switch fileFormat {
	case protos.FileFormat_CSV:
		return NewCsvReader(fileName, opt)
	default:
		return nil, unsupportedFormatError(fileFormat)
	}
}

type Writer interface {
	Write(arrow.Record) error
	Close() error
}

func NewWriter(fileName string, fileFormat protos.FileFormat, opt *Options) (Writer, error) {
	switch fileFormat {
	case protos.FileFormat_CSV:
		return NewCsvWriter(fileName, opt)
	default:
		return nil, unsupportedFormatError(fileFormat)
	}
}

func unsupportedFormatError(fileFormat protos.FileFormat) error {
	return fmt.Errorf("file unsupported file format: %d", fileFormat)
}
