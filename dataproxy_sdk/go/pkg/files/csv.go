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
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/csv"
	"github.com/secretflow/kuscia/pkg/utils/nlog"
)

type csvReader struct {
	*csv.Reader
	file *os.File
}

func NewCsvReader(fileName string, opt *Options) (Reader, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("open file:%s\nerror: %w", fileName, err)
	}
	var opts []csv.Option
	opts = append(opts, csv.WithComma(opt.Comma))
	opts = append(opts, csv.WithChunk(opt.ChunkSize))
	opts = append(opts, csv.WithHeader(opt.WithHeader))

	var reader *csv.Reader
	if opt.Schema != nil {
		reader = csv.NewReader(file, opt.Schema, opts...)
	} else {
		if len(opt.IncludeColumns) > 0 {
			opts = append(opts, csv.WithIncludeColumns(opt.IncludeColumns))
		}
		if len(opt.ColumnTypes) > 0 {
			opts = append(opts, csv.WithColumnTypes(opt.ColumnTypes))
		}
		reader = csv.NewInferringReader(file, opts...)
	}
	return &csvReader{Reader: reader, file: file}, nil
}

func (r *csvReader) Read() (arrow.Record, error) {
	if r.Next() {
		return r.Record(), nil
	}

	if err := r.Err(); err == nil || errors.Is(err, io.EOF) {
		return nil, nil
	}
	return nil, fmt.Errorf("read csv data error: %w", r.Err())
}

func (r *csvReader) Close() error {
	r.Release()
	return r.file.Close()
}

type csvWriter struct {
	w    *csv.Writer
	file *os.File
	opts *Options
}

func NewCsvWriter(fileName string, opts *Options) (Writer, error) {
	file, err := os.Create(fileName)
	if err != nil {
		return nil, fmt.Errorf("create file: %s\nerror: %w", fileName, err)
	}

	return &csvWriter{w: nil, file: file, opts: opts}, nil
}

func (w *csvWriter) Write(record arrow.Record) error {
	if w.w == nil {
		w.w = csv.NewWriter(w.file, record.Schema(), csv.WithHeader(w.opts.WithHeader))
	}
	if err := w.w.Write(record); err != nil {
		return fmt.Errorf("Write csv data error: %w", err)
	}
	return nil
}

func (w *csvWriter) Close() error {
	if w.w != nil {
		if err := w.w.Flush(); err != nil {
			nlog.Warnf("[dataproxy sdk] Write csv flush data error: %v", err)
		}
	}
	return w.file.Close()
}
