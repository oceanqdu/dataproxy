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
	"github.com/apache/arrow/go/v17/arrow/util"
	"github.com/secretflow/dataproxy/dataproxy_sdk/go/pkg/flightcli"
	"github.com/secretflow/dataproxy/dataproxy_sdk/go/pkg/protos"
	"github.com/secretflow/kuscia/pkg/utils/nlog"
)

type DataProxyStreamWriter interface {
	Put(record arrow.Record) error
	Close() error
}

type simpleStreamWriter struct {
	writer     *flightcli.FlightWriter
	owner      *dataproxyClient
	uploadInfo *protos.UploadInfo
}

func NewSimpleStreamWriter(writer *flightcli.FlightWriter, owner *dataproxyClient, info *protos.UploadInfo) DataProxyStreamWriter {
	return &simpleStreamWriter{writer, owner, info}
}

const kMaxBatchSize = 4 * 1024 * 1024

func (c *simpleStreamWriter) Put(record arrow.Record) error {
	recordSize := util.TotalRecordSize(record)

	if recordSize <= kMaxBatchSize {
		return c.writeWithErrorHandling(record)
	}

	totalRows := record.NumRows()
	rowsPerSlice := max(1, totalRows/((recordSize+kMaxBatchSize-1)/kMaxBatchSize))

	for offset := int64(0); offset < totalRows; {
		sliceRows := min(offset+rowsPerSlice, totalRows)
		slice := record.NewSlice(offset, sliceRows)
		defer slice.Release()
		if err := c.writeWithErrorHandling(slice); err != nil {
			return err
		}
		offset += rowsPerSlice
	}

	return nil
}

func (c *simpleStreamWriter) writeWithErrorHandling(slice arrow.Record) error {
	if err := c.writer.Write(slice); err != nil {
		if c.owner != nil {
			if delErr := c.owner.deleteDomainData(c.uploadInfo); delErr != nil {
				nlog.Warn("[dataproxy sdk] delete domain data failed: ", delErr)
			}
		}

		if !errors.Is(err, io.EOF) {
			return fmt.Errorf("write record failed: %w", err)
		}
	}
	return nil
}

func (c *simpleStreamWriter) Close() error {
	return c.writer.Close()
}
