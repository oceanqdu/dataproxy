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

package utils

import (
	"context"
	"fmt"
	"sync"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/flight"
	pb "github.com/apache/arrow/go/v17/arrow/flight/gen/flight"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/secretflow/kuscia/pkg/utils/nlog"
)

func MakeTestRecord() arrow.Record {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name,1", Type: arrow.BinaryTypes.String},
		{Name: "man", Type: arrow.FixedWidthTypes.Boolean},
	}, nil)
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), arrow.NewSchema(schema.Fields(), nil))
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "b", "c"}, nil)
	builder.Field(2).(*array.BooleanBuilder).AppendValues([]bool{true, false, true}, nil)
	return builder.NewRecord()
}

type MockFlightServer struct {
	DpMum   int
	Address string
	schema  *arrow.Schema
	records []arrow.Record
	lock    sync.Mutex
	flight.BaseFlightServer
}

func (m *MockFlightServer) GetFlightInfo(ctx context.Context, request *flight.FlightDescriptor) (*flight.FlightInfo,
	error) {
	var endpoints []*flight.FlightEndpoint
	if m.DpMum > 0 {
		for i := 0; i < m.DpMum; i++ {
			endpoints = append(endpoints, &flight.FlightEndpoint{
				Ticket: &flight.Ticket{},
				Location: []*flight.Location{
					{
						Uri: m.Address,
					},
				},
			})
		}
	} else {
		endpoints = append(endpoints, &flight.FlightEndpoint{
			Ticket: &flight.Ticket{},
			Location: []*flight.Location{
				{
					Uri: "kuscia://datamesh",
				},
			},
		})
	}
	info := &flight.FlightInfo{
		Endpoint: endpoints,
	}

	return info, nil
}

func (m *MockFlightServer) DoPut(stream flight.FlightService_DoPutServer) error {
	rdr, err := flight.NewRecordReader(stream)
	if err != nil {
		return err
	}
	defer rdr.Release()

	m.lock.Lock()
	defer m.lock.Unlock()

	m.records = make([]arrow.Record, 0)
	for rdr.Next() {
		record := rdr.Record()
		if record == nil {
			break
		}
		m.records = append(m.records, record.NewSlice(0, record.NumRows()))
	}

	nlog.Info("[dataproxy sdk] MockFlightServer DoPut success")
	return stream.Send(&flight.PutResult{})
}

func (m *MockFlightServer) DoGet(request *flight.Ticket, stream flight.FlightService_DoGetServer) (err error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if len(m.records) == 0 {
		return fmt.Errorf("MockFlightServer no data %p", &m)
	}

	wr := flight.NewRecordWriter(stream, ipc.WithSchema(m.records[0].Schema()))
	defer wr.Close()

	for _, record := range m.records {
		if err := wr.Write(record); err != nil {
			return fmt.Errorf("MockFlightServer DoGet: %w", err)
		}
	}

	nlog.Info("[dataproxy sdk] MockFlightServer DoGet success")
	return nil
}

func (f *MockFlightServer) DoAction(cmd *flight.Action, stream flight.FlightService_DoActionServer) error {
	return stream.Send(&pb.Result{})
}
