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

package flightcli

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/secretflow/dataproxy/dataproxy_sdk/go/pkg/utils"
)

func TestPutAndGet(t *testing.T) {
	client, _ := setupTestServer(t, 0)

	record := utils.MakeTestRecord()

	testDoAction(t, client, record)

	testDoPut(t, client, record)

	testDoGet(t, client, record)
}

func TestPutAndGetWithDp(t *testing.T) {
	client, _ := setupTestServer(t, 4)

	record := utils.MakeTestRecord()

	testDoAction(t, client, record)

	testDoPut(t, client, record)

	testDoGet(t, client, record)
}

func setupTestServer(t *testing.T, dpNum int) (*FlightClient, flight.Server) {
	s := flight.NewServerWithMiddleware(nil)
	s.Init("localhost:0")
	f := &utils.MockFlightServer{DpMum: dpNum, Address: s.Addr().String()}
	s.RegisterFlightService(f)

	go s.Serve()
	t.Cleanup(s.Shutdown)

	client, err := NewFlightClient(s.Addr().String(), nil)
	if err != nil {
		t.Fatalf("Client initialization failed: %v", err)
	}
	t.Cleanup(client.Close)

	return client, s
}

func testDoPut(t *testing.T, client *FlightClient, record arrow.Record) {
	t.Run("DoPut", func(t *testing.T) {
		writer, err := client.DoPut(&flight.FlightDescriptor{}, record.Schema())
		if err != nil {
			t.Fatalf("DoPut failed: %v", err)
		}
		defer writer.Close()

		if err := writer.Write(record); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	})
}

func testDoGet(t *testing.T, client *FlightClient, record arrow.Record) {
	t.Run("DoGet", func(t *testing.T) {
		getResults, err := client.DoGet(&flight.FlightDescriptor{})
		if err != nil {
			t.Fatalf("DoGet failed: %v", err)
		}

		for _, result := range getResults {
			tmp, err := result.Reader.Read()
			if err != nil {
				t.Fatalf("Read failed: %v", err)
			}
			t.Cleanup(result.Close)

			if !array.RecordEqual(record, tmp) {
				t.Error("Records are not equal")
			}
		}
	})
}

func testDoAction(t *testing.T, client *FlightClient, record arrow.Record) {
	t.Run("DoAction", func(t *testing.T) {
		getResults, err := client.DoAction("test", nil)
		if err != nil {
			t.Fatalf("DoAction failed: %v", err)
		}
		if getResults.Body != nil {
			t.Fatalf("Mock DoAction should not return a body")
		}
	})
}
