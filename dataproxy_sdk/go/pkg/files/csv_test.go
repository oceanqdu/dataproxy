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
	"os"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/secretflow/dataproxy/dataproxy_sdk/go/pkg/utils"
)

func TestCsv(t *testing.T) {
	record := utils.MakeTestRecord()
	filePath := "test.csv"
	writer, err := NewCsvWriter(filePath, DefaultOptions())
	if err != nil {
		t.Fatalf("NewCsvWriter failed")
	}
	defer writer.Close()
	defer func() {
		os.Remove(filePath)
	}()

	if err := writer.Write(record); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	reader, err := NewCsvReader(filePath, DefaultOptions())
	if err != nil {
		t.Fatalf("NewCsvReader failed: %v", err)
	}
	defer reader.Close()

	record2, err := reader.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !array.RecordEqual(record, record2) {
		t.Fatalf("Records mismatch\nOriginal: %v\nRead: %v", record, record2)
	}
}
