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
	"fmt"
	"os"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/secretflow/dataproxy/dataproxy_sdk/go/pkg/files"
	"github.com/secretflow/dataproxy/dataproxy_sdk/go/pkg/protos"
	"github.com/secretflow/dataproxy/dataproxy_sdk/go/pkg/utils"
	"github.com/stretchr/testify/suite"
)

type dataproxyTestBase struct {
	server  flight.Server
	client  DataProxyClient
	record  arrow.Record
	testDir string
}

func (b *dataproxyTestBase) setup(s *suite.Suite, mock *utils.MockFlightServer) {
	testDir, err := os.MkdirTemp("", "")
	s.Require().NoError(err, "创建临时目录失败")
	b.testDir = testDir

	// 初始化Flight服务器
	b.server = flight.NewServerWithMiddleware(nil)
	b.server.Init("localhost:0")

	mock.Address = b.server.Addr().String()
	b.server.RegisterFlightService(mock)
	go b.server.Serve()

	config := &protos.DataProxyConfig{
		DataProxyAddr: b.server.Addr().String(),
	}
	b.client, err = NewDataProxyClient(config)
	s.Require().NoError(err, "客户端初始化失败")

	b.record = utils.MakeTestRecord()
}

func (b *dataproxyTestBase) release() {
	if b.client != nil {
		b.client.Close()
	}

	if b.server != nil {
		b.server.Shutdown()
	}

	b.record.Release()
	os.RemoveAll(b.testDir)
}

func (b *dataproxyTestBase) testUploadAndDownload(s *suite.Suite) {
	testFile := b.testDir + "test.csv"
	file, err := files.NewCsvWriter(testFile, files.DefaultOptions())
	s.Require().NoError(err, "创建CSV文件失败")
	defer file.Close()
	err = file.Write(b.record)
	s.Require().NoError(err, "写入文件失败")

	var columns []*protos.DataColumn
	for _, field := range b.record.Schema().Fields() {
		columns = append(columns, &protos.DataColumn{
			Name: field.Name,
			Type: field.Type.Name(),
		})
	}
	uploadInfo := &protos.UploadInfo{
		Type:    "table",
		Columns: columns,
	}

	err = b.client.Upload(uploadInfo, testFile, protos.FileFormat_CSV)
	s.Require().NoError(err, "上传文件失败")

	resultFile := b.testDir + "result.csv"
	err = b.client.Download(&protos.DownloadInfo{}, resultFile, protos.FileFormat_CSV)
	s.Require().NoError(err, "下载文件失败")

	s.Require().NoError(checkDownloadFile(resultFile, b.record))
}

func checkDownloadFile(fileName string, record arrow.Record) error {
	readOptions := files.DefaultOptions()
	readOptions.ChunkSize = int(record.NumRows())
	fileReader, err := files.NewCsvReader(fileName, readOptions)
	if err != nil {
		return err
	}
	defer fileReader.Close()
	for {
		result, err := fileReader.Read()
		if err != nil {
			return err
		}
		if result == nil {
			break
		}
		if !array.RecordEqual(result, record) {
			return fmt.Errorf("download file not equal to upload file")
		}
	}
	return nil
}

func (b *dataproxyTestBase) uploadTestData(s *suite.Suite) {
	var columns []*protos.DataColumn
	for _, field := range b.record.Schema().Fields() {
		columns = append(columns, &protos.DataColumn{
			Name: field.Name,
			Type: field.Type.Name(),
		})
	}

	// 执行上传操作
	writer, err := b.client.GetWriter(&protos.UploadInfo{
		Type:    "table",
		Columns: columns,
	})
	s.Require().NoError(err, "获取Writer失败")
	defer writer.Close()

	s.Require().NoError(writer.Put(b.record), "数据写入失败")
}

func (b *dataproxyTestBase) testWriteAndRead(s *suite.Suite) {
	b.uploadTestData(s)

	// 执行下载验证
	reader, err := b.client.GetReader(&protos.DownloadInfo{})
	s.Require().NoError(err, "获取Reader失败")
	defer reader.Close()

	result, err := reader.Get()
	s.Require().NoError(err, "数据读取失败")
	if !array.RecordEqual(b.record, result) {
		errMsg := fmt.Sprintf("Records mismatch\nOriginal: %v\nRead: %v", b.record, result)
		s.FailNow(errMsg)
	}
}

func (b *dataproxyTestBase) testRunSql(s *suite.Suite) {
	b.uploadTestData(s)

	// 执行SQL查询
	reader, err := b.client.RunSql(&protos.SqlInfo{})
	s.Require().NoError(err, "执行SQL失败")
	defer reader.Close()

	result, err := reader.Get()
	s.Require().NoError(err, "获取结果失败")
	if !array.RecordEqual(b.record, result) {
		errMsg := fmt.Sprintf("Records mismatch\nOriginal: %v\nRead: %v", b.record, result)
		s.FailNow(errMsg)
	}
}

type DataProxyTestSuite struct {
	suite.Suite
	dataproxyTestBase
}

// 初始化测试套件
func (s *DataProxyTestSuite) SetupTest() {
	s.setup(&s.Suite, &utils.MockFlightServer{})
}

func (s *DataProxyTestSuite) TearDownTest() {
	s.release()
}

func (s *DataProxyTestSuite) TestUploadAndDownload() {
	s.testUploadAndDownload(&s.Suite)
}

func (s *DataProxyTestSuite) TestWriteAndRead() {
	s.testWriteAndRead(&s.Suite)
}

func (s *DataProxyTestSuite) TestRunSql() {
	s.testRunSql(&s.Suite)

}

func TestDataProxySuite(t *testing.T) {
	suite.Run(t, new(DataProxyTestSuite))
}

type DataProxyTestWithDpSuite struct {
	suite.Suite
	dataproxyTestBase
}

// 初始化测试套件
func (s *DataProxyTestWithDpSuite) SetupTest() {
	s.setup(&s.Suite, &utils.MockFlightServer{DpMum: 3})
}

func (s *DataProxyTestWithDpSuite) TearDownTest() {
	s.release()
}

func TestDataProxyTestWithDpSuite(t *testing.T) {
	suite.Run(t, new(DataProxyTestWithDpSuite))
}

func (s *DataProxyTestWithDpSuite) TestUploadAndDownload() {
	s.testUploadAndDownload(&s.Suite)
}

func (s *DataProxyTestWithDpSuite) TestWriteAndRead() {
	s.testWriteAndRead(&s.Suite)
}

func (s *DataProxyTestWithDpSuite) TestRunSql() {
	s.testRunSql(&s.Suite)
}
