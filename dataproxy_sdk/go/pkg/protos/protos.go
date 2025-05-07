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

package protos

import (
	"fmt"
	"os"

	"github.com/secretflow/kuscia/proto/api/v1alpha1"
	"github.com/secretflow/kuscia/proto/api/v1alpha1/datamesh"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type FileFormat int

const (
	FileFormat_UNKNOWN FileFormat = 0
	FileFormat_CSV     FileFormat = 1
	FileFormat_BINARY  FileFormat = 2
	FileFormat_ORC     FileFormat = 3
)

type TlsConfig struct {
	CertificatePath string
	PrivateKeyPath  string
	CaFilePath      string
}

type DataProxyConfig struct {
	DataProxyAddr string
	*TlsConfig
}

type CompressionType int

const (
	CompressionType_UNCOMPRESSED CompressionType = 0
	CompressionType_SNAPPY       CompressionType = 1
	CompressionType_GZIP         CompressionType = 2
	CompressionType_BROTLI       CompressionType = 3
	CompressionType_ZSTD         CompressionType = 4
	CompressionType_LZ4          CompressionType = 5
	CompressionType_LZ4_FRAME    CompressionType = 6
	CompressionType_LZO          CompressionType = 7
	CompressionType_BZ2          CompressionType = 8
	CompressionType_LZ4_HADOOP   CompressionType = 9
)

type OrcFileInfo struct {
	Compression          CompressionType
	CompressionBlockSize int64
	StripeSize           int64
}

type DownloadInfo struct {
	DomaindataId  string
	PartitionSpec string
	*OrcFileInfo
}

type SqlInfo struct {
	DatasourceId string
	Sql          string
}

type DataColumn struct {
	Name     string
	Type     string
	Comment  string
	Nullable bool
}

type UploadInfo struct {
	DomaindataId string
	Name         string
	Type         string
	RelativeUri  string
	DatasourceId string
	Attributes   map[string]string
	Columns      []*DataColumn
	Vendor       string
}

func formatToContentType(format FileFormat) (datamesh.ContentType, error) {
	switch format {
	case FileFormat_BINARY:
		return datamesh.ContentType_RAW, nil
	case FileFormat_CSV, FileFormat_ORC:
		return datamesh.ContentType_CSV, nil
	default:
		return datamesh.ContentType(0), fmt.Errorf("unsupported file format: %v", format)
	}
}

func changeToKusciaFileFormat(fileFormat FileFormat) (v1alpha1.FileFormat, error) {
	switch fileFormat {
	case FileFormat_BINARY:
		return v1alpha1.FileFormat_BINARY, nil
	case FileFormat_CSV, FileFormat_ORC:
		return v1alpha1.FileFormat_CSV, nil
	default:
		return v1alpha1.FileFormat_UNKNOWN, fmt.Errorf("unsupported file format: %v", fileFormat)
	}
}

func CheckUploadInfo(info *UploadInfo) error {
	validTypes := map[string]struct{}{
		"table":         {},
		"model":         {},
		"rule":          {},
		"serving_model": {},
	}

	if _, exists := validTypes[info.Type]; !exists {
		return fmt.Errorf("unsupported type: %q", info.Type)
	}

	if info.Type == "table" && len(info.Columns) == 0 {
		return fmt.Errorf("table type requires at least one column")
	}
	return nil
}

func BuildUploadAny(info *UploadInfo, fileFormat FileFormat) (*anypb.Any, error) {
	contentType, err := formatToContentType(fileFormat)
	if err != nil {
		return nil, err
	}
	msg := &datamesh.CommandDomainDataUpdate{
		DomaindataId: info.DomaindataId,
		ContentType:  contentType,
	}

	if fileFormat != FileFormat_BINARY {
		msg.FileWriteOptions = &datamesh.FileWriteOptions{
			Options: &datamesh.FileWriteOptions_CsvOptions{
				CsvOptions: &datamesh.CSVWriteOptions{FieldDelimiter: ","},
			},
		}
	}
	var anyPb anypb.Any
	if err := anyPb.MarshalFrom(msg); err != nil {
		return nil, fmt.Errorf("BuildUploadAny MarshalFrom error: %v", err)
	}
	return &anyPb, nil
}

func BuildDownloadAny(info *DownloadInfo, fileFormat FileFormat) (*anypb.Any, error) {
	contentType, err := formatToContentType(fileFormat)
	if err != nil {
		return nil, err
	}
	msg := &datamesh.CommandDomainDataQuery{
		DomaindataId:  info.DomaindataId,
		PartitionSpec: info.PartitionSpec,
		ContentType:   contentType,
	}
	var anyMsg anypb.Any
	if err := anyMsg.MarshalFrom(msg); err != nil {
		return nil, fmt.Errorf("BuildDownloadAny MarshalFrom error: %v", err)
	}
	return &anyMsg, nil
}

func BuildSqlAny(info *SqlInfo) (*anypb.Any, error) {
	msg := &datamesh.CommandDataSourceSqlQuery{
		DatasourceId: info.DatasourceId,
		Sql:          info.Sql,
	}
	var anyMsg anypb.Any
	if err := anyMsg.MarshalFrom(msg); err != nil {
		return nil, fmt.Errorf("BuildSqlAny MarshalFrom error: %v", err)
	}
	return &anyMsg, nil
}

func BuildActionCreateDomainDataRequest(info *UploadInfo, fileFormat FileFormat) (*datamesh.CreateDomainDataRequest, error) {
	ff, err := changeToKusciaFileFormat(fileFormat)
	if err != nil {
		return nil, err
	}
	cols := make([]*v1alpha1.DataColumn, 0, len(info.Columns))
	for _, col := range info.Columns {
		cols = append(cols, &v1alpha1.DataColumn{
			Name:        col.Name,
			Type:        col.Type,
			Comment:     col.Comment,
			NotNullable: col.Nullable,
		})
	}
	createDomainDataReq := &datamesh.CreateDomainDataRequest{
		DomaindataId: info.DomaindataId,
		Name:         info.Name,
		Type:         info.Type,
		RelativeUri:  info.RelativeUri,
		DatasourceId: info.DatasourceId,
		Attributes:   info.Attributes,
		Columns:      cols,
		Vendor:       info.Vendor,
		FileFormat:   ff,
	}
	return createDomainDataReq, nil
}

func GetDomaindataIdFromResponse(data []byte) (string, error) {
	var resp datamesh.CreateDomainDataResponse
	if err := proto.Unmarshal(data, &resp); err != nil {
		return "", fmt.Errorf("GetDomaindataIdFromResponse Unmarshal error: %v", err)
	}
	respData := resp.GetData()
	if respData == nil {
		return "", fmt.Errorf("response data is nil")
	}
	return respData.GetDomaindataId(), nil
}

func BuildActionDeleteDomainDataRequest(info *UploadInfo) *datamesh.DeleteDomainDataRequest {
	return &datamesh.DeleteDomainDataRequest{
		DomaindataId: info.DomaindataId,
	}
}

func GetDPConfigValueFromEnv(config *DataProxyConfig) {
	if addr, exists := os.LookupEnv("KUSCIA_DATA_MESH_ADDR"); exists {
		config.DataProxyAddr = addr
	}

	if config.DataProxyAddr == "" {
		config.DataProxyAddr = "datamesh:8071"
	}

	certFile, certOk := os.LookupEnv("CLIENT_CERT_FILE")
	keyFile, keyOk := os.LookupEnv("CLIENT_PRIVATE_KEY_FILE")
	caFile, caOk := os.LookupEnv("TRUSTED_CA_FILE")

	if !(certOk && keyOk && caOk) {
		return
	}

	config.TlsConfig = &TlsConfig{
		CertificatePath: certFile,
		PrivateKeyPath:  keyFile,
		CaFilePath:      caFile,
	}
}
