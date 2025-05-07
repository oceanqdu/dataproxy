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
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/secretflow/dataproxy/dataproxy_sdk/go/pkg/files"
	"github.com/secretflow/dataproxy/dataproxy_sdk/go/pkg/flightcli"
	"github.com/secretflow/dataproxy/dataproxy_sdk/go/pkg/protos"
	"github.com/secretflow/dataproxy/dataproxy_sdk/go/pkg/utils"
	"github.com/secretflow/kuscia/pkg/utils/nlog"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type DataProxyClient interface {
	Upload(info *protos.UploadInfo, filePath string, fileFormat protos.FileFormat) error
	Download(info *protos.DownloadInfo, filePath string, fileFormat protos.FileFormat) error
	RunSql(info *protos.SqlInfo) (DataProxyStreamReader, error)
	GetWriter(info *protos.UploadInfo) (DataProxyStreamWriter, error)
	GetReader(info *protos.DownloadInfo) (DataProxyStreamReader, error)
	Close()
}

type dataproxyClient struct {
	client *flightcli.FlightClient
}

func NewDataProxyClient(config *protos.DataProxyConfig) (DataProxyClient, error) {
	var (
		nativeTLSConfig *tls.Config
		err             error
	)
	protos.GetDPConfigValueFromEnv(config)
	if config.TlsConfig != nil {
		if nativeTLSConfig, err = utils.BuildClientTLSConfigViaPath(config.TlsConfig.CaFilePath,
			config.TlsConfig.CertificatePath, config.TlsConfig.PrivateKeyPath); err != nil {
			return nil, err
		}
	}

	client, err := flightcli.NewFlightClient(config.DataProxyAddr, nativeTLSConfig)
	if err != nil {
		return nil, err
	}
	return &dataproxyClient{client}, nil
}

func (c *dataproxyClient) Close() {
	c.client.Close()
}

func (c *dataproxyClient) Upload(info *protos.UploadInfo, filePath string, fileFormat protos.FileFormat) error {
	nlog.Info("[dataproxy sdk] upload: ", filePath)

	writer, err := c.getWriter(info, fileFormat)
	if err != nil {
		return err
	}
	streamWriter := NewSimpleStreamWriter(writer, c, info)
	defer streamWriter.Close()

	file, err := files.NewReader(filePath, fileFormat, buildFileOption(writer.Schema()))
	if err != nil {
		return err
	}
	defer file.Close()

	nlog.Info("[dataproxy sdk] start upload: ", filePath)
	for {
		record, err := file.Read()
		if err != nil {
			return err
		}
		if record == nil {
			break
		}

		if err := streamWriter.Put(record); err != nil {
			return err
		}
	}

	nlog.Info("[dataproxy sdk] upload done: ", filePath)
	return nil
}

func buildFileOption(schema *arrow.Schema) *files.Options {
	options := files.DefaultOptions()
	columnNames := make([]string, 0, len(schema.Fields()))
	for _, field := range schema.Fields() {
		columnNames = append(columnNames, field.Name)
	}
	options.IncludeColumns = columnNames
	options.Schema = schema
	return options
}

func streamToFile(reader DataProxyStreamReader, index int, writer files.Writer) error {
	for {
		record, err := reader.GetByIndex(index)
		if err != nil {
			return err
		}
		if record == nil {
			break
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	return nil
}

func mergeFiles(baseFileName string, files []string, fileFormat protos.FileFormat) error {
	if len(files) == 0 {
		return nil
	}
	defer clearTmpFile(files)
	if fileFormat == protos.FileFormat_ORC {
		// TODO: 合并orc文件
		return nil
	} else {
		baseFile, err := os.OpenFile(baseFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return fmt.Errorf("open file: %s\nerror: %w", baseFileName, err)
		}
		defer baseFile.Close()
		for _, file := range files {
			f, err := os.Open(file)
			if err != nil {
				return fmt.Errorf("open file: %s\nerror: %w", file, err)
			}
			defer f.Close()
			_, err = io.Copy(baseFile, f)
			if err != nil {
				return fmt.Errorf("copy error: %w", err)
			}
		}
	}
	return nil
}

func clearTmpFile(files []string) {
	for _, file := range files {
		if err := os.Remove(file); err != nil {
			nlog.Warn("[dataproxy sdk] clear file: ", file, " err:", err)
		}
	}
}

func startDownload(reader DataProxyStreamReader, fileName string, fileFormat protos.FileFormat) ([]string, error) {
	var (
		wg          sync.WaitGroup
		errChan     = make(chan error, reader.GetSize())
		tmpFiles    = make([]string, 0, reader.GetSize())
		ctx, cancel = context.WithCancel(context.Background())
	)
	defer cancel()

	var downloadFile = func(index int, fileName string, withHeader bool) {
		options := files.DefaultOptions()
		options.WithHeader = withHeader
		writer, err := files.NewWriter(fileName, fileFormat, options)
		if err != nil {
			errChan <- err
			return
		}
		defer writer.Close()
		nlog.Info("[dataproxy sdk] start download: ", fileName)

		if err := streamToFile(reader, index, writer); err != nil {
			select {
			case <-ctx.Done():
			case errChan <- err:
				cancel()
			default:
				cancel()
			}
		}
		nlog.Info("[dataproxy sdk] download done: ", fileName)
	}

	for i := 1; i < reader.GetSize(); i++ {
		tmpFile := fmt.Sprintf("%s.%d", fileName, i)
		tmpFiles = append(tmpFiles, tmpFile)

		wg.Add(1)
		go func() {
			defer wg.Done()

			downloadFile(i, tmpFile, false)
		}()
	}

	downloadFile(0, fileName, true)

	go func() {
		wg.Wait()
		close(errChan)
	}()

	var errs []error
	for err := range errChan {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		clearTmpFile(tmpFiles)
		return nil, errors.Join(errs...)
	}

	return tmpFiles, nil
}

func (c *dataproxyClient) Download(info *protos.DownloadInfo, filePath string, fileFormat protos.FileFormat) error {
	nlog.Info("[dataproxy sdk] download: ", filePath)
	anyMsg, err := protos.BuildDownloadAny(info, fileFormat)
	if err != nil {
		return err
	}
	readers, err := c.getReader(anyMsg)
	if err != nil || len(readers) == 0 {
		return fmt.Errorf("get reader error: %v", err)
	}
	streamReader := NewParallelStreamReader(readers)
	tmpFiles, err := startDownload(streamReader, filePath, fileFormat)
	if err != nil {
		return err
	}
	nlog.Info("[dataproxy sdk] start merge: ", filePath, " ", tmpFiles)
	return mergeFiles(filePath, tmpFiles, fileFormat)
}

func (c *dataproxyClient) GetWriter(info *protos.UploadInfo) (DataProxyStreamWriter, error) {
	fileFormat := protos.FileFormat_CSV
	if info.Type != "table" {
		fileFormat = protos.FileFormat_BINARY
	}
	writer, err := c.getWriter(info, fileFormat)
	if err != nil {
		return nil, err
	}
	streamWriter := NewSimpleStreamWriter(writer, c, info)
	return streamWriter, nil
}

func (c *dataproxyClient) GetReader(info *protos.DownloadInfo) (DataProxyStreamReader, error) {
	anyMsg, err := protos.BuildDownloadAny(info, protos.FileFormat_CSV)
	if err != nil {
		return nil, err
	}
	readers, err := c.getReader(anyMsg)
	if err != nil {
		return nil, err
	}
	return NewSerialStreamReader(readers), nil
}

func (c *dataproxyClient) RunSql(info *protos.SqlInfo) (DataProxyStreamReader, error) {
	anyMsg, err := protos.BuildSqlAny(info)
	if err != nil {
		return nil, err
	}
	readers, err := c.getReader(anyMsg)
	if err != nil {
		return nil, err
	}
	return NewSerialStreamReader(readers), nil
}

func (c *dataproxyClient) createDomainData(info *protos.UploadInfo,
	fileFormat protos.FileFormat) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("createDomainData failed: %w", err)
		}
	}()

	createDomainDataReqMsg, err := protos.BuildActionCreateDomainDataRequest(info, fileFormat)
	if err != nil {
		return
	}
	createDomainDataReqData, err := proto.Marshal(createDomainDataReqMsg)
	if err != nil {
		return fmt.Errorf("marshal createDomainDataReqMsg failed: %w", err)
	}
	resp, err := c.client.DoAction("ActionCreateDomainDataRequest", createDomainDataReqData)
	if err != nil {
		return
	}
	domainDataID, _ := protos.GetDomaindataIdFromResponse(resp.Body)
	if domainDataID != info.DomaindataId {
		return fmt.Errorf("domain data ID mismatch: expect %s, got %s",
			info.DomaindataId, domainDataID)
	}
	return nil
}

func (c *dataproxyClient) deleteDomainData(info *protos.UploadInfo) error {
	deleteDomainDataReqMsg := protos.BuildActionDeleteDomainDataRequest(info)
	deleteDomainDataReqData, err := proto.Marshal(deleteDomainDataReqMsg)
	if err != nil {
		return fmt.Errorf("marshal deleteDomainDataReqMsg failed: %w", err)
	}
	_, err = c.client.DoAction("ActionDeleteDomainDataRequest", deleteDomainDataReqData)
	return err
}

func (c *dataproxyClient) buildWriterSchema(info *protos.UploadInfo) (*arrow.Schema, error) {
	fields := make([]arrow.Field, 0, len(info.Columns))
	for _, column := range info.Columns {
		dataType, err := utils.GetDataType(column.Type)
		if err != nil {
			return nil, err
		}
		fields = append(fields, arrow.Field{
			Name:     column.Name,
			Type:     dataType,
			Nullable: column.Nullable,
		})
	}
	return arrow.NewSchema(fields, nil), nil
}

func (c *dataproxyClient) getReader(anyMsg *anypb.Any) ([]*flightcli.FlightReader, error) {
	data, err := proto.Marshal(anyMsg)
	if err != nil {
		return nil, fmt.Errorf("getReader Marshal error: %w", err)
	}
	descriptor := &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  data,
	}
	return c.client.DoGet(descriptor)
}

func (c *dataproxyClient) getWriter(info *protos.UploadInfo, fileFormat protos.FileFormat) (*flightcli.FlightWriter, error) {
	if err := protos.CheckUploadInfo(info); err != nil {
		return nil, err
	}
	anyPb, err := protos.BuildUploadAny(info, fileFormat)
	if err != nil {
		return nil, err
	}
	anyData, err := proto.Marshal(anyPb)
	if err != nil {
		return nil, fmt.Errorf("marshal upload info failed: %w", err)
	}
	descriptor := &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  anyData,
	}
	schema, err := c.buildWriterSchema(info)
	if err != nil {
		return nil, err
	}
	err = c.createDomainData(info, fileFormat)
	if err != nil {
		return nil, err
	}
	readers, err := c.client.DoPut(descriptor, schema)
	if err != nil {
		return nil, err
	}
	return readers, nil
}
