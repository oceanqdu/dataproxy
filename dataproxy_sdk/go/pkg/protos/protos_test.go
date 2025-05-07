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
	"os"
	"testing"
)

func TestGetDPConfigValueFromEnv(t *testing.T) {
	config := &DataProxyConfig{}
	GetDPConfigValueFromEnv(config)
	if config.DataProxyAddr != "datamesh:8071" {
		t.Error("DataProxyConfig is nil")
	}

	addr := "aaaaa"
	cert := "bbbbb"
	key := "ccccc"
	ca := "ddddd"
	if err := os.Setenv("KUSCIA_DATA_MESH_ADDR", addr); err != nil {
		t.Fatal("设置失败:", err)
	}
	if err := os.Setenv("CLIENT_CERT_FILE", cert); err != nil {
		t.Fatal("设置失败:", err)
	}
	if err := os.Setenv("CLIENT_PRIVATE_KEY_FILE", key); err != nil {
		t.Fatal("设置失败:", err)
	}
	if err := os.Setenv("TRUSTED_CA_FILE", ca); err != nil {
		t.Fatal("设置失败:", err)
	}
	config.DataProxyAddr = "AAAAAA"
	GetDPConfigValueFromEnv(config)
	if config.DataProxyAddr != addr {
		t.Errorf("mismatch\nOriginal: %v\nTarget: %v", config.DataProxyAddr, addr)
	}
	if config.TlsConfig == nil {
		t.Fatalf("TlsConfig is nil")
	}
	if config.TlsConfig.CertificatePath != cert {
		t.Errorf("mismatch\nOriginal: %v\nTarget: %v", config.TlsConfig.CertificatePath, cert)
	}
	if config.TlsConfig.PrivateKeyPath != key {
		t.Errorf("mismatch\nOriginal: %v\nTarget: %v", config.TlsConfig.PrivateKeyPath, key)
	}
	if config.TlsConfig.CaFilePath != ca {
		t.Errorf("mismatch\nOriginal: %v\nTarget: %v", config.TlsConfig.CaFilePath, ca)
	}
}

func TestCheckUploadInfo(t *testing.T) {
	info := &UploadInfo{}
	if err := CheckUploadInfo(info); err == nil {
		t.Error("CheckUploadInfo mistake")
	}

	info.Type = "table"
	if err := CheckUploadInfo(info); err == nil {
		t.Error("CheckUploadInfo mistake")
	}
}

func TestBuildUploadAny(t *testing.T) {
	info := &UploadInfo{}

	_, err := BuildUploadAny(info, 6)
	if err == nil {
		t.Error("BuildUploadAny mistake")
	}

	_, err = BuildUploadAny(info, FileFormat_CSV)
	if err != nil {
		t.Error("BuildUploadAny mistake")
	}
}

func TestBuildDownloadAny(t *testing.T) {
	info := &DownloadInfo{}

	_, err := BuildDownloadAny(info, 6)
	if err == nil {
		t.Error("BuildUploadAny mistake")
	}

	_, err = BuildDownloadAny(info, FileFormat_CSV)
	if err != nil {
		t.Error("BuildUploadAny mistake")
	}
}

func TestBuildSqlAny(t *testing.T) {
	info := &SqlInfo{}

	if _, err := BuildSqlAny(info); err != nil {
		t.Error("BuildUploadAny mistake")
	}
}

func TestBuildActionCreateDomainDataRequest(t *testing.T) {
	info := &UploadInfo{}

	if _, err := BuildActionCreateDomainDataRequest(info, 6); err == nil {
		t.Error("BuildUploadAny mistake")
	}

	if _, err := BuildActionCreateDomainDataRequest(info, FileFormat_BINARY); err != nil {
		t.Error("BuildUploadAny mistake")
	}
}

func TestGetDomaindataIdFromResponse(t *testing.T) {
	if _, err := GetDomaindataIdFromResponse(nil); err == nil {
		t.Error("BuildUploadAny mistake")
	}
}
