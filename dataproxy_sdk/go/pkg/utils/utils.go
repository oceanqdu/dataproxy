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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/apache/arrow/go/v17/arrow"
)

func loadFile(fileName string) ([]byte, error) {
	content, err := os.ReadFile(fileName)
	if err != nil {
		return nil, fmt.Errorf("error reading %v certificate, %v", fileName, err.Error())
	}
	return content, nil
}

// BuildTLSCertificateViaPath builds tls certificate.
func BuildTLSCertificateViaPath(certPath, keyPath string) ([]tls.Certificate, error) {
	certPEMBlock, err := loadFile(certPath)
	if err != nil {
		return nil, err
	}

	keyPEMBlock, err := loadFile(keyPath)
	if err != nil {
		return nil, err
	}

	certs := make([]tls.Certificate, 1)
	certs[0], err = tls.X509KeyPair(certPEMBlock, keyPEMBlock)
	if err != nil {
		return nil, fmt.Errorf("X509KeyPair err: %v", err)
	}
	return certs, nil
}

func BuildClientTLSConfigViaPath(caPath, certPath, keyPath string) (*tls.Config, error) {
	if caPath == "" || certPath == "" || keyPath == "" {
		return nil, fmt.Errorf("load client tls config failed, ca|clientcert|clientkey path can't be empty")
	}

	caCertFile, err := loadFile(caPath)
	if err != nil {
		return nil, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCertFile)

	certs, err := BuildTLSCertificateViaPath(certPath, keyPath)
	if err != nil {
		return nil, err
	}
	config := &tls.Config{
		RootCAs:      caCertPool,
		Certificates: certs,
	}
	return config, nil
}

func GetDataType(typeStr string) (arrow.DataType, error) {
	switch typeStr {
	case "bool":
		return arrow.FixedWidthTypes.Boolean, nil
	case "int8":
		return arrow.PrimitiveTypes.Int8, nil
	case "int16":
		return arrow.PrimitiveTypes.Int16, nil
	case "int32":
		return arrow.PrimitiveTypes.Int32, nil
	case "int64", "int":
		return arrow.PrimitiveTypes.Int64, nil
	case "uint8":
		return arrow.PrimitiveTypes.Uint8, nil
	case "uint16":
		return arrow.PrimitiveTypes.Uint16, nil
	case "uint32":
		return arrow.PrimitiveTypes.Uint32, nil
	case "uint64":
		return arrow.PrimitiveTypes.Uint64, nil
	case "float32":
		return arrow.PrimitiveTypes.Float32, nil
	case "float64", "float":
		return arrow.PrimitiveTypes.Float64, nil
	case "string", "str", "utf8":
		return arrow.BinaryTypes.String, nil
	default:
		return nil, fmt.Errorf("GetDataType unsupported type: %s", typeStr)
	}
}
