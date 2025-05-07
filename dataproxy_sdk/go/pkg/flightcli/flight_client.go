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
	"context"
	"crypto/tls"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/secretflow/kuscia/pkg/utils/nlog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type FlightWriter struct {
	*flight.Writer
	schema *arrow.Schema
	stream flight.FlightService_DoPutClient
}

func (w *FlightWriter) Schema() *arrow.Schema {
	return w.schema
}

func (w *FlightWriter) Close() error {
	if err := w.Writer.Close(); err != nil {
		nlog.Warn("[dataproxy sdk] close flight writer failed: ", err)
	}
	return w.stream.CloseSend()
}

type FlightReader struct {
	*flight.Reader
	client flight.Client
}

func (r *FlightReader) Close() {
	r.Reader.Release()

	if r.client != nil {
		if err := r.client.Close(); err != nil {
			nlog.Warn("[dataproxy sdk] FlightReader close flight client failed: ", err)
		}
	}
}

type FlightClient struct {
	client flight.Client
}

func NewFlightClient(address string, tlsConfig *tls.Config) (*FlightClient, error) {
	creds := insecure.NewCredentials()
	if tlsConfig != nil {
		creds = credentials.NewTLS(tlsConfig)
	}

	client, err := flight.NewClientWithMiddleware(
		address,
		nil,
		nil,
		grpc.WithTransportCredentials(creds),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create flight client: %w", err)
	}

	return &FlightClient{client: client}, nil
}

func (c *FlightClient) Close() {
	if c.client != nil {
		c.client.Close()
	}
}

type flightEndpoint struct {
	Ticket   *flight.Ticket
	DpClient flight.Client
}

func (c *FlightClient) getFlightEndpoints(desc *flight.FlightDescriptor) ([]flightEndpoint, error) {
	if desc == nil {
		return nil, fmt.Errorf("flight descriptor cannot be nil")
	}
	flightInfo, err := c.client.GetFlightInfo(context.Background(), desc)
	if err != nil {
		return nil, fmt.Errorf("get flight info failed: %w", err)
	}
	if len(flightInfo.Endpoint) == 0 {
		return nil, fmt.Errorf("no endpoints available")
	}
	var endpoints []flightEndpoint
	for _, ep := range flightInfo.Endpoint {
		if len(ep.Location) == 0 {
			return nil, fmt.Errorf("empty endpoint location")
		}

		client, err := c.createEndpointClient(ep.Location[0].Uri)
		if err != nil {
			return nil, err
		}

		endpoints = append(endpoints, flightEndpoint{
			Ticket:   ep.Ticket,
			DpClient: client,
		})
	}

	return endpoints, nil
}

func (c *FlightClient) createEndpointClient(uri string) (flight.Client, error) {
	if strings.HasPrefix(uri, "kuscia://") {
		return c.client, nil
	}

	client, err := flight.NewClientWithMiddleware(
		uri,
		nil,
		nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("NewClientWithMiddleware err: %w", err)
	}
	return client, nil
}

func (c *FlightClient) DoGet(desc *flight.FlightDescriptor) ([]*FlightReader, error) {
	endpoints, err := c.getFlightEndpoints(desc)
	if err != nil {
		return nil, err
	}
	var readers []*FlightReader
	for _, ep := range endpoints {
		stream, err := ep.DpClient.DoGet(context.Background(), ep.Ticket)
		if err != nil {
			return nil, fmt.Errorf("doGet failed: %w", err)
		}

		reader, err := flight.NewRecordReader(stream)
		if err != nil {
			return nil, fmt.Errorf("create record reader failed: %w", err)
		}

		client := ep.DpClient
		if client == c.client {
			client = nil // Avoid closing parent client
		}

		readers = append(readers, &FlightReader{
			Reader: reader,
			client: client,
		})
	}
	if len(readers) == 0 {
		return nil, fmt.Errorf("DoGet no readers created")
	}
	if err := validateSchemas(readers); err != nil {
		return nil, err
	}

	return readers, nil
}

func validateSchemas(readers []*FlightReader) error {
	baseSchema := readers[0].Reader.Schema()
	for _, r := range readers[1:] {
		if !baseSchema.Equal(r.Reader.Schema()) {
			return fmt.Errorf("schema mismatch between readers")
		}
	}
	return nil
}

func (c *FlightClient) DoPut(desc *flight.FlightDescriptor, schema *arrow.Schema) (*FlightWriter, error) {
	endpoints, err := c.getFlightEndpoints(desc)
	if err != nil {
		return nil, err
	}

	stream, err := endpoints[0].DpClient.DoPut(context.Background())
	if err != nil {
		return nil, fmt.Errorf("doPut failed: %w", err)
	}

	if err := stream.SendMsg(&flight.Ticket{
		Ticket: endpoints[0].Ticket.GetTicket(),
	}); err != nil {
		return nil, fmt.Errorf("ticket send failed: %w", err)
	}

	return &FlightWriter{
		Writer: flight.NewRecordWriter(stream, ipc.WithSchema(schema)),
		schema: schema,
		stream: stream,
	}, nil
}

type ActionResult struct {
	Body []byte
}

func (c *FlightClient) DoAction(actionType string, actionBody []byte) (*ActionResult, error) {
	action := &flight.Action{
		Type: actionType,
		Body: actionBody,
	}
	stream, err := c.client.DoAction(context.Background(), action)
	if err != nil {
		return nil, fmt.Errorf("FlightClient doAction failed: %w", err)
	}
	err = stream.CloseSend()
	if err != nil {
		return nil, fmt.Errorf("FlightClient doAction failed: %w", err)
	}
	result, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("FlightClient doAction failed: %w", err)
	}
	return &ActionResult{
		Body: result.GetBody(),
	}, nil
}
