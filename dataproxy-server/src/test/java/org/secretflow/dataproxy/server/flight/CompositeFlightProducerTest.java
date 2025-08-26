/*
 * Copyright 2025 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.secretflow.dataproxy.server.flight;

import com.google.protobuf.Any;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightProducer.CallContext;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.secretflow.dataproxy.core.param.ParamWrapper;
import org.secretflow.dataproxy.core.service.impl.CacheTicketService;
import org.secretflow.v1alpha1.kusciaapi.Domaindatasource;
import org.secretflow.v1alpha1.kusciaapi.Flightdm;
import org.secretflow.v1alpha1.kusciaapi.Flightinner;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author yuexie
 * @date 2025/6/9 14:45
 **/
@ExtendWith(MockitoExtension.class)
public class CompositeFlightProducerTest {

    @Mock
    private ProducerRegistry mockRegistry;

    @Mock
    private FlightProducer mockFlightProducer;

    private CompositeFlightProducer producer;

    @BeforeEach
    void setUp() {
        producer = new CompositeFlightProducer(mockRegistry);
    }

    @Test
    void getStream() {
        CallContext context = mock(CallContext.class);
        Ticket ticket = new Ticket("test".getBytes());
        ServerStreamListener listener = mock(ServerStreamListener.class);

        when(mockRegistry.getOrDefaultNoOp(anyString())).thenReturn(mockFlightProducer);

        producer.getStream(context, ticket, listener);

        verify(mockFlightProducer).getStream(context, ticket, listener);
    }

    @Test
    void getStreamWithNullTicket() {
        CallContext context = mock(CallContext.class);
        ServerStreamListener listener = mock(ServerStreamListener.class);

        assertThrows(NullPointerException.class, () -> producer.getStream(context, null, listener));
    }

    @Test
    void listFlights() {
        CallContext context = mock(CallContext.class);
        Criteria criteria = mock(Criteria.class);
        FlightProducer.StreamListener<FlightInfo> listener = mock(FlightProducer.StreamListener.class);

        when(mockRegistry.getOrDefaultNoOp(anyString())).thenReturn(mockFlightProducer);

        producer.listFlights(context, criteria, listener);

        verify(mockFlightProducer).listFlights(context, criteria, listener);
    }

    @Test
    void listFlightsWithNullCriteria() {
        CallContext context = mock(CallContext.class);
        FlightProducer.StreamListener<FlightInfo> listener = mock(FlightProducer.StreamListener.class);

        assertThrows(NullPointerException.class, () -> producer.listFlights(context, null, listener));
    }


    @Test
    void getFlightInfoWithCommandDataMeshQuery() {
        assertDoesNotThrow(() -> returnFlightInfoFromProducer(Any.pack(mockCommandDataMeshQuery())));
    }

    @Test
    void getFlightInfoWithCommandDataMeshSqlQuery() {
        assertDoesNotThrow(() -> returnFlightInfoFromProducer(Any.pack(mockCommandDataMeshSqlQuery())));
    }

    @Test
    void getFlightInfoWithCommandDataMeshUpdate() {
        assertDoesNotThrow(() -> returnFlightInfoFromProducer(Any.pack(mockCommandDataMeshUpdate())));
    }

    @Test
    void getFlightInfoWithTicketDomainDataQuery() {
        assertDoesNotThrow(() -> {

            ParamWrapper paramWrapper = ParamWrapper.of("mock", null);

            byte[] mocks = CacheTicketService.getInstance().generateTicket(paramWrapper);

            Flightdm.TicketDomainDataQuery ticketDomainDataQuery = mockTicketDomainDataQuery(new String(mocks, StandardCharsets.UTF_8));

            paramWrapper.setParamIfAbsent(ticketDomainDataQuery);
            returnFlightInfoFromProducer(Any.pack(ticketDomainDataQuery));
        });
    }


    private void returnFlightInfoFromProducer(Any any) {
        FlightDescriptor descriptor = FlightDescriptor.command(any.toByteArray());
        CallContext context = mock(CallContext.class);
        FlightInfo expectedInfo = mock(FlightInfo.class);

        when(mockRegistry.getOrDefaultNoOp(anyString())).thenReturn(mockFlightProducer);
        when(mockFlightProducer.getFlightInfo(context, descriptor)).thenReturn(expectedInfo);

        FlightInfo result = producer.getFlightInfo(context, descriptor);

        assertEquals(expectedInfo, result);
        verify(mockFlightProducer).getFlightInfo(context, descriptor);
    }

    @Test
    void getSchemaWhenCommandDataMeshQuery() {
        CallContext context = mock(CallContext.class);

        FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(mockCommandDataMeshQuery()).toByteArray());

        SchemaResult mock = mock(SchemaResult.class);

        when(mockRegistry.getOrDefaultNoOp(anyString())).thenReturn(mockFlightProducer);
        when(mockFlightProducer.getSchema(context, descriptor)).thenReturn(mock);

        SchemaResult schema = producer.getSchema(context, descriptor);

        verify(mockFlightProducer).getSchema(context, descriptor);
        assertEquals(mock, schema);

    }

    @Test
    void shouldHandleExceptionWhenGetSchema() {

        CallContext context = mock(CallContext.class);
        FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(mockCommandDataMeshQuery()).toByteArray());

        SchemaResult mock = mock(SchemaResult.class);

        when(mockRegistry.getOrDefaultNoOp(anyString())).thenReturn(mockFlightProducer);
        when(mockFlightProducer.getSchema(context, descriptor)).thenReturn(mock);
        when(mockFlightProducer.getSchema(context, descriptor)).thenThrow(new RuntimeException("test error"));

        assertThrows(RuntimeException.class, () -> producer.getSchema(context, descriptor));
        verify(mockFlightProducer).getSchema(context, descriptor);
    }

    private Flightinner.CommandDataMeshQuery mockCommandDataMeshQuery() {
        return Flightinner.CommandDataMeshQuery.newBuilder()
                .setDatasource(Domaindatasource.DomainDataSource.newBuilder().setType("mock"))
                .build();
    }

    private Flightinner.CommandDataMeshUpdate mockCommandDataMeshUpdate() {
        return Flightinner.CommandDataMeshUpdate.newBuilder()
                .setDatasource(Domaindatasource.DomainDataSource.newBuilder().setType("mock"))
                .build();
    }

    private Flightinner.CommandDataMeshSqlQuery mockCommandDataMeshSqlQuery() {
        return Flightinner.CommandDataMeshSqlQuery.newBuilder()
                .setDatasource(Domaindatasource.DomainDataSource.newBuilder().setType("mock"))
                .build();
    }

    private Flightdm.TicketDomainDataQuery mockTicketDomainDataQuery(String domainDataHandle) {

        return Flightdm.TicketDomainDataQuery.newBuilder()
                .setDomaindataHandle(domainDataHandle)
                .build();
    }
}
