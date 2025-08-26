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

package org.secretflow.dataproxy.plugin.hive.utils;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import org.secretflow.dataproxy.common.exceptions.DataproxyException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.secretflow.dataproxy.plugin.hive.utils.HiveUtil.buildMultiRowInsertSql;

@ExtendWith(MockitoExtension.class)
class HiveUtilTest {

//    @Test
//    void testBuildQuerySql() {
//        String tableName = "table";
//        List<String> fields = new ArrayList<>();
//        fields.add("col1");
//        fields.add("col2");
//        fields.add("col3");
//        String stmt = HiveUtil.buildQuerySql(tableName, fields, "region=us, date=2025-06-26");
//        assertEquals("select col1,col2,col3 from table where region='us' and date='2025-06-26'", stmt);
//    }

    @Test
    void testJDBCType2ArrowType() {
        assertEquals(Types.MinorType.INT.getType(), HiveUtil.jdbcType2ArrowType("int"));
        assertEquals(Types.MinorType.BIGINT.getType(), HiveUtil.jdbcType2ArrowType("bigint"));
        assertEquals(Types.MinorType.FLOAT4.getType(), HiveUtil.jdbcType2ArrowType("float"));
        assertEquals(Types.MinorType.FLOAT8.getType(), HiveUtil.jdbcType2ArrowType("double"));
        assertEquals(Types.MinorType.VARCHAR.getType(), HiveUtil.jdbcType2ArrowType("varchar"));
        assertEquals(Types.MinorType.BIT.getType(), HiveUtil.jdbcType2ArrowType("boolean"));
        assertEquals(Types.MinorType.DATEDAY.getType(), HiveUtil.jdbcType2ArrowType("date"));
        assertEquals(Types.MinorType.TIMESTAMPMILLI.getType(), HiveUtil.jdbcType2ArrowType("timestamp"));
    }

    @Test
    void testNormal() {
        List<Field> fields = Arrays.asList(
                Field.nullable("id", new ArrowType.Int(32, true)),
                Field.nullable("name", new ArrowType.Utf8()),
                Field.nullable("dt", new ArrowType.Utf8())
        );
        Schema schema = new Schema(fields);

        List<Map<String, Object>> data = List.of(
                Map.of("id", 1, "name", "Alice"),
                Map.of("id", 2, "name", "Bob")
        );
        Map<String, String> partition = Map.of("dt", "2024-06-01");

        DatabaseRecordWriter.SqlWithParams sp = buildMultiRowInsertSql("user", schema, data, partition);

        // SQL 模板校验
        assertEquals(
                "INSERT INTO TABLE user PARTITION (dt = ?) (id, name) VALUES (?, ?), (?, ?)",
                sp.sql
        );

        // Parameter validation: partition values + row values
        assertEquals(List.of("2024-06-01", 1, "Alice", 2, "Bob"), sp.params);
    }

    /**
     * Throws exception when no data is provided
     */
    @Test
    void testEmptyData() {
        Schema schema = new Schema(Collections.emptyList());
        Exception ex = assertThrows(
                IllegalArgumentException.class,
                () -> buildMultiRowInsertSql("t", schema, Collections.emptyList(), Collections.emptyMap())
        );
        assertEquals("No data to insert", ex.getMessage());
    }

    /**
     * Throws exception when table name is invalid
     */
    @Test
    void testInvalidTableName() {
        Schema schema = new Schema(List.of(Field.nullable("a", new ArrowType.Int(32, true))));
        Exception ex = assertThrows(
                DataproxyException.class,
                () -> buildMultiRowInsertSql("user;--", schema,
                        List.of(Map.of("a", 1)), Collections.emptyMap())
        );
        assertTrue(ex.getMessage().contains("Invalid tableName"));
    }

    /**
     * Throws exception when field name is invalid
     */
    @Test
    void testInvalidFieldName() {
        List<Field> fields = List.of(Field.nullable("a b", new ArrowType.Int(32, true)));
        Schema schema = new Schema(fields);
        Exception ex = assertThrows(
                DataproxyException.class,
                () -> buildMultiRowInsertSql("t", schema,
                        List.of(Map.of("a b", 1)), Collections.emptyMap())
        );
        assertTrue(ex.getMessage().contains("Invalid field name"));
    }

    /**
     * Throws exception when partition key is invalid
     */
    @Test
    void testInvalidPartitionKey() {
        Schema schema = new Schema(List.of(Field.nullable("a", new ArrowType.Int(32, true))));
        Exception ex = assertThrows(
                DataproxyException.class,
                () -> buildMultiRowInsertSql("t", schema,
                        List.of(Map.of("a", 1)), Map.of("a b", "v"))
        );
        assertTrue(ex.getMessage().contains("Invalid partition key"));
    }

}
