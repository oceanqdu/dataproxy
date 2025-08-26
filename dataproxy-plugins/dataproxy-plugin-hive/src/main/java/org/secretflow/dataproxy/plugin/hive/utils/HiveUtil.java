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

import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.plugin.database.config.DatabaseConnectConfig;
import org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter.parsePartition;

@Slf4j
public class HiveUtil {

    public static Connection initHive(DatabaseConnectConfig config) {
        String endpoint = config.endpoint();
        String ip;
        int port = 10000; // default port

        if (endpoint.contains(":")) {
            String[] parts = endpoint.split(":");
            ip = parts[0];
            if (parts.length > 1 && !parts[1].isEmpty()) {
                port = Integer.parseInt(parts[1]);
            }
        } else {
            ip = endpoint;
        }

        // Validate IP address/hostname to prevent JDBC URL injection
        if (ip == null || !ip.matches("^[a-zA-Z0-9._-]+$") || ip.contains("..") || ip.startsWith(".") || ip.endsWith(".")) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE,
                "Invalid IP address or hostname: " + ip);
        }

        // Validate database name to prevent JDBC URL injection
        String database = config.database();
        if (database == null || !database.matches("^[a-zA-Z0-9_]+$")) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE,
                "Invalid database name: " + database);
        }

        Connection conn;
        try{
            // hive Authentication None
            if(!config.username().isEmpty() && !config.password().isEmpty()) {
                conn = DriverManager.getConnection(String.format("jdbc:hive2://%s:%s/%s", ip, port, database), config.username(), config.password());
            } else {
                conn = DriverManager.getConnection(String.format("jdbc:hive2://%s:%s/%s", ip, port, database));
            }
        } catch (Exception e) {
            log.error("database init error \"{}\"", e.getMessage());
            throw new RuntimeException(e);
        }
        return conn;

    }

    public static String buildQuerySql(String tableName, List<String> fields, String whereClause) {
        final Pattern columnOrValuePattern = Pattern.compile("^[a-zA-Z0-9_]+$");

        // Validate table name
        if (!columnOrValuePattern.matcher(tableName).matches()) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid tableName:" + tableName);
        }

        // Validate field names
        for (String field : fields) {
            if (!columnOrValuePattern.matcher(field).matches()) {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid field name:" + field);
            }
        }

        // Process where clause
        String processedWhereClause = "";
        if (!whereClause.isEmpty()) {
            String[] groups = whereClause.split("[,/]");
            if (groups.length > 1) {
                final Map<String, String> partitionSpec = parsePartition(whereClause);

                for (Map.Entry<String, String> entry : partitionSpec.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();

                    // Validate partition key name
                    if (!columnOrValuePattern.matcher(key).matches()) {
                        throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid partition key:" + key);
                    }

                    // Validate partition value - only allow letters, numbers, underscores, hyphens, and dots
                    if (!value.matches("^[a-zA-Z0-9_.-]+$")) {
                        throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid partition value:" + value);
                    }
                }

                List<String> list = partitionSpec.keySet().stream()
                        .map(k -> k + "='" + escapeString(partitionSpec.get(k)) + "'")
                        .toList();
                processedWhereClause = String.join(" and ", list);
            } else {
                // For simple where conditions, use stricter validation or disallow
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE,
                    "Invalid where clause format. Use partition format like 'key=value,key2=value2'");
            }
        }

        String sql =  "select " + String.join(",", fields) + " from " + tableName +
                      (processedWhereClause.isEmpty() ? "" : " where " + processedWhereClause);
        log.info("buildQuerySql sql:{}", sql);
        return sql;
    }

    public static String buildCreateTableSql(String tableName, Schema schema, Map<String, String> partition) {
        final Pattern identifierPattern = Pattern.compile("^[a-zA-Z0-9_]+$");

        // Validate table name
        if (!identifierPattern.matcher(tableName).matches()) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid tableName:" + tableName);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(tableName).append(" (\n");

        List<Field> fields = schema.getFields();
        Set<String> partitionKeys = partition.keySet(); // Partition field names collection

        // Used for quick field lookup by name
        Map<String, Field> fieldMap = new LinkedHashMap<>();
        for (Field field : fields) {
            fieldMap.put(field.getName(), field);
        }

        // Validate all field names
        for (Field field : fields) {
            String fieldName = field.getName();
            if (!identifierPattern.matcher(fieldName).matches()) {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid field name:" + fieldName);
            }
        }

        // Validate partition key names
        for (String partKey : partitionKeys) {
            if (!identifierPattern.matcher(partKey).matches()) {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid partition key:" + partKey);
            }
        }

        // Table fields (excluding partition fields)
        boolean first = true;
        for (Field field : fields) {
            String fieldName = field.getName();
            if (partitionKeys.contains(fieldName)) {
                continue; // Skip partition fields
            }

            if (!first) {
                sb.append(",\n");
            }
            sb.append("  ").append(fieldName)
                    .append(" ")
                    .append(arrowTypeToJdbcType(field.getType()));
            first = false;
        }
        sb.append("\n)");

        // Partition fields (require types, types from schema)
        if (!partitionKeys.isEmpty()) {
            sb.append("\nPARTITIONED BY (\n");
            first = true;
            for (String partKey : partitionKeys) {
                Field partitionField = fieldMap.get(partKey);
                if (partitionField == null) {
                    log.error("Partition column '{}' not found in schema", partKey);
                    throw new IllegalArgumentException("Partition column '" + partKey + "' not found in schema");
                }

                if (!first) {
                    sb.append(",\n");
                }
                sb.append("  ").append(partKey)
                        .append(" ")
                        .append(arrowTypeToJdbcType(partitionField.getType()));
                first = false;
            }
            sb.append("\n)");
        }

        log.info("buildCreateTableSql sql:{}", sb);
        return sb.toString();
    }

    public static DatabaseRecordWriter.SqlWithParams buildMultiRowInsertSql(String tableName,
                                                                            Schema schema,
                                                                            List<Map<String, Object>> dataList,
                                                                            Map<String, String> partition
    ) {
        final Pattern identifierPattern = Pattern.compile("^[a-zA-Z0-9_]+$");

        if (dataList == null || dataList.isEmpty()) {
            throw new IllegalArgumentException("No data to insert");
        }

        if (!identifierPattern.matcher(tableName).matches()) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid tableName:" + tableName);
        }
        for (Field f : schema.getFields()) {
            if (!identifierPattern.matcher(f.getName()).matches()) {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid field name:" + f.getName());
            }
        }
        for (String k : partition.keySet()) {
            if (!identifierPattern.matcher(k).matches()) {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid partition key:" + k);
            }
        }

        Set<String> partitionKeys = partition.keySet();
        List<String> columns = schema.getFields().stream()
                .map(Field::getName)
                .filter(n -> !partitionKeys.contains(n))
                .collect(Collectors.toList());

        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO TABLE ").append(tableName);

        if (!partition.isEmpty()) {
            List<String> partitionPlaceholders = partition.keySet()
                    .stream()
                    .map(k -> k + " = ?")
                    .collect(Collectors.toList());
            sb.append(" PARTITION (").append(String.join(", ", partitionPlaceholders)).append(")");
        }

        List<String> colPlaceholders = columns.stream()
                .map(c -> "?")
                .collect(Collectors.toList());
        sb.append(" (").append(String.join(", ", columns)).append(") VALUES ");

        String singleRow = "(" + String.join(", ", colPlaceholders) + ")";
        List<String> allRows = Collections.nCopies(dataList.size(), singleRow);
        sb.append(String.join(", ", allRows));

        List<Object> params = new ArrayList<>();

        for (String k : partition.keySet()) {
            params.add(partition.get(k));
        }

        for (Map<String, Object> row : dataList) {
            for (String col : columns) {
                params.add(row.get(col));
            }
        }

        return new DatabaseRecordWriter.SqlWithParams(sb.toString(), params);
    }

    private static String escapeString(String str) {
        return str.replace("'", "''");
    }

    public static ArrowType jdbcType2ArrowType(String jdbcType) {
        if (jdbcType == null || jdbcType.isEmpty()) {
            throw new IllegalArgumentException("Hive type is null or empty");
        }

        String type = jdbcType.trim().toLowerCase();

        // Handle decimal(p,s)
        if (type.startsWith("decimal")) {
            Pattern pattern = Pattern.compile("decimal\\((\\d+),(\\d+)\\)");
            Matcher matcher = pattern.matcher(type);
            if (matcher.find()) {
                int precision = Integer.parseInt(matcher.group(1));
                int scale = Integer.parseInt(matcher.group(2));
                return new ArrowType.Decimal(precision, scale, 128);
            } else {
                return new ArrowType.Decimal(38, 10, 128); // Default precision
            }
        }

        return switch (type) {
            case "tinyint" -> new ArrowType.Int(8, true);
            case "smallint" -> new ArrowType.Int(16, true);
            case "int", "integer" -> new ArrowType.Int(32, true);
            case "bigint" -> new ArrowType.Int(64, true);
            case "float" -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case "double" -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case "string", "varchar", "char" -> new ArrowType.Utf8();
            case "boolean" -> new ArrowType.Bool();
            case "date" -> new ArrowType.Date(DateUnit.DAY);
            case "timestamp" -> new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            case "binary" -> new ArrowType.Binary();

            default -> throw new IllegalArgumentException("Unsupported Hive type: " + jdbcType);
        };
    }

    public static String arrowTypeToJdbcType(ArrowType arrowType) {
        if (arrowType instanceof ArrowType.Utf8) {
            return "STRING";
        } else if (arrowType instanceof ArrowType.Int intType) {
            int bitWidth = intType.getBitWidth();
            boolean signed = intType.getIsSigned();
            return switch (bitWidth) {
                case 8 -> signed ? "TINYINT" : "TINYINT UNSIGNED";
                case 16 -> signed ? "SMALLINT" : "SMALLINT UNSIGNED";
                case 32 -> signed ? "INT" : "INT UNSIGNED";
                case 64 -> signed ? "BIGINT" : "BIGINT UNSIGNED";
                default -> throw new IllegalArgumentException("Unsupported Int bitWidth: " + bitWidth);
            };
        } else if (arrowType instanceof ArrowType.FloatingPoint fp) {
            return switch (fp.getPrecision()) {
                case SINGLE -> "FLOAT";
                case DOUBLE -> "DOUBLE";
                default -> throw new IllegalArgumentException("Unsupported floating point type");
            };
        } else if (arrowType instanceof ArrowType.Bool) {
            return "BOOLEAN";
        } else if (arrowType instanceof ArrowType.Date) {
            return "DATE";
        } else if (arrowType instanceof ArrowType.Time) {
            return "TIME";
        } else if (arrowType instanceof ArrowType.Timestamp) {
            return "TIMESTAMP";
        } else if (arrowType instanceof ArrowType.Decimal dec) {
            return "DECIMAL(" + dec.getPrecision() + ", " + dec.getScale() + ")";
        } else if (arrowType instanceof ArrowType.Binary || arrowType instanceof ArrowType.FixedSizeBinary) {
            return "BINARY";
        } else {
            throw new IllegalArgumentException("Unsupported Arrow type: " + arrowType.getClass());
        }
    }

    public static boolean checkTableExists(Connection connection, String tableName) {
        final Pattern identifierPattern = Pattern.compile("^[a-zA-Z0-9_]+$");

        // Validate table name
        if (!identifierPattern.matcher(tableName).matches()) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid tableName:" + tableName);
        }

        ResultSet rs = null;
        Statement stmt = null;
        try {
            // Hive doesn't support parameterized SHOW TABLES query, use string concatenation but validate table name first
            stmt = connection.createStatement();
            rs = stmt.executeQuery("SHOW TABLES LIKE '" + tableName + "'");
            return rs.next();
        } catch (SQLException e) {
            log.error("check whether table has existed error: {}", e.getMessage());
            throw new RuntimeException(e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                log.error("close result or statement error: {}", e.getMessage());
            }
        }
    }

}
