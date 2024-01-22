// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package com.starrocks.connector.flink.tools.cdc;

import com.starrocks.connector.flink.catalog.starrocks.DataModel;
import com.starrocks.connector.flink.catalog.starrocks.FieldSchema;
import com.starrocks.connector.flink.catalog.starrocks.TableSchema;
import com.starrocks.connector.flink.tools.cdc.mysql.MysqlType;
import org.apache.flink.util.StringUtils;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.StringJoiner;
import java.util.Map;

public class SourceSchema {
    private final String databaseName;
    private final String tableName;
    private final String tableComment;
    private final LinkedHashMap<String, FieldSchema> fields;
    public final List<String> primaryKeys;

    public SourceSchema(
            DatabaseMetaData metaData, String databaseName, String tableName, String tableComment)
            throws Exception {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.tableComment = tableComment;

        fields = new LinkedHashMap<>();
        try (ResultSet rs = metaData.getColumns(databaseName, null, tableName, null)) {
            while (rs.next()) {
                String fieldName = rs.getString("COLUMN_NAME");
                String comment = rs.getString("REMARKS");
                String fieldType = rs.getString("TYPE_NAME");
                Integer precision = rs.getInt("COLUMN_SIZE");

                if (rs.wasNull()) {
                    precision = null;
                }
                Integer scale = rs.getInt("DECIMAL_DIGITS");
                if (rs.wasNull()) {
                    scale = null;
                }
                String starRocksTypeStr = MysqlType.toStarRocksType(fieldType, precision, scale);
                fields.put(fieldName, new FieldSchema(fieldName, starRocksTypeStr, comment));
            }
        }

        primaryKeys = new ArrayList<>();
        try (ResultSet rs = metaData.getPrimaryKeys(databaseName, null, tableName)) {
            while (rs.next()) {
                String fieldName = rs.getString("COLUMN_NAME");
                primaryKeys.add(fieldName);
            }
        }
    }

    public String getTableIdentifier(){
        return getString(databaseName, null, tableName);
    }

    public static String getString(String databaseName, String schemaName, String tableName) {
        StringJoiner identifier = new StringJoiner(".");
        if(!StringUtils.isNullOrWhitespaceOnly(databaseName)){
            identifier.add(databaseName);
        }
        if(!StringUtils.isNullOrWhitespaceOnly(schemaName)){
            identifier.add(schemaName);
        }

        if(!StringUtils.isNullOrWhitespaceOnly(tableName)){
            identifier.add(tableName);
        }

        return identifier.toString();
    }

    public TableSchema convertTableSchema(Map<String, String> tableProps) {
        TableSchema tableSchema = new TableSchema();
        tableSchema.setModel(DataModel.PRIMARY);
        tableSchema.setFields(this.fields);
        tableSchema.setKeys(this.primaryKeys);
        tableSchema.setTableComment(this.tableComment);
        tableSchema.setDistributeKeys(this.primaryKeys);
        tableSchema.setProperties(tableProps);
        return tableSchema;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public LinkedHashMap<String, FieldSchema> getFields() {
        return fields;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public String getTableComment() {
        return tableComment;
    }
}
