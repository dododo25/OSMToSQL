package com.dododo.osmtosql.service;

import java.util.*;
import java.util.stream.Collectors;

public class CreateTableRequestBuilder {

    private final String schemaName;

    private final String tableName;

    private final Collection<String> added;

    private final Collection<Column> primaryColumns;

    private final Collection<Column> columns;

    private final Map<String, String> references;

    public CreateTableRequestBuilder(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;

        this.added = new HashSet<>();
        this.primaryColumns = new HashSet<>();
        this.columns = new HashSet<>();
        this.references = new HashMap<>();
    }

    public void addPrimaryColumn(String columnName, Type type, Integer precision, Integer scale) {
        if (added.contains(columnName)) {
            throw new IllegalArgumentException();
        }

        added.add(columnName);
        primaryColumns.add(new Column(columnName, type, precision, scale, false));
    }

    public void addColumn(String columnName, Type type, Integer precision, Integer scale, boolean nullable) {
        if (added.contains(columnName)) {
            throw new IllegalArgumentException();
        }

        added.add(columnName);
        columns.add(new Column(columnName, type, precision, scale, nullable));
    }

    public void asForeignColumn(String columnName, String relativeSchemaName, String relativeTableName,
                                String relativeColumnName) {
        if (!added.contains(columnName)) {
            throw new IllegalArgumentException();
        }

        references.put(columnName, String.format("%s.%s(%s)", relativeSchemaName, relativeTableName,
                relativeColumnName));
    }

    public String[] build() {
        List<String> results = new ArrayList<>();

        results.add(buildCreateTableSQLRequest());
        results.addAll(buildAddForeignKeySQLRequests());

        return results.toArray(new String[0]);
    }

    private String buildCreateTableSQLRequest() {
        String sqlFormat = "create table %s.%s (%s)";

        Collection<String> groupedColumns = new ArrayList<>();

        primaryColumns.forEach(column -> groupedColumns.add(column.build()));
        columns.forEach(column -> groupedColumns.add(column.build()));

        if (!primaryColumns.isEmpty()) {
            String primaryKeyColumns = primaryColumns.stream().map(column -> column.name)
                    .collect(Collectors.joining(", "));

            groupedColumns.add(String.format("primary key (%s)", primaryKeyColumns));
        }

        return String.format(sqlFormat, schemaName, tableName, String.join(", ", groupedColumns));
    }

    private List<String> buildAddForeignKeySQLRequests() {
        String sqlFormat = String.format("alter table %s.%s add constraint %s foreign key (%s) references %s",
                schemaName, tableName, "%s", "%s", "%s");
        String constraintSqlFormat = String.format("%s_%s_%s_fk", schemaName, tableName, "%s");

        return references.entrySet()
                .stream()
                .map(entry -> String.format(sqlFormat, String.format(constraintSqlFormat, entry.getKey()),
                        entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    private static class Column {

        private final String name;

        private final Type type;

        private final Integer precision;

        private final Integer scale;

        private final boolean nullable;

        private Column(String name, Type type, Integer precision, Integer scale, boolean nullable) {
            this.name = name;
            this.type = type;
            this.precision = precision;
            this.scale = scale;
            this.nullable = nullable;
        }

        public String build() {
            String result = String.format("%s %s", name, type.value);

            if (precision != null) {
                result += String.format("(%d", precision);

                if (scale != null) {
                    result += String.format(", %d", scale);
                }

                result += ")";
            }

            if (!nullable) {
                result += " not null";
            }

            return result;
        }
    }

    public enum Type {
        VARCHAR("varchar"),
        SMALLINT("smallint"),
        BIGINT("bigint"),
        DECIMAL("decimal"),
        GEOMETRY("geometry");

        private final String value;

        Type(String value) {
            this.value = value;
        }
    }
}
