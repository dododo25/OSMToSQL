package com.dododo.osmtosql.service;

import net.postgis.jdbc.geometry.Geometry;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;

public class SaveEntityRequestBuilder<T> {

    private final String schemaName;

    private final String tableName;

    private final Map<String, Function<T, String>> columns;

    public SaveEntityRequestBuilder(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columns = new HashMap<>();
    }

    public void mapShort(String columnName, Function<T, Short> function) {
        columns.put(columnName, t -> String.valueOf(function.apply(t)));
    }

    public void mapLong(String columnName, ToLongFunction<T> function) {
        columns.put(columnName, t -> String.valueOf(function.applyAsLong(t)));
    }

    public void mapDouble(String columnName, ToDoubleFunction<T> function) {
        columns.put(columnName, t -> String.valueOf(function.applyAsDouble(t)));
    }

    public void mapString(String columnName, Function<T, String> function) {
        columns.put(columnName, t -> String.format("'%s'", function.apply(t).replace("'", "''")));
    }

    public void mapGeometry(String columnName, Function<T, Geometry> function) {
        columns.put(columnName, t -> {
            Geometry geom = function.apply(t);
            return geom == null ? "null" : String.format("St_GeomFromText('%s')", geom);
        });
    }

    public String build(T t) {
        String sqlFormat = "insert into %s.%s (%s) values (%s)";

        String[] columnNames = new String[columns.size()];
        String[] values = new String[columns.size()];

        prepareValues(columnNames, values, t);

        return String.format(sqlFormat, schemaName, tableName,
                String.join(", ", columnNames), String.join(", ", values));
    }

    private void prepareValues(String[] columnsNames, String[] values, T t) {
        int index = 0;

        for (Map.Entry<String, Function<T, String>> entry : columns.entrySet()) {
            columnsNames[index] = entry.getKey();
            values[index] = entry.getValue().apply(t);

            index++;
        }
    }
}
