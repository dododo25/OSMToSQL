package com.dododo.osmtosql.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

public class PostgreSQLService {

    public static final int INVALID = -1;

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLService.class);

    private final Connection connection;

    public PostgreSQLService(Connection connection) {
        this.connection = connection;
    }

    public int dropSchema(String name) {
        String sql = "DROP SCHEMA IF EXISTS %s CASCADE";

        try (Statement statement = connection.createStatement()) {
            statement.execute(String.format(sql, name));
            return 0;
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
            return INVALID;
        }
    }

    public int createSchema(String name) {
        String sql = "CREATE SCHEMA IF NOT EXISTS %s";

        try (Statement statement = connection.createStatement()) {
            statement.execute(String.format(sql, name));
            return 0;
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
            return INVALID;
        }
    }

    public int createExtension(String name) {
        String sql = "CREATE EXTENSION IF NOT EXISTS %s";

        try (Statement statement = connection.createStatement()) {
            statement.execute(String.format(sql, name));
            return 0;
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
            return INVALID;
        }
    }

    public int createTable(CreateTableRequestBuilder mapping) {
        String lastSQl = null;

        try (Statement statement = connection.createStatement()) {
            for (String sql : mapping.build()) {
                lastSQl = sql;
                statement.execute(sql);
            }

            return 0;
        } catch (SQLException e) {
            LOGGER.error("{} - {}", lastSQl, e.getMessage(), e);
            return INVALID;
        }
    }

    public <T> void save(SaveEntityRequestBuilder<T> builder, T t) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(builder.build(t));
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    public <T> void saveAll(SaveEntityRequestBuilder<T> builder, Collection<T> c) {
        try (Statement statement = connection.createStatement()) {
            for (T t : c) {
                statement.addBatch(builder.build(t));
            }

            statement.executeLargeBatch();
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }
}
