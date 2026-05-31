package org.texttechnologylab.duui.dua.backend.postgres;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

@FunctionalInterface
public interface DUAPostgresConnectionProvider {
    Connection openConnection() throws SQLException;

    static DUAPostgresConnectionProvider from(DataSource dataSource) {
        Objects.requireNonNull(dataSource, "dataSource");
        return dataSource::getConnection;
    }
}
