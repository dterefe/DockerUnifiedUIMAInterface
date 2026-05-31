package org.texttechnologylab.duui.dua.backend.postgres;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public enum DUAPostgresRangeType {
    INT4("int4range"),
    INT8("int8range");

    private final String functionName;

    DUAPostgresRangeType(String functionName) {
        this.functionName = functionName;
    }

    public String expression() {
        return functionName + "(?, ?, '[)')";
    }

    public void bindPoint(PreparedStatement statement, int parameter, int value) throws SQLException {
        if (this == INT4) {
            statement.setInt(parameter, value);
        } else {
            statement.setLong(parameter, value);
        }
    }

    public void bindRange(PreparedStatement statement, int firstParameter, int begin, int end) throws SQLException {
        bindPoint(statement, firstParameter, begin);
        bindPoint(statement, firstParameter + 1, end);
    }
}
