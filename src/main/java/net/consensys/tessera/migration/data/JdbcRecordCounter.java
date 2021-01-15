package net.consensys.tessera.migration.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class JdbcRecordCounter implements RecordCounter {

    private final String jdbcUrl;

    public JdbcRecordCounter(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    @Override
    public long count() throws Exception {

        Connection connection = DriverManager.getConnection(jdbcUrl);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM STORE");

        try(connection;statement;resultSet) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }
}
