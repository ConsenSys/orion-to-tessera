package net.consensys.tessera.migration.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lmax.disruptor.dsl.Disruptor;
import net.consensys.tessera.migration.MigrateCommand;
import net.consensys.tessera.migration.OrionKeyHelper;

import javax.json.JsonObject;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Objects;

public class JdbcOrionDataAdapter implements OrionDataAdapter {

    private final MigrateCommand.InboundJdbcArgs jdbcConfig;

    private final ObjectMapper cborObjectMapper;

    private final OrionKeyHelper orionKeyHelper;

    private Disruptor<OrionRecordEvent> disruptor;

    public JdbcOrionDataAdapter(MigrateCommand.InboundJdbcArgs jdbcConfig,
                                ObjectMapper cborObjectMapper,
                                OrionKeyHelper orionKeyHelper) {

        this.jdbcConfig = Objects.requireNonNull(jdbcConfig);
        this.cborObjectMapper = Objects.requireNonNull(cborObjectMapper);
        this.orionKeyHelper = Objects.requireNonNull(orionKeyHelper);
    }

    @Override
    public void start() throws Exception {

        Connection connection = DriverManager.getConnection(jdbcConfig.getUrl(),jdbcConfig.getUsername(), jdbcConfig.getPassword());
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM STORE");
        try(connection;statement;resultSet) {

            while(resultSet.next()) {
                String key = resultSet.getString("KEY");
                byte[] value = resultSet.getBytes("VALUE");

                JsonObject jsonObject = cborObjectMapper.readValue(value, JsonObject.class);


            }
        }
    }

}
