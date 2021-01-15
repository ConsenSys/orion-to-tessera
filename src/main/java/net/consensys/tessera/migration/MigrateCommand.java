package net.consensys.tessera.migration;

import com.quorum.tessera.config.Config;
import com.quorum.tessera.config.util.JaxbUtil;
import net.consensys.tessera.migration.config.MigrateConfigCommand;
import net.consensys.tessera.migration.data.InputType;
import net.consensys.tessera.migration.data.MigrateDataCommand;
import net.consensys.tessera.migration.data.TesseraJdbcOptions;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import picocli.CommandLine;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.Callable;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

public class MigrateCommand implements Callable<Config> {

    @CommandLine.Option(names = {"-f", "orionfile", "orionconfig"}, required = true, description = "Orion config file")
    private OrionKeyHelper orionKeyHelper;

    @CommandLine.Option(names = {"-o", "outputfile"}, required = true, description = "Output Tessera config file")
    private Path outputFile;

    @CommandLine.Option(names = {"-sv", "skipValidation"})
    private boolean skipValidation;

    @CommandLine.Option(names = {"-v", "--verbose"})
    private boolean verbose;

    @CommandLine.Mixin
    private TesseraJdbcOptions tesseraJdbcOptions;

    public static class InboundDBArgs {
        private String jdbcUrl;

        private org.iq80.leveldb.DB leveldb;

        public String getJdbcArgs() {
            return jdbcUrl;
        }

        public org.iq80.leveldb.DB getLevelDb() {
            return leveldb;
        }

        public void setJdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
        }

        public void setLevelDb(org.iq80.leveldb.DB leveldb) {
            this.leveldb = leveldb;
        }

        public InputType inputType() {
            if(Objects.nonNull(jdbcUrl)) {
                return InputType.JDBC;
            }

            if(Objects.nonNull(leveldb)) {
                return InputType.LEVELDB;
            }

            throw new UnsupportedOperationException("GURU meditation");
        }

    }

    @Override
    public Config call() throws Exception {

        MigrateConfigCommand migrateConfigCommand = new MigrateConfigCommand(orionKeyHelper.getFilePath(),outputFile,skipValidation,verbose);
        Config config = migrateConfigCommand.call();

        config.getJdbcConfig().setUsername(tesseraJdbcOptions.getUsername());
        config.getJdbcConfig().setPassword(tesseraJdbcOptions.getPassword());
        config.getJdbcConfig().setUrl(tesseraJdbcOptions.getUrl());

        try(OutputStream outputStream = new  TeeOutputStream(Files.newOutputStream(outputFile),System.out)) {
            JaxbUtil.marshalWithNoValidation(config, outputStream);
        }

        InboundDBArgs args = createArgs(orionKeyHelper.getConfig().storage(), orionKeyHelper.getConfig().workDir(), "routerdb");
        MigrateDataCommand migrateDataCommand = new MigrateDataCommand(args,tesseraJdbcOptions,orionKeyHelper);

        boolean outcome = migrateDataCommand.call();

        return config;
    }

    private InboundDBArgs createArgs(final String storage, final Path storagePath, String dbName) {
        final String[] storageOptions = storage.split(":", 2);
        if (storageOptions.length > 1) {
            dbName = storageOptions[1];
        }

        if (storage.toLowerCase().startsWith("leveldb")) {
            try {
                Options options = new Options();
                options.logger(System.out::println);
                options.createIfMissing(true);

                DB open = factory.open(storagePath.resolve(dbName).toAbsolutePath().toFile(), options);

                InboundDBArgs args = new InboundDBArgs();
                args.setLevelDb(open);
                return args;
            } catch (final IOException e) {
                throw new RuntimeException("Couldn't create LevelDB store: " + dbName, e);
            }
        }

        if (storage.toLowerCase().startsWith("sql")) {
            InboundDBArgs args = new InboundDBArgs();
            args.setJdbcUrl(dbName);
            return args;
        }

        throw new UnsupportedOperationException("unsupported storage mechanism: " + storage);
    }
}
