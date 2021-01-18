package net.consensys.tessera.migration;

import com.quorum.tessera.config.Config;
import net.consensys.tessera.migration.config.MigrateConfigCommand;
import net.consensys.tessera.migration.data.InputType;
import net.consensys.tessera.migration.data.MigrateDataCommand;
import net.consensys.tessera.migration.data.TesseraJdbcOptions;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.kv.KeyValueStore;
import org.apache.tuweni.kv.MapDBKeyValueStore;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Function;

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
    private TesseraJdbcOptions tesseraJdbcOptions = new TesseraJdbcOptions();

    public static class InboundDBArgs {
        private String jdbcUrl;

        private org.iq80.leveldb.DB leveldb;

        private KeyValueStore<Bytes, Bytes> mapdb;

        public String getJdbcArgs() {
            return jdbcUrl;
        }

        public org.iq80.leveldb.DB getLevelDb() {
            return leveldb;
        }

        public KeyValueStore<Bytes, Bytes> getMapDB() {
            return mapdb;
        }

        public void setJdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
        }

        public void setLevelDb(org.iq80.leveldb.DB leveldb) {
            this.leveldb = leveldb;
        }

        public void setMapdb(KeyValueStore<Bytes, Bytes> mapdb) {
            this.mapdb = mapdb;
        }

        public InputType inputType() {
            if(Objects.nonNull(jdbcUrl)) {
                return InputType.JDBC;
            }

            if(Objects.nonNull(mapdb)) {
                return InputType.MAPDB;
            }

            if(Objects.nonNull(leveldb)) {
                return InputType.LEVELDB;
            }

            throw new UnsupportedOperationException("No supported database was found");
        }

    }

    @Override
    public Config call() throws Exception {

        MigrateConfigCommand migrateConfigCommand
                = new MigrateConfigCommand(orionKeyHelper.getFilePath(), outputFile, skipValidation, verbose, tesseraJdbcOptions);
        Config config = migrateConfigCommand.call();

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

        if (storage.toLowerCase().startsWith("mapdb")) {
            final Function<Bytes, Bytes> bytesIdentityFn = Function.identity();
            final KeyValueStore<Bytes, Bytes> mapdb = MapDBKeyValueStore
                    .open(storagePath.resolve(dbName), bytesIdentityFn, bytesIdentityFn, bytesIdentityFn, bytesIdentityFn);

            InboundDBArgs args = new InboundDBArgs();
            args.setMapdb(mapdb);
            return args;
        }

        throw new UnsupportedOperationException("unsupported storage mechanism: " + storage);
    }
}
