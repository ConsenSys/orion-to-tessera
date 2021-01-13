package net.consensys.tessera.migration;

import com.quorum.tessera.config.Config;
import com.quorum.tessera.config.util.JaxbUtil;
import net.consensys.tessera.migration.config.MigrateConfigCommand;
import net.consensys.tessera.migration.data.InputType;
import net.consensys.tessera.migration.data.MigrateDataCommand;
import net.consensys.tessera.migration.data.TesseraJdbcOptions;
import org.iq80.leveldb.DB;
import picocli.CommandLine;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.Callable;

public class MigrateCommand implements Callable<Config> {

    @CommandLine.Option(names = {"-f","orionfile","orionconfig"},required = true)
    private OrionKeyHelper orionKeyHelper;

    @CommandLine.Option(names = {"-o","outputfile"},required = true)
    private Path outputFile;

    @CommandLine.Option(names = {"-sv","skipValidation"})
    private boolean skipValidation;

    @CommandLine.Option(names = {"-v","--verbose"})
    private boolean verbose;

    @CommandLine.Mixin
    private TesseraJdbcOptions tesseraJdbcOptions;

    public static class InboundJdbcArgs {
        @CommandLine.Option(names = {"jdbc.user"},required = true)
        private String username;

        @CommandLine.Option(names = {"jdbc.password"},required = true)
        private String password;

        @CommandLine.Option(names = "jdbc.url",required = true)
        private String url;

        public String getUsername() {
            return username;
        }

        public String getPassword() {
            return password;
        }

        public String getUrl() {
            return url;
        }
    }

    public static class LevelDbArgs {
        @CommandLine.Option(names = "leveldb", required = true)
        private org.iq80.leveldb.DB leveldb;

        public DB getLeveldb() {
            return leveldb;
        }
    }

    public static class Args {
        @CommandLine.ArgGroup(exclusive = false, multiplicity = "1", heading = "Jdbc input args%n")
        private InboundJdbcArgs jdbcArgs;

        @CommandLine.ArgGroup(exclusive = false, multiplicity = "1", heading = "LevelDb input args%n")
        private LevelDbArgs levelDbArgs;

        public InboundJdbcArgs getJdbcArgs() {
            return jdbcArgs;
        }

        public LevelDbArgs getLevelDbArgs() {
            return levelDbArgs;
        }

        public InputType inputType() {
            if(Objects.nonNull(jdbcArgs)) {
                return InputType.JDBC;
            }

            if(Objects.nonNull(levelDbArgs)) {
                return InputType.LEVELDB;
            }

            throw new UnsupportedOperationException("GURU meditation");
        }

    }

    @CommandLine.ArgGroup(exclusive = true, multiplicity = "1")
    private Args args;

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

        MigrateDataCommand migrateDataCommand = new MigrateDataCommand(args,tesseraJdbcOptions,orionKeyHelper);

        boolean outcome = migrateDataCommand.call();


        return config;
    }


}
