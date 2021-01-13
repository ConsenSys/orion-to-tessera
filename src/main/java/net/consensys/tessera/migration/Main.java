package net.consensys.tessera.migration;

import net.consensys.tessera.migration.data.LevelDbCmdConvertor;
import org.iq80.leveldb.DB;
import picocli.CommandLine;

public class Main {

    public static void main(String... args) throws Exception {

        MigrateCommand migrateCommand = new MigrateCommand();

        CommandLine commandLine = new CommandLine(migrateCommand)
                .setCaseInsensitiveEnumValuesAllowed(true);

        commandLine.registerConverter(OrionKeyHelper.class,new OrionKeyHelperConvertor());
        commandLine.registerConverter(DB.class,new LevelDbCmdConvertor());

        int exitCode = commandLine.execute(args);

        System.exit(exitCode);
    }

}
