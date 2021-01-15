package net.consensys.tessera.migration.data;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr353.JSR353Module;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.quorum.tessera.encryption.Encryptor;
import com.quorum.tessera.encryption.EncryptorFactory;
import net.consensys.tessera.migration.MigrateCommand;
import net.consensys.tessera.migration.OrionKeyHelper;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;

public class MigrateDataCommand implements Callable<Boolean> {

    private TesseraJdbcOptions tesseraJdbcOptions;

    private OrionKeyHelper orionKeyHelper;

    private Encryptor tesseraEncryptor = EncryptorFactory.newFactory("NACL").create();

    private MigrateCommand.InboundDBArgs args;

    private ObjectMapper cborObjectMapper = JsonMapper.builder(new CBORFactory())
            .addModule(new Jdk8Module())
            .addModule(new JSR353Module())
            .serializationInclusion(JsonInclude.Include.NON_NULL)
            .build();

    static EntityManagerFactory createEntityManagerFactory(TesseraJdbcOptions jdbcOptions) {
        Map jdbcProperties = new HashMap<>();
        jdbcProperties.put("javax.persistence.jdbc.user", jdbcOptions.getUsername());
        jdbcProperties.put("javax.persistence.jdbc.password", jdbcOptions.getPassword());
        jdbcProperties.put("javax.persistence.jdbc.url", jdbcOptions.getUrl());
        jdbcProperties.put("eclipselink.logging.level", "FINE");
        jdbcProperties.put("eclipselink.logging.parameters", "true");
        jdbcProperties.put("eclipselink.logging.level.sql", "FINE");

        jdbcProperties.put(
                "javax.persistence.schema-generation.database.action",jdbcOptions.getAction());

        return Persistence.createEntityManagerFactory("tessera-em", jdbcProperties);
    }

    public MigrateDataCommand(MigrateCommand.InboundDBArgs args,
                              TesseraJdbcOptions tesseraJdbcOptions,
                              OrionKeyHelper orionKeyHelper) {
        this.args = args;
        this.tesseraJdbcOptions = tesseraJdbcOptions;
        this.orionKeyHelper = orionKeyHelper;
    }

    @Override
    public Boolean call() throws Exception {


        Disruptor<OrionRecordEvent> disruptor
                = new Disruptor<>(OrionRecordEvent.FACTORY, 128, (ThreadFactory) Thread::new, ProducerType.SINGLE, new BlockingWaitStrategy());


        InputType inputType = args.inputType();
        final RecordCounter recordCounter;
        final OrionDataAdapter inboundAdapter;
        final PrivacyGroupPayloadLookup privacyGroupPayloadLookup;
        switch (inputType) {
            case LEVELDB:
                inboundAdapter = new LevelDbOrionDataAdapter(args.getLevelDb(),cborObjectMapper,disruptor);
                recordCounter = new LevelDbRecordCounter(args.getLevelDb(),cborObjectMapper);
                privacyGroupPayloadLookup = new LeveldbPrivacyGroupPayloadLookup(args.getLevelDb(),cborObjectMapper);
                break;
            case JDBC:
                inboundAdapter = new JdbcOrionDataAdapter(args.getJdbcArgs(),cborObjectMapper,orionKeyHelper);
                recordCounter = new JdbcRecordCounter(args.getJdbcArgs());
                privacyGroupPayloadLookup = new JdbcPrivacyGroupPayloadLookup(args.getJdbcArgs(),cborObjectMapper);
                break;
            default:throw new UnsupportedOperationException("");
        }

        EntityManagerFactory entityManagerFactory = createEntityManagerFactory(tesseraJdbcOptions);

        int count = (int) recordCounter.count();
        System.out.printf("COUNT %d",count);
        System.out.println();
        CountDownLatch countDownLatch = new CountDownLatch(count);

        disruptor
                .handleEventsWith(new EncryptedPayloadEventHandler(cborObjectMapper))
                .then(new PrivacyGroupPayloadEventHandler(privacyGroupPayloadLookup))
                .then(new RecipientBoxesEventHandler(orionKeyHelper))
                .then(new ValidateEventHandler(orionKeyHelper,tesseraEncryptor))
                .then(new PersistEventHandler(entityManagerFactory))
                .then(new CompletionHandler(countDownLatch));

      //  disruptor.setDefaultExceptionHandler(new FatalExceptionHandler());

        disruptor.start();
        inboundAdapter.start();

        countDownLatch.await();

        disruptor.shutdown();

        return Boolean.TRUE;
    }
}
