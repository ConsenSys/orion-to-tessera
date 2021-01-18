package net.consensys.tessera.migration.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.kv.KeyValueStore;

import javax.json.JsonObject;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

public class KeyValueStoreRecordCounter implements RecordCounter {

    private final KeyValueStore<Bytes, Bytes> database;

    private final ObjectMapper cborObjectMapper;

    public KeyValueStoreRecordCounter(KeyValueStore<Bytes, Bytes> database, ObjectMapper cborObjectMapper) {
        this.database = database;
        this.cborObjectMapper = cborObjectMapper;
    }

    @Override
    public long count() throws Exception {
        AtomicLong counter = new AtomicLong(0);
        Iterator<Bytes> iterator = this.database.keysAsync().get().iterator();

        if (!iterator.hasNext()) {
            return 0;
        }
        Bytes dbKey = iterator.next();

        for (; iterator.hasNext(); dbKey = iterator.next()) {
            Bytes val = database.getAsync(dbKey).get();

            byte[] value = val.toArray();

            JsonObject jsonObject = cborObjectMapper.readValue(value, JsonObject.class);
            PayloadType payloadType = PayloadType.get(jsonObject);

            if(payloadType == PayloadType.EncryptedPayload) {
                if(jsonObject.containsKey("privacyGroupId")) {
                    counter.incrementAndGet();
                }
            }
        }

        return counter.get();
    }
}
