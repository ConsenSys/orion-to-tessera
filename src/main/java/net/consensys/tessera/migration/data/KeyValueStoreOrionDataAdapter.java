package net.consensys.tessera.migration.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lmax.disruptor.dsl.Disruptor;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.kv.KeyValueStore;

import javax.json.JsonObject;
import java.util.Iterator;


public class KeyValueStoreOrionDataAdapter implements OrionDataAdapter {

    private KeyValueStore<Bytes, Bytes> database;

    private ObjectMapper cborObjectMapper;

    private Disruptor<OrionRecordEvent> disruptor;

    public KeyValueStoreOrionDataAdapter(KeyValueStore<Bytes, Bytes> db,
                                         ObjectMapper cborObjectMapper,
                                         Disruptor<OrionRecordEvent> disruptor) {
        this.database = db;
        this.cborObjectMapper = cborObjectMapper;
        this.disruptor = disruptor;
    }

    @Override
    public void start() throws Exception {
        Iterator<Bytes> iterator = this.database.keysAsync().get().iterator();

        if (!iterator.hasNext()) {
            return;
        }
        Bytes dbKey = iterator.next();

        for (; iterator.hasNext(); dbKey = iterator.next()) {
            Bytes val = database.getAsync(dbKey).get();

            String key = dbKey.toBase64String();
            byte[] value = val.toArray();

            JsonObject jsonObject = cborObjectMapper.readValue(value, JsonObject.class);

            PayloadType payloadType = PayloadType.get(jsonObject);

            if (payloadType == PayloadType.EncryptedPayload) {
                if(jsonObject.containsKey("privacyGroupId")) {
                    System.out.println("Publishing " + key);
                    disruptor.publishEvent(new OrionRecordEvent(InputType.MAPDB, key, value));
                    System.out.println("Published " + key);
                }

            }
        }
    }

}
