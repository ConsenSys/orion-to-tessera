package net.consensys.tessera.migration.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.consensys.orion.enclave.EncryptedPayload;
import net.consensys.orion.enclave.PrivacyGroupPayload;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.kv.KeyValueStore;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Base64;
import java.util.Optional;

public class KeyValueStorePrivacyGroupPayloadLookup implements PrivacyGroupPayloadLookup {

    private final KeyValueStore<Bytes, Bytes> database;

    private final ObjectMapper objectMapper;

    public KeyValueStorePrivacyGroupPayloadLookup(KeyValueStore<Bytes, Bytes> database, ObjectMapper objectMapper) {
        this.database = database;
        this.objectMapper = objectMapper;
    }

    @Override
    public Optional<PrivacyGroupPayload> findRecipients(EncryptedPayload encryptedPayload) {
        byte[] privacyGroupId = encryptedPayload.privacyGroupId();
        byte[] encodedGroupId = Base64.getEncoder().encode(privacyGroupId);

        Bytes privacyGroupPayloadData;

        try {
            privacyGroupPayloadData = database.getAsync(Bytes.wrap(encodedGroupId)).get();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }

        return Optional.ofNullable(privacyGroupPayloadData)
                .map(Bytes::toArray)
                .map(data -> {
                    try {
                        return objectMapper.readValue(data,PrivacyGroupPayload.class);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
    }
}
