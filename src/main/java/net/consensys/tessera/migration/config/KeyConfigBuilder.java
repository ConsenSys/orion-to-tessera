package net.consensys.tessera.migration.config;

import com.quorum.tessera.config.KeyConfiguration;
import com.quorum.tessera.config.KeyData;

import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class KeyConfigBuilder {

    private String passwordsFile;

    private List<String> publicKeys = List.of();

    private List<String> privateKeys = List.of();

    public KeyConfigBuilder withPasswordsFile(String passwordsFile) {
        this.passwordsFile = passwordsFile;
        return this;
    }

    public KeyConfigBuilder withPublicKeys(List<String> publicKeys) {
        this.publicKeys = publicKeys;
        return this;
    }

    public KeyConfigBuilder withPrivateKeys(List<String> privateKeys) {
        this.privateKeys = privateKeys;
        return this;
    }

    static KeyConfigBuilder create() {
        return new KeyConfigBuilder();
    }

    public KeyConfiguration build() {

        Objects.requireNonNull(publicKeys);
        Objects.requireNonNull(privateKeys);

        if(publicKeys.size() != privateKeys.size()) {
            throw new IllegalStateException("Expected public and private key pairs to match");
        }

        List<KeyData> keys = IntStream.range(0,privateKeys.size())
                .mapToObj(i -> {
                    KeyData keyData = new KeyData();
                    keyData.setPrivateKeyPath(Paths.get(privateKeys.get(i)));
                    keyData.setPublicKeyPath(Paths.get(publicKeys.get(i)));
                    return keyData;
        }).collect(Collectors.toList());

        KeyConfiguration keyConfiguration = new KeyConfiguration();
        Optional.ofNullable(passwordsFile)
                .map(Paths::get)
                .ifPresent(keyConfiguration::setPasswordFile);

        keyConfiguration.setKeyData(keys);

        return keyConfiguration;
    }

}
