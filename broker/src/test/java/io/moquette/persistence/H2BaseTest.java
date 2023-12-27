package io.moquette.persistence;

import io.moquette.BrokerConstants;
import org.h2.mvstore.MVStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertFalse;

abstract class H2BaseTest {
    MVStore mvStore;

    @BeforeEach
    public void setUp() {
        this.mvStore = new MVStore.Builder()
            .fileName(BrokerConstants.DEFAULT_PERSISTENT_PATH)
            .autoCommitDisabled()
            .open();
    }

    @AfterEach
    public void tearDown() {
        this.mvStore.close();
        File dbFile = new File(BrokerConstants.DEFAULT_PERSISTENT_PATH);
        if (dbFile.exists()) {
            dbFile.delete();
        }
        assertFalse(dbFile.exists());
    }
}
