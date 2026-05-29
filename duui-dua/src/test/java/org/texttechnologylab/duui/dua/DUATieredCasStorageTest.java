package org.texttechnologylab.duui.dua;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.texttechnologylab.duui.dua.uima.storage.DUAOrderedKvCasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUASqliteCasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUATieredCasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUATieredWritePolicy;

class DUATieredCasStorageTest {
    @TempDir
    Path temp;

    @Test
    void writeBackPersistsDenseHotSlotsToDurableStoreOnFlush() {
        Path path = temp.resolve("tiered.sqlite");
        try (DUASqliteCasStorage durable = new DUASqliteCasStorage(path);
             DUATieredCasStorage tiered = new DUATieredCasStorage(durable, 1, DUATieredWritePolicy.WRITE_BACK)) {
            tiered.writeIntSlot(1, 101, "begin", 42);
            tiered.writeIntSlot(2, 101, "begin", 84);
            assertEquals(42, tiered.readIntSlotOrDefault(1, 101, "begin", 0));
            tiered.flush();
        }

        try (DUASqliteCasStorage durable = new DUASqliteCasStorage(path)) {
            assertEquals(42, durable.readIntSlotOrDefault(1, 101, "begin", 0));
            assertEquals(84, durable.readIntSlotOrDefault(2, 101, "begin", 0));
        }
    }

    @Test
    void coldPromotionKeepsIndependentSlotsLazyInsteadOfTreatingWholeFsAsLoaded() {
        Path path = temp.resolve("promotion.sqlite");
        try (DUASqliteCasStorage durable = new DUASqliteCasStorage(path)) {
            durable.writeIntSlot(1, 101, "begin", 7);
            durable.writeIntSlot(1, 102, "end", 9);
        }

        try (DUASqliteCasStorage durable = new DUASqliteCasStorage(path);
             DUATieredCasStorage tiered = new DUATieredCasStorage(durable, 1, DUATieredWritePolicy.WRITE_THROUGH)) {
            assertEquals(7, tiered.readIntSlotOrDefault(1, 101, "begin", 0));
            assertEquals(9, tiered.readIntSlotOrDefault(1, 102, "end", 0));
        }
    }

    @Test
    void writeBackCanUseOrderedKvAsDurableShardStore() {
        Path path = temp.resolve("tiered-kv");
        try (DUAOrderedKvCasStorage durable = new DUAOrderedKvCasStorage(path);
             DUATieredCasStorage tiered = new DUATieredCasStorage(durable, 1, DUATieredWritePolicy.WRITE_BACK)) {
            tiered.writeIntSlot(1, 101, "begin", 11);
            tiered.writeIntSlot(2, 101, "begin", 22);
            tiered.flush();
        }

        try (DUAOrderedKvCasStorage durable = new DUAOrderedKvCasStorage(path)) {
            assertEquals(11, durable.readIntSlotOrDefault(1, 101, "begin", 0));
            assertEquals(22, durable.readIntSlotOrDefault(2, 101, "begin", 0));
        }
    }
}
