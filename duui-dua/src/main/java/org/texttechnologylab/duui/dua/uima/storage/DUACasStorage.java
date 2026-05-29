package org.texttechnologylab.duui.dua.uima.storage;

import java.util.Optional;

public interface DUACasStorage extends AutoCloseable {
    Optional<DUACasValue> readSlot(int fsRef, String featureName);

    void writeSlot(int fsRef, String featureName, DUACasValue value);

    default Optional<DUACasValue> readSlot(int fsRef, int featureCode, String featureName) {
        return readSlot(fsRef, featureName);
    }

    default void writeSlot(int fsRef, int featureCode, String featureName, DUACasValue value) {
        writeSlot(fsRef, featureName, value);
    }

    void initializeArray(DUACasArrayKind kind, int fsRef, int length);

    int arraySize(DUACasArrayKind kind, int fsRef);

    Optional<DUACasValue> readArrayValue(DUACasArrayKind kind, int fsRef, int index);

    void writeArrayValue(DUACasArrayKind kind, int fsRef, int index, DUACasValue value);

    String stringForCode(int code);

    int codeForString(String value);

    int allocateFsId(int typeCode, int viewId);

    default void onFsCreated(int fsRef, int typeCode, int viewId) {
    }

    default void onFsDeleted(int fsRef) {
    }

    @Override
    default void close() {
    }
}
