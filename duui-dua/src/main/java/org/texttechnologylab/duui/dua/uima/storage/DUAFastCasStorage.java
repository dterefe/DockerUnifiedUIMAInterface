package org.texttechnologylab.duui.dua.uima.storage;

public interface DUAFastCasStorage extends DUACasStorage {
    int readIntSlotOrDefault(int fsRef, int featureCode, String featureName, int defaultValue);

    void writeIntSlot(int fsRef, int featureCode, String featureName, int value);
}
