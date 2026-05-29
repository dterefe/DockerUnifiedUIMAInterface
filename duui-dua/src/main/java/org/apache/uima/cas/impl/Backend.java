/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.uima.cas.impl;

import org.apache.uima.cas.Feature;

public interface Backend {

  SlotBackend slots();

  ArrayBackend arrays();

  CollectionBackend collections();

  StringBackend strings();

  LifecycleBackend lifecycle();

  interface SlotBackend {
    boolean getBooleanValue(int fsRef, Feature feature);

    void setBooleanValue(int fsRef, Feature feature, boolean value);

    byte getByteValue(int fsRef, Feature feature);

    void setByteValue(int fsRef, Feature feature, byte value);

    short getShortValue(int fsRef, Feature feature);

    void setShortValue(int fsRef, Feature feature, short value);

    int getIntValue(int fsRef, Feature feature);

    void setIntValue(int fsRef, Feature feature, int value);

    long getLongValue(int fsRef, Feature feature);

    void setLongValue(int fsRef, Feature feature, long value);

    float getFloatValue(int fsRef, Feature feature);

    void setFloatValue(int fsRef, Feature feature, float value);

    double getDoubleValue(int fsRef, Feature feature);

    void setDoubleValue(int fsRef, Feature feature, double value);

    String getStringValue(int fsRef, Feature feature);

    void setStringValue(int fsRef, Feature feature, String value);

    int getRefValue(int fsRef, Feature feature);

    void setRefValue(int fsRef, Feature feature, int targetFsRef);
  }

  interface ArrayBackend {
    default void initializeFsArray(int fsRef, int length) {}

    int sizeFsArray(int fsRef);

    int getFsArrayRefValue(int fsRef, int index);

    void setFsArrayRefValue(int fsRef, int index, int targetFsRef);

    void copyFromFsArray(int fsRef, int destPos, int[] srcFsRefs, int srcPos, int length);

    void copyToFsArray(int fsRef, int srcPos, int[] destFsRefs, int destPos, int length);

    default void initializeIntegerArray(int fsRef, int length) {}

    int sizeIntegerArray(int fsRef);

    int getIntegerArrayValue(int fsRef, int index);

    void setIntegerArrayValue(int fsRef, int index, int value);

    void copyFromIntegerArray(int fsRef, int destPos, int[] src, int srcPos, int length);

    void copyToIntegerArray(int fsRef, int srcPos, int[] dest, int destPos, int length);

    default void initializeFloatArray(int fsRef, int length) {}

    int sizeFloatArray(int fsRef);

    float getFloatArrayValue(int fsRef, int index);

    void setFloatArrayValue(int fsRef, int index, float value);

    void copyFromFloatArray(int fsRef, int destPos, float[] src, int srcPos, int length);

    void copyToFloatArray(int fsRef, int srcPos, float[] dest, int destPos, int length);

    default void initializeStringArray(int fsRef, int length) {}

    int sizeStringArray(int fsRef);

    String getStringArrayValue(int fsRef, int index);

    void setStringArrayValue(int fsRef, int index, String value);

    void copyFromStringArray(int fsRef, int destPos, String[] src, int srcPos, int length);

    void copyToStringArray(int fsRef, int srcPos, String[] dest, int destPos, int length);

    default void initializeBooleanArray(int fsRef, int length) {}

    int sizeBooleanArray(int fsRef);

    boolean getBooleanArrayValue(int fsRef, int index);

    void setBooleanArrayValue(int fsRef, int index, boolean value);

    void copyFromBooleanArray(int fsRef, int destPos, boolean[] src, int srcPos, int length);

    void copyToBooleanArray(int fsRef, int srcPos, boolean[] dest, int destPos, int length);

    default void initializeByteArray(int fsRef, int length) {}

    int sizeByteArray(int fsRef);

    byte getByteArrayValue(int fsRef, int index);

    void setByteArrayValue(int fsRef, int index, byte value);

    void copyFromByteArray(int fsRef, int destPos, byte[] src, int srcPos, int length);

    void copyToByteArray(int fsRef, int srcPos, byte[] dest, int destPos, int length);

    default void initializeShortArray(int fsRef, int length) {}

    int sizeShortArray(int fsRef);

    short getShortArrayValue(int fsRef, int index);

    void setShortArrayValue(int fsRef, int index, short value);

    void copyFromShortArray(int fsRef, int destPos, short[] src, int srcPos, int length);

    void copyToShortArray(int fsRef, int srcPos, short[] dest, int destPos, int length);

    default void initializeLongArray(int fsRef, int length) {}

    int sizeLongArray(int fsRef);

    long getLongArrayValue(int fsRef, int index);

    void setLongArrayValue(int fsRef, int index, long value);

    void copyFromLongArray(int fsRef, int destPos, long[] src, int srcPos, int length);

    void copyToLongArray(int fsRef, int srcPos, long[] dest, int destPos, int length);

    default void initializeDoubleArray(int fsRef, int length) {}

    int sizeDoubleArray(int fsRef);

    double getDoubleArrayValue(int fsRef, int index);

    void setDoubleArrayValue(int fsRef, int index, double value);

    void copyFromDoubleArray(int fsRef, int destPos, double[] src, int srcPos, int length);

    void copyToDoubleArray(int fsRef, int srcPos, double[] dest, int destPos, int length);
  }

  interface CollectionBackend {
    void initFsArrayListFromCasData(int fsRef);

    void saveFsArrayListToCasData(int fsRef);

    void initFsHashSetFromCasData(int fsRef);

    void saveFsHashSetToCasData(int fsRef);

    void initInt2FsFromCasData(int fsRef);

    void saveInt2FsToCasData(int fsRef);

    void initIntegerArrayListFromCasData(int fsRef);

    void saveIntegerArrayListToCasData(int fsRef);
  }

  interface StringBackend {
    String getStringForCode(int code);

    int getCodeForString(String value);
  }

  interface LifecycleBackend {
    int allocateFsId(int typeCode, int viewId);

    void onFsCreated(int fsRef, int typeCode, int viewId);

    void onFsDeleted(int fsRef);
  }
}
