package org.texttechnologylab.duui.dua;

import org.apache.uima.cas.CAS;
import org.apache.uima.cas.FeatureStructure;
import org.apache.uima.cas.Type;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.IntegerArray;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.duui.dua.uima.DUABackedCasFactory;
import org.texttechnologylab.duui.dua.uima.storage.DUAStorageBackend;
import org.texttechnologylab.duui.dua.uima.storage.DUATieredCasStorage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class DUABackedCasTest {
    @Test
    void jCasUsesInstalledDuaBackendForSlotsAndArrays() throws Exception {
        JCas jCas = DUABackedCasFactory.createJCas();
        CAS cas = jCas.getCas();

        Type annotationType = cas.getAnnotationType();
        FeatureStructure fs = cas.createFS(annotationType);
        fs.setIntValue(annotationType.getFeatureByBaseName("begin"), 4);
        fs.setIntValue(annotationType.getFeatureByBaseName("end"), 9);

        assertEquals(4, fs.getIntValue(annotationType.getFeatureByBaseName("begin")));
        assertEquals(9, fs.getIntValue(annotationType.getFeatureByBaseName("end")));

        IntegerArray array = new IntegerArray(jCas, 3);
        array.set(2, 42);

        assertEquals(42, array.get(2));
        assertNotNull(cas.getTypeSystem());
        DUAStorageBackend backend = assertInstanceOf(DUAStorageBackend.class, jCas.getCasImpl().getBaseCAS().backend());
        assertInstanceOf(DUATieredCasStorage.class, backend.storage());
    }
}
