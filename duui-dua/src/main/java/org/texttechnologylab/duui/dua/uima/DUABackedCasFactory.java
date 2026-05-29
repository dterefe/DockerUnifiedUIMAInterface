package org.texttechnologylab.duui.dua.uima;

import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.texttechnologylab.duui.dua.uima.storage.DUAConcurrentMemoryCasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUADenseMemoryCasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUAOrderedKvCasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUASqliteCasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUAStorageBackend;
import org.texttechnologylab.duui.dua.uima.storage.DUATieredCasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUATieredWritePolicy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class DUABackedCasFactory {
    private DUABackedCasFactory() {
    }

    public static JCas createJCas() throws UIMAException {
        return createJCas(DUACasBackendProfile.DUA_CANONICAL);
    }

    public static JCas createJCas(DUACasBackendProfile profile) throws UIMAException {
        JCas jCas = JCasFactory.createJCas();
        install(jCas, profile, null);
        return jCas;
    }

    public static JCas createJCas(TypeSystemDescription typeSystemDescription) throws UIMAException {
        return createJCas(typeSystemDescription, DUACasBackendProfile.DUA_CANONICAL);
    }

    public static JCas createJCas(TypeSystemDescription typeSystemDescription, DUACasBackendProfile profile) throws UIMAException {
        JCas jCas = JCasFactory.createJCas(typeSystemDescription);
        install(jCas, profile, null);
        return jCas;
    }

    public static JCas createSqliteJCas(Path sqlitePath) throws UIMAException {
        JCas jCas = JCasFactory.createJCas();
        install(jCas, DUACasBackendProfile.SQLITE, sqlitePath);
        return jCas;
    }

    public static JCas createSqliteJCas(TypeSystemDescription typeSystemDescription, Path sqlitePath) throws UIMAException {
        JCas jCas = JCasFactory.createJCas(typeSystemDescription);
        install(jCas, DUACasBackendProfile.SQLITE, sqlitePath);
        return jCas;
    }

    public static JCas createCanonicalJCas(Path storageDirectory) throws UIMAException {
        JCas jCas = JCasFactory.createJCas();
        install(jCas, DUACasBackendProfile.DUA_CANONICAL, storageDirectory);
        return jCas;
    }

    public static JCas createCanonicalJCas(TypeSystemDescription typeSystemDescription,
                                           Path storageDirectory) throws UIMAException {
        JCas jCas = JCasFactory.createJCas(typeSystemDescription);
        install(jCas, DUACasBackendProfile.DUA_CANONICAL, storageDirectory);
        return jCas;
    }

    public static JCas createBaselineJCas() throws UIMAException {
        return JCasFactory.createJCas();
    }

    private static void install(JCas jCas, DUACasBackendProfile profile, Path sqlitePath) {
        switch (profile) {
            case DUA_CANONICAL, TIERED_ORDERED_KV_WRITE_BACK -> {
                Path storageDirectory = sqlitePath == null ? temporaryCanonicalStore() : sqlitePath;
                DUAOrderedKvCasStorage durable = new DUAOrderedKvCasStorage(storageDirectory);
                DUACasBackendInstaller.install(jCas, new DUAStorageBackend(
                        new DUATieredCasStorage(durable, 100_000, DUATieredWritePolicy.WRITE_BACK)));
            }
            case UIMA_HEAP_BASELINE -> {
            }
            case CONCURRENT_MEMORY -> DUACasBackendInstaller.install(
                    jCas, new DUAStorageBackend(new DUAConcurrentMemoryCasStorage()));
            case DENSE_MEMORY -> DUACasBackendInstaller.install(
                    jCas, new DUAStorageBackend(new DUADenseMemoryCasStorage()));
            case ORDERED_KV -> {
                Path storageDirectory = sqlitePath == null ? temporaryCanonicalStore() : sqlitePath;
                DUACasBackendInstaller.install(jCas, new DUAStorageBackend(new DUAOrderedKvCasStorage(storageDirectory)));
            }
            case SQLITE -> {
                if (sqlitePath == null) {
                    throw new IllegalArgumentException("sqlitePath is required for SQLITE backend");
                }
                DUACasBackendInstaller.install(jCas, new DUAStorageBackend(new DUASqliteCasStorage(sqlitePath)));
            }
            case TIERED_SQLITE_WRITE_BACK -> {
                if (sqlitePath == null) {
                    throw new IllegalArgumentException("sqlitePath is required for TIERED_SQLITE_WRITE_BACK backend");
                }
                DUASqliteCasStorage durable = new DUASqliteCasStorage(sqlitePath);
                DUACasBackendInstaller.install(jCas, new DUAStorageBackend(
                        new DUATieredCasStorage(durable, 100_000, DUATieredWritePolicy.WRITE_BACK)));
            }
        }
    }

    private static Path temporaryCanonicalStore() {
        try {
            return Files.createTempDirectory("dua-canonical-cas-");
        } catch (IOException e) {
            throw new IllegalStateException("Could not create temporary DUA canonical CAS store", e);
        }
    }
}
