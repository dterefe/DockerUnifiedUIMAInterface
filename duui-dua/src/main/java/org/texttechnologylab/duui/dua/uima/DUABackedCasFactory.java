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

    public static JCas createView() throws UIMAException {
        return createView(DUACasBackendProfile.DUA_CANONICAL);
    }

    public static JCas createView(DUACasBackendProfile profile) throws UIMAException {
        JCas view = JCasFactory.createJCas();
        install(view, profile, null);
        return view;
    }

    public static JCas createView(TypeSystemDescription typeSystemDescription) throws UIMAException {
        return createView(typeSystemDescription, DUACasBackendProfile.DUA_CANONICAL);
    }

    public static JCas createView(TypeSystemDescription typeSystemDescription, DUACasBackendProfile profile) throws UIMAException {
        JCas view = JCasFactory.createJCas(typeSystemDescription);
        install(view, profile, null);
        return view;
    }

    public static JCas createSqliteView(Path sqlitePath) throws UIMAException {
        JCas view = JCasFactory.createJCas();
        install(view, DUACasBackendProfile.SQLITE, sqlitePath);
        return view;
    }

    public static JCas createSqliteView(TypeSystemDescription typeSystemDescription, Path sqlitePath) throws UIMAException {
        JCas view = JCasFactory.createJCas(typeSystemDescription);
        install(view, DUACasBackendProfile.SQLITE, sqlitePath);
        return view;
    }

    public static JCas createCanonicalView(Path storageDirectory) throws UIMAException {
        JCas view = JCasFactory.createJCas();
        install(view, DUACasBackendProfile.DUA_CANONICAL, storageDirectory);
        return view;
    }

    public static JCas createCanonicalView(TypeSystemDescription typeSystemDescription,
                                           Path storageDirectory) throws UIMAException {
        JCas view = JCasFactory.createJCas(typeSystemDescription);
        install(view, DUACasBackendProfile.DUA_CANONICAL, storageDirectory);
        return view;
    }

    public static JCas createBaselineView() throws UIMAException {
        return JCasFactory.createJCas();
    }

    private static void install(JCas view, DUACasBackendProfile profile, Path sqlitePath) {
        switch (profile) {
            case DUA_CANONICAL, TIERED_ORDERED_KV_WRITE_BACK -> {
                Path storageDirectory = sqlitePath == null ? temporaryCanonicalStore() : sqlitePath;
                DUAOrderedKvCasStorage durable = new DUAOrderedKvCasStorage(storageDirectory);
                DUACasBackendInstaller.install(view, new DUAStorageBackend(
                        new DUATieredCasStorage(durable, 100_000, DUATieredWritePolicy.WRITE_BACK)));
            }
            case UIMA_HEAP_BASELINE -> {
            }
            case CONCURRENT_MEMORY -> DUACasBackendInstaller.install(
                    view, new DUAStorageBackend(new DUAConcurrentMemoryCasStorage()));
            case DENSE_MEMORY -> DUACasBackendInstaller.install(
                    view, new DUAStorageBackend(new DUADenseMemoryCasStorage()));
            case ORDERED_KV -> {
                Path storageDirectory = sqlitePath == null ? temporaryCanonicalStore() : sqlitePath;
                DUACasBackendInstaller.install(view, new DUAStorageBackend(new DUAOrderedKvCasStorage(storageDirectory)));
            }
            case SQLITE -> {
                if (sqlitePath == null) {
                    throw new IllegalArgumentException("sqlitePath is required for SQLITE backend");
                }
                DUACasBackendInstaller.install(view, new DUAStorageBackend(new DUASqliteCasStorage(sqlitePath)));
            }
            case TIERED_SQLITE_WRITE_BACK -> {
                if (sqlitePath == null) {
                    throw new IllegalArgumentException("sqlitePath is required for TIERED_SQLITE_WRITE_BACK backend");
                }
                DUASqliteCasStorage durable = new DUASqliteCasStorage(sqlitePath);
                DUACasBackendInstaller.install(view, new DUAStorageBackend(
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
