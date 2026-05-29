package org.texttechnologylab.duui.dua.store;

public interface DUAStoreClient extends AutoCloseable {
    DUAFeatureStructureStore featureStructures();

    DUAAssociationStore associations();

    DUAGraphNavigationStore graph();

    DUAPayloadStore payloads();

    DUAExecutionGateway execution();

    @Override
    void close();
}
