package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUIDocument;

public interface IDUUIProcess extends AutoCloseable {

    void onEvent(DUUIEvent event);

    void addObserver(IDUUIProcessObserver observer);

    void removeObserver(IDUUIProcessObserver observer);

    List<DUUIEvent> drainEvents();

    boolean isFinished();

    boolean isTerminal();

    int progress();

    DUUIDocument addDocument(DUUIDocument document);

    void addDocuments(Collection<DUUIDocument> documents);

    Set<DUUIDocument> getDocuments();

    Set<String> getDocumentPaths();

    DUUIDocument findDocumentByPath(String path);

    int getDocumentCount();

    void clearDocuments();

    @Deprecated
    Map<String, String> getPipelineStatus();

    @Deprecated
    Map<String, DUUIStatus> getPipelineStatusEnum();

    @Deprecated
    void setPipelineStatus(String name, DUUIStatus status);

    @Deprecated
    void setPipelineStatus(String name, String status);

    Exception getFatalRunError();

    boolean setFatalRunErrorIfAbsent(Exception error);

    void clearFatalRunError();

    void registerWorker(Thread thread);

    List<Thread> workers();

    void clearWorkerThreads();

    IDUUIProcess reset();

    void shutdown();

    @Override
    default void close() {
        shutdown();
    }

    IDUUIProcess setProfiler(Path prometheusProfilerOutputPath, String runIdentifier);

    boolean hasProfiler();
}
