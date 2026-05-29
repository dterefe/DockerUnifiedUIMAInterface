package org.texttechnologylab.duui.dua.store;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public interface DUAExecutionGateway extends AutoCloseable {
    <T> CompletableFuture<T> submit(DUAOperation<T> operation);

    static DUAExecutionGateway virtualThreads() {
        return new ExecutorBackedDUAExecutionGateway(Executors.newVirtualThreadPerTaskExecutor());
    }

    @Override
    void close();

    final class ExecutorBackedDUAExecutionGateway implements DUAExecutionGateway {
        private final ExecutorService executor;

        private ExecutorBackedDUAExecutionGateway(ExecutorService executor) {
            this.executor = Objects.requireNonNull(executor, "executor");
        }

        @Override
        public <T> CompletableFuture<T> submit(DUAOperation<T> operation) {
            Objects.requireNonNull(operation, "operation");
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return operation.execute();
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            }, executor);
        }

        @Override
        public void close() {
            executor.close();
        }
    }
}
