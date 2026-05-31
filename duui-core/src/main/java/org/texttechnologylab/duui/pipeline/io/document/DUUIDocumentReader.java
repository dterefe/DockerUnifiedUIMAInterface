package org.texttechnologylab.duui.pipeline.io.document;

import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.artifact.DUUIArtifactEmitter;
import org.texttechnologylab.duui.clients.handle.DUUIAddress;
import org.texttechnologylab.duui.filesystem.DUUIDirectory;
import org.texttechnologylab.duui.filesystem.DUUIDocumentClient;
import org.texttechnologylab.duui.filesystem.DUUIFile;
import org.texttechnologylab.duui.filesystem.DUUIFileSystemObject;
import org.texttechnologylab.duui.filesystem.DUUIStream;
import org.texttechnologylab.duui.pipeline.DUUIGenerator;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Stream;

public final class DUUIDocumentReader<T> implements DUUIGenerator<T> {
    private final DUUIDocumentClient client;
    private final List<DUUIAddress> sources;
    private final DUUIDocumentDeserializer<T> deserializer;
    private final Predicate<DUUIFile> filter;

    private DUUIDocumentReader(Builder<T> builder) {
        this.client = Objects.requireNonNull(builder.client, "client");
        this.sources = List.copyOf(builder.sources);
        if (sources.isEmpty()) {
            throw new IllegalArgumentException("DUUI document reader requires at least one source.");
        }
        this.deserializer = Objects.requireNonNull(builder.deserializer, "deserializer");
        this.filter = Objects.requireNonNull(builder.filter, "filter");
    }

    public static <T> Builder<T> builder(DUUIDocumentDeserializer<T> deserializer) {
        return new Builder<>(deserializer);
    }

    @Override
    public void generate(DUUIArtifactEmitter<T> emitter) throws Exception {
        for (DUUIFile file : files()) {
            DUUIStream<InputStream> read = client.read(file);
            try (Stream<InputStream> streams = read.stream()) {
                for (InputStream input : streams.toList()) {
                    try (InputStream closeable = input) {
                        emitter.emit(DUUIArtifact.of(deserializer.read(file, closeable)));
                    }
                }
            }
        }
    }

    private List<DUUIFile> files() {
        List<DUUIFile> files = new ArrayList<>();
        for (DUUIAddress source : sources) {
            collect(client.proxy(source), files);
        }
        return files.stream()
                .filter(filter)
                .sorted(Comparator.comparing(file -> file.address().value()))
                .toList();
    }

    private void collect(DUUIFileSystemObject object, List<DUUIFile> files) {
        if (object instanceof DUUIFile file) {
            files.add(file);
            return;
        }
        if (object instanceof DUUIDirectory directory) {
            try (Stream<DUUIFileSystemObject> children = client.list(directory)) {
                children.forEach(child -> collect(child, files));
            }
        }
    }

    public static final class Builder<T> {
        private final DUUIDocumentDeserializer<T> deserializer;
        private final List<DUUIAddress> sources = new ArrayList<>();
        private DUUIDocumentClient client;
        private Predicate<DUUIFile> filter = file -> true;

        private Builder(DUUIDocumentDeserializer<T> deserializer) {
            this.deserializer = Objects.requireNonNull(deserializer, "deserializer");
        }

        public Builder<T> client(DUUIDocumentClient client) {
            this.client = Objects.requireNonNull(client, "client");
            return this;
        }

        public Builder<T> source(DUUIAddress source) {
            sources.add(Objects.requireNonNull(source, "source"));
            return this;
        }

        public Builder<T> source(DUUIFileSystemObject source) {
            return source(Objects.requireNonNull(source, "source").address());
        }

        public Builder<T> filter(Predicate<DUUIFile> filter) {
            this.filter = Objects.requireNonNull(filter, "filter");
            return this;
        }

        public DUUIDocumentReader<T> build() {
            return new DUUIDocumentReader<>(this);
        }
    }
}
