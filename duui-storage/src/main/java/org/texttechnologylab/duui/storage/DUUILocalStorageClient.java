package org.texttechnologylab.duui.storage;

import org.texttechnologylab.duui.clients.handle.DUUIAddress;
import org.texttechnologylab.duui.filesystem.DUUIDocumentClient;
import org.texttechnologylab.duui.filesystem.DUUIExplorer;
import org.texttechnologylab.duui.filesystem.DUUIDirectory;
import org.texttechnologylab.duui.filesystem.DUUIFile;
import org.texttechnologylab.duui.filesystem.DUUIFileMetadata;
import org.texttechnologylab.duui.filesystem.DUUIFileSystemObject;
import org.texttechnologylab.duui.filesystem.DUUIStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Local filesystem implementation of {@link DUUIDocumentClient}.
 */
public final class DUUILocalStorageClient implements DUUIDocumentClient {

    private final Path root;

    public DUUILocalStorageClient() {
        this(Path.of("."));
    }

    public DUUILocalStorageClient(Path root) {
        this.root = Objects.requireNonNull(root, "root").toAbsolutePath().normalize();
    }

    @Override
    public DUUIFile file(DUUIAddress address) {
        return new LocalFile(resolve(address));
    }

    @Override
    public DUUIDirectory directory(DUUIAddress address) {
        return new LocalDirectory(resolve(address));
    }

    @Override
    public DUUIExplorer explorer(DUUIDirectory directory) {
        if (!(directory instanceof LocalDirectory)) {
            throw new IllegalArgumentException("Expected LocalDirectory");
        }
        return new LocalExplorer((LocalDirectory) directory);
    }

    @Override
    public DUUIFileSystemObject proxy(DUUIAddress address) {
        Path resolved = resolve(address);
        if (Files.isDirectory(resolved)) {
            return new LocalDirectory(resolved);
        }
        return new LocalFile(resolved);
    }

    @Override
    public DUUIFile write(DUUIAddress address, InputStream input) throws IOException {
        Objects.requireNonNull(input, "input");
        Path target = resolve(address);
        Path parent = target.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.copy(input, target, StandardCopyOption.REPLACE_EXISTING);
        return new LocalFile(target);
    }

    @Override
    public void shutdown() {
    }

    private Path resolve(DUUIAddress address) {
        String path = Objects.requireNonNull(address.path(), "address.path");
        if (path.startsWith("/")) path = path.substring(1);
        return root.resolve(path).normalize();
    }

    static DUUIFileMetadata metadata(Path path) {
        Path abs = path.toAbsolutePath().normalize();
        String name = abs.getFileName() != null ? abs.getFileName().toString() : "";
        boolean exists = Files.exists(abs);
        boolean isDir = Files.isDirectory(abs);
        boolean isFile = Files.isRegularFile(abs);
        String ext = "";
        if (name.contains(".") && isFile) {
            ext = name.substring(name.lastIndexOf('.'));
        }
        FileTime created = FileTime.from(Instant.EPOCH);
        FileTime modified = FileTime.from(Instant.EPOCH);
        FileTime accessed = FileTime.from(Instant.EPOCH);
        try {
            BasicFileAttributes attr = Files.readAttributes(abs, BasicFileAttributes.class);
            created = attr.creationTime();
            modified = attr.lastModifiedTime();
            accessed = attr.lastAccessTime();
        } catch (IOException ignored) { }
        long size = isFile ? abs.toFile().length() : 0;
        boolean readable = Files.isReadable(abs);
        boolean writable = Files.isWritable(abs);
        boolean executable = Files.isExecutable(abs);
        boolean symlink = Files.isSymbolicLink(abs);
        return new DUUIFileMetadata(name, abs.toString(), ext, "application/octet-stream",
                size, exists, isFile, isDir, symlink, false, readable, writable, executable,
                created, modified, accessed, "unknown", Map.of());
    }

    class LocalFile implements DUUIFile {
        final Path path;

        LocalFile(Path path) {
            this.path = path;
        }

        @Override
        public DUUIFileMetadata metadata() {
            return DUUILocalStorageClient.metadata(path);
        }

        @Override
        public String name() {
            return metadata().name();
        }

        @Override
        public DUUIAddress address() {
            return new DUUIAddress("local", "file", root.relativize(path).toString(), null, null);
        }

        @Override
        public DUUIStream<InputStream> read() {
            return new LocalReadStream(path);
        }
    }

    class LocalDirectory implements DUUIDirectory {
        final Path path;

        LocalDirectory(Path path) {
            this.path = path;
        }

        @Override
        public DUUIFileMetadata metadata() {
            return DUUILocalStorageClient.metadata(path);
        }

        @Override
        public String name() {
            return metadata().name();
        }

        @Override
        public DUUIAddress address() {
            return new DUUIAddress("local", "directory", root.relativize(path).toString(), null, null);
        }

        @Override
        public DUUIExplorer explorer() {
            return new LocalExplorer(this);
        }

        @Override
        public Stream<DUUIFileSystemObject> children() {
            return explorer().current();
        }
    }

    class LocalExplorer implements DUUIExplorer {
        private final LocalDirectory directory;

        LocalExplorer(LocalDirectory directory) {
            this.directory = directory;
        }

        @Override
        public DUUIDirectory directory() {
            return directory;
        }

        @Override
        public DUUIAddress address() {
            return directory.address();
        }

        @Override
        public Stream<DUUIFileSystemObject> current() {
            Path dir = directory.path;
            if (!Files.isDirectory(dir)) return Stream.empty();
            try {
                return Files.list(dir).map(p -> {
                    if (Files.isDirectory(p)) return (DUUIFileSystemObject) new LocalDirectory(p);
                    return (DUUIFileSystemObject) new LocalFile(p);
                });
            } catch (IOException e) {
                return Stream.empty();
            }
        }

        @Override
        public Stream<DUUIFileSystemObject> complete() {
            return current();
        }

        @Override
        public Stream<DUUIFileSystemObject> breadthFirst() {
            return current();
        }

        @Override
        public Stream<DUUIFileSystemObject> breadthFirst(int depth) {
            return current();
        }

        @Override
        public Stream<DUUIFileSystemObject> search(String name) {
            return current().filter(o -> o.name().contains(name));
        }

        @Override
        public Stream<DUUIFileSystemObject> search(Map<String, String> attributes) {
            return current();
        }
    }

    static class LocalReadStream implements DUUIStream<InputStream> {
        private final Path path;
        private volatile boolean cancelled;

        LocalReadStream(Path path) {
            this.path = path;
        }

        @Override
        public Stream<InputStream> stream() {
            try {
                return Stream.of(Files.newInputStream(path));
            } catch (IOException e) {
                return Stream.empty();
            }
        }

        @Override
        public void cancel() { cancelled = true; }

        @Override
        public boolean cancelled() { return cancelled; }
    }
}
