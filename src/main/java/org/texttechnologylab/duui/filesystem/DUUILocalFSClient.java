package org.texttechnologylab.duui.filesystem;

import org.texttechnologylab.duui.clients.handle.DUUIAddress;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Stream;

public final class DUUILocalFSClient implements DUUIDocumentClient {
    private final Path root;

    public DUUILocalFSClient() {
        this(Path.of("").toAbsolutePath());
    }

    public DUUILocalFSClient(Path root) {
        this.root = Objects.requireNonNull(root, "root").toAbsolutePath().normalize();
    }

    public Path root() {
        return root;
    }

    @Override
    public DUUIFileSystemObject proxy(DUUIAddress address) {
        Path path = path(address);
        if (Files.isDirectory(path)) {
            return directory(path);
        }
        return file(path);
    }

    @Override
    public File file(DUUIAddress address) {
        return file(path(address));
    }

    public File file(Path path) {
        return new File(resolve(path));
    }

    @Override
    public Directory directory(DUUIAddress address) {
        return directory(path(address));
    }

    public Directory directory(Path path) {
        return new Directory(resolve(path));
    }

    @Override
    public Explorer explorer(DUUIDirectory directory) {
        return new Explorer(path(directory.address()));
    }

    public Explorer explorer(Path path) {
        return new Explorer(resolve(path));
    }

    @Override
    public void shutdown() {
    }

    public final class File implements DUUIFile {
        private final Path path;

        private File(Path path) {
            this.path = Objects.requireNonNull(path, "path").toAbsolutePath().normalize();
        }

        public DUUILocalFSClient client() {
            return DUUILocalFSClient.this;
        }

        @Override
        public DUUIAddress address() {
            return DUUILocalFSClient.address(path);
        }

        @Override
        public DUUIFileMetadata metadata() {
            return DUUILocalFSClient.metadata(path);
        }

        @Override
        public String name() {
            return metadata().name();
        }

        @Override
        public DUUIStream<InputStream> read() {
            return new ReadStream(path);
        }
    }

    public final class Directory implements DUUIDirectory {
        private final Path path;

        private Directory(Path path) {
            this.path = Objects.requireNonNull(path, "path").toAbsolutePath().normalize();
        }

        public DUUILocalFSClient client() {
            return DUUILocalFSClient.this;
        }

        @Override
        public DUUIAddress address() {
            return DUUILocalFSClient.address(path);
        }

        @Override
        public DUUIFileMetadata metadata() {
            return DUUILocalFSClient.metadata(path);
        }

        @Override
        public String name() {
            return metadata().name();
        }

        @Override
        public Explorer explorer() {
            return new Explorer(path);
        }

        @Override
        public Stream<DUUIFileSystemObject> children() {
            return list(path).map(DUUILocalFSClient.this::object);
        }
    }

    public final class Explorer implements DUUIExplorer {
        private final Path directory;

        private Explorer(Path directory) {
            this.directory = Objects.requireNonNull(directory, "directory").toAbsolutePath().normalize();
        }

        public DUUILocalFSClient client() {
            return DUUILocalFSClient.this;
        }

        @Override
        public DUUIAddress address() {
            return DUUILocalFSClient.address(directory);
        }

        @Override
        public Directory directory() {
            return new Directory(directory);
        }

        @Override
        public Stream<DUUIFileSystemObject> current() {
            return list(directory).map(DUUILocalFSClient.this::object);
        }

        @Override
        public Stream<DUUIFileSystemObject> complete() {
            return current();
        }

        @Override
        public Stream<DUUIFileSystemObject> breadthFirst() {
            return breadthFirst(Integer.MAX_VALUE);
        }

        @Override
        public Stream<DUUIFileSystemObject> breadthFirst(int depth) {
            int effectiveDepth = depth < 0 ? Integer.MAX_VALUE : depth;
            return walk(directory, effectiveDepth).skip(1).map(DUUILocalFSClient.this::object);
        }

        @Override
        public Stream<DUUIFileSystemObject> search(String name) {
            return breadthFirst().filter(object -> object.name().equals(name));
        }

        @Override
        public Stream<DUUIFileSystemObject> search(Map<String, String> attributes) {
            Map<String, String> expected = attributes == null ? Map.of() : attributes;
            return breadthFirst().filter(matches(expected));
        }

        private Predicate<DUUIFileSystemObject> matches(Map<String, String> expected) {
            return object -> {
                if (expected.isEmpty()) {
                    return true;
                }
                DUUIFileMetadata metadata = object.metadata();
                Map<String, String> values = Map.ofEntries(
                        Map.entry("name", nullToEmpty(metadata.name())),
                        Map.entry("path", nullToEmpty(metadata.path())),
                        Map.entry("extension", nullToEmpty(metadata.extension())),
                        Map.entry("mediaType", nullToEmpty(metadata.mediaType())),
                        Map.entry("owner", nullToEmpty(metadata.owner())),
                        Map.entry("size", Long.toString(metadata.size())),
                        Map.entry("exists", Boolean.toString(metadata.exists())),
                        Map.entry("file", Boolean.toString(metadata.file())),
                        Map.entry("directory", Boolean.toString(metadata.directory())),
                        Map.entry("hidden", Boolean.toString(metadata.hidden())),
                        Map.entry("readable", Boolean.toString(metadata.readable())),
                        Map.entry("writable", Boolean.toString(metadata.writable())),
                        Map.entry("executable", Boolean.toString(metadata.executable()))
                );
                return expected.entrySet().stream().allMatch(entry -> Objects.equals(values.get(entry.getKey()), entry.getValue()));
            };
        }

        private String nullToEmpty(String value) {
            return value == null ? "" : value;
        }
    }

    private static final class ReadStream implements DUUIStream<InputStream> {
        private final Path path;
        private final AtomicBoolean cancelled = new AtomicBoolean(false);

        private ReadStream(Path path) {
            this.path = Objects.requireNonNull(path, "path").toAbsolutePath().normalize();
        }

        @Override
        public Stream<InputStream> stream() {
            if (cancelled.get()) {
                return Stream.empty();
            }
            return Stream.of(open(path)).onClose(this::cancel);
        }

        @Override
        public void cancel() {
            cancelled.set(true);
        }

        @Override
        public boolean cancelled() {
            return cancelled.get();
        }

        private static InputStream open(Path path) {
            try {
                return Files.newInputStream(path);
            } catch (IOException e) {
                throw new IllegalStateException("Could not open file stream: " + path, e);
            }
        }
    }

    private Path resolve(Path path) {
        Path effective = Objects.requireNonNull(path, "path");
        if (effective.isAbsolute()) {
            return effective.normalize();
        }
        return root.resolve(effective).normalize();
    }

    private DUUIFileSystemObject object(Path path) {
        Path absolute = path.toAbsolutePath().normalize();
        return Files.isDirectory(absolute) ? new Directory(absolute) : new File(absolute);
    }

    private static DUUIAddress address(Path path) {
        return DUUIAddress.parse(path.toAbsolutePath().normalize().toUri().toString());
    }

    private static Path path(DUUIAddress address) {
        return Path.of(Objects.requireNonNull(address, "address").uri()).toAbsolutePath().normalize();
    }

    private static DUUIFileMetadata metadata(Path path) {
        Path absolute = path.toAbsolutePath().normalize();
        String name = absolute.getFileName() == null ? absolute.toString() : absolute.getFileName().toString();
        BasicFileAttributes basic = basic(absolute);
        return new DUUIFileMetadata(
                name,
                absolute.toString(),
                extension(name),
                mediaType(absolute),
                basic == null ? 0L : basic.size(),
                Files.exists(absolute),
                Files.isRegularFile(absolute),
                Files.isDirectory(absolute),
                Files.isSymbolicLink(absolute),
                hidden(absolute),
                Files.isReadable(absolute),
                Files.isWritable(absolute),
                Files.isExecutable(absolute),
                basic == null ? null : basic.creationTime(),
                basic == null ? null : basic.lastModifiedTime(),
                basic == null ? null : basic.lastAccessTime(),
                owner(absolute),
                attributes(absolute)
        );
    }

    private static Stream<Path> list(Path directory) {
        try {
            return Files.list(directory);
        } catch (IOException e) {
            return Stream.empty();
        }
    }

    private static Stream<Path> walk(Path directory, int depth) {
        try {
            return Files.walk(directory, depth);
        } catch (IOException e) {
            return Stream.empty();
        }
    }

    private static BasicFileAttributes basic(Path path) {
        try {
            return Files.readAttributes(path, BasicFileAttributes.class);
        } catch (IOException e) {
            return null;
        }
    }

    private static Map<String, Object> attributes(Path path) {
        try {
            return new LinkedHashMap<>(Files.readAttributes(path, "*"));
        } catch (IOException e) {
            return Map.of();
        }
    }

    private static boolean hidden(Path path) {
        try {
            return Files.isHidden(path);
        } catch (IOException e) {
            return false;
        }
    }

    private static String owner(Path path) {
        try {
            FileOwnerAttributeView view = Files.getFileAttributeView(path, FileOwnerAttributeView.class);
            return view == null || view.getOwner() == null ? null : view.getOwner().getName();
        } catch (IOException e) {
            return null;
        }
    }

    private static String mediaType(Path path) {
        try {
            return Files.probeContentType(path);
        } catch (IOException e) {
            return null;
        }
    }

    private static String extension(String name) {
        int index = name.lastIndexOf('.');
        if (index < 0 || index == name.length() - 1) {
            return "";
        }
        return name.substring(index + 1);
    }
}
