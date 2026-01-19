package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.folder;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.function.Predicate;
import java.util.stream.Stream;

public final class FolderStructureService {

    private FolderStructureService() {
    }

    public static DUUIDirectoryNode buildLocalTree(
        Path root,
        int maxDepth,
        boolean includeFiles,
        Predicate<Path> filter
    ) {
        Objects.requireNonNull(root, "root");
        return buildLocalTree0(root, 0, maxDepth, includeFiles, filter);
    }

    private static DUUIDirectoryNode buildLocalTree0(
        Path path,
        int depth,
        int maxDepth,
        boolean includeFiles,
        Predicate<Path> filter
    ) {
        if (filter != null && !filter.test(path)) {
            return null;
        }

        boolean isDir = Files.isDirectory(path);
        DUUIDirectoryNode.Type type = isDir ? DUUIDirectoryNode.Type.DIR : DUUIDirectoryNode.Type.FILE;

        long mtime = 0L;
        Long size = null;
        String mimeType = null;

        try {
            BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            mtime = Optional.ofNullable(attrs.lastModifiedTime()).map(t -> t.toMillis()).orElse(0L);
            if (!attrs.isDirectory()) {
                size = attrs.size();
                try {
                    mimeType = Files.probeContentType(path);
                } catch (IOException ignored) {
                }
            } else {
                mimeType = "inode/directory";
            }
        } catch (IOException ignored) {
        }

        if (!isDir) {
            return DUUIDirectoryNode.from(path, type, depth, false, size, mimeType, mtime, List.of());
        }

        if (maxDepth >= 0 && depth >= maxDepth) {
            return DUUIDirectoryNode.from(path, type, depth, true, null, mimeType, mtime, List.of());
        }

        List<DUUIDirectoryNode> children = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(path)) {
            for (Path child : ds) {
                try {
                    if (Files.isDirectory(child) || includeFiles) {
                        DUUIDirectoryNode c = buildLocalTree0(child, depth + 1, maxDepth, includeFiles, filter);
                        if (c != null) {
                            children.add(c);
                        }
                    }
                } catch (Exception ignored) {
                }
            }
        } catch (IOException ignored) {
        }

        boolean hasChildren = !children.isEmpty();
        return DUUIDirectoryNode.from(path, type, depth, hasChildren, null, mimeType, mtime, children);
    }

    /**
     * Breadth-first paging over a tree.
     */
    public static Stream<List<DUUIDirectoryNode>> bfs(DUUIDirectoryNode root, int pageSize) {
        if (root == null) return Stream.of(List.of());
        int size = Math.max(1, pageSize);

        Deque<DUUIDirectoryNode> q = new ArrayDeque<>();
        q.add(root);

        List<List<DUUIDirectoryNode>> pages = new ArrayList<>();
        List<DUUIDirectoryNode> current = new ArrayList<>(size);

        while (!q.isEmpty()) {
            DUUIDirectoryNode n = q.removeFirst();
            current.add(n);
            if (current.size() >= size) {
                pages.add(List.copyOf(current));
                current.clear();
            }

            for (DUUIDirectoryNode c : n.children()) {
                q.addLast(c);
            }
        }

        if (!current.isEmpty()) {
            pages.add(List.copyOf(current));
        }

        return pages.stream();
    }

    public static ExecutorService newVirtualThreadExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    public static Semaphore newSemaphore(int maxConcurrency) {
        return new Semaphore(Math.max(1, maxConcurrency));
    }
}
