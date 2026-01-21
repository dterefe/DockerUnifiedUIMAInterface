package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class FolderStructureService {

    private static final ExecutorService SHARED_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

    private FolderStructureService() {
    }

    public static DUUIDirectoryNode buildLocalTree(
        Path root,
        int maxDepth,
        boolean includeFiles,
        Predicate<Path> filter
    ) {
        Objects.requireNonNull(root, "root");
        return buildLocalTree(create(root, 0), maxDepth, includeFiles, filter);
    }

    public static DUUIDirectoryNode buildLocalTree(
        DUUIDirectoryNode node,
        int maxDepth,
        boolean includeFiles,
        Predicate<Path> filter
    ) {
        if (!node.canTraverse(maxDepth)) return node;

        if (filter != null && !filter.test(Path.of(node.path()))) {
            return null;
        }


        try (DirectoryStream<Path> ds = Files.newDirectoryStream(Path.of(node.path()))) {
            for (Path child : ds) {
                if (!Files.isDirectory(child) && !includeFiles) continue;

                DUUIDirectoryNode c = create(child, node.depth() + 1);
                try {
                    DUUIDirectoryNode f = buildLocalTree(c, maxDepth, includeFiles, filter);
                    if (f != null) { 
                        node.children().add(c); 
                    } 
                } catch (Exception ignored) {
                    c.state(DUUIDirectoryNode.TokenState.MAYBE_MORE_PAGES);
                }
            }
        } catch (IOException ignored) {
            node.state(DUUIDirectoryNode.TokenState.MAYBE_MORE_PAGES);
        }

        return node;
    }

    protected static DUUIDirectoryNode create(Path p, int depth) {

        boolean isDir = Files.isDirectory(p);
        long mtime = 0L;
        Long size = null;
        String mimeType = null;

        try {
            BasicFileAttributes attrs = Files.readAttributes(p, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            mtime = Optional.ofNullable(attrs.lastModifiedTime()).map(t -> t.toMillis()).orElse(0L);
            if (!attrs.isDirectory()) {
                size = attrs.size();
                try {
                    mimeType = Files.probeContentType(p);
                } catch (IOException ignored) {
                }
            } else {
                mimeType = "inode/directory";
            }
        } catch (IOException ignored) {
        }

        return DUUIDirectoryNode.from(
            p, 
            isDir, 
            depth, 
            size, 
            mimeType, 
            mtime
        );
    }

    /**
     * Breadth-first paging over a tree.
     */
    public static Stream<List<DUUIDirectoryNode>> bfs(
        IDUUIFolderPickerApi handler,
        DUUIDirectoryNode root,
        int stepSize,
        boolean includeFiles
    ) {
        if (handler == null) throw new IllegalArgumentException("Handler must not be null");
        if (root == null) return Stream.empty();

        Semaphore semaphore = newSemaphore(handler.getDirectoryTreeMaxConcurrency());

        Spliterator<List<DUUIDirectoryNode>> sp =
            new Spliterators.AbstractSpliterator<>(
                Long.MAX_VALUE,
                Spliterator.ORDERED | Spliterator.NONNULL
            ) {

            boolean started = false;
            int depth = 0;
            List<DUUIDirectoryNode> frontier = List.of();

            @Override
            public boolean tryAdvance(Consumer<? super List<DUUIDirectoryNode>> action) {
                if (!started) {
                    started = true;
                    frontier = List.of(root);
                }

                if (frontier.isEmpty()) return false;

                int submitted = 0;
                try {
                    CompletionService<DUUIDirectoryNode> cs = new ExecutorCompletionService<>(SHARED_EXECUTOR);
                    for (var parent : frontier) {
                        try {
                            semaphore.acquire();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        cs.submit(() -> {
                            try {
                                return handler.getDirectoryTree(
                                    parent,
                                    parent.depth() + stepSize,
                                    includeFiles
                                );
                            } finally {
                                semaphore.release();
                            }
                        });
                        submitted++;
                    }

                    for (int i = 0; i < submitted; i++) {
                        cs.take().get();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                }

                var all = frontier
                    .stream()
                    .flatMap(p -> p.children().stream())
                    .toList();
                
                action.accept(all);
                frontier = all.stream().filter(DUUIDirectoryNode::isDirectory).toList();
                depth++;
                return true;
            }
        };

        return StreamSupport.stream(sp, false);
    }



    public static ExecutorService newVirtualThreadExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    public static Semaphore newSemaphore(int maxConcurrency) {
        return new Semaphore(Math.max(1, maxConcurrency));
    }
}
