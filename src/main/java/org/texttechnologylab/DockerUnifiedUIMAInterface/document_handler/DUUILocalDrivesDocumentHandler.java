package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
// import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUILocalDrivesDocumentHandler.FolderTreeBuilder;

import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.IDUUIFolderPickerApi.DUUIFolder;

public class DUUILocalDrivesDocumentHandler extends DUUILocalDocumentHandler implements IDUUIFolderPickerApi {

    String rootPath;

    private volatile int directoryTreeMaxConcurrency = 32;

    public DUUILocalDrivesDocumentHandler(String rootPath) {

        try {
            Path path = Paths.get(rootPath);
            if (!Files.exists(path) && !Files.isDirectory(path))
                throw new InvalidPathException(rootPath, "The path does not exist.");
        } catch (InvalidPathException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("[DUUILocalDrivesDocumentHandler]: An error occurred while initializing root path: ", e);
        }

        this.rootPath = rootPath;
    }

    @Override
    public DUUIFolder getFolderStructure() {

        Path rootDir = Paths.get(this.rootPath);

        return FolderTreeBuilder.buildTree(rootDir);
    }

    @Override
    public int getDirectoryTreeMaxConcurrency() {
        return directoryTreeMaxConcurrency;
    }

    @Override
    public void setDirectoryTreeMaxConcurrency(int maxConcurrency) {
        directoryTreeMaxConcurrency = maxConcurrency <= 0 ? 32 : maxConcurrency;
    }

    @Override
    public DUUIDirectoryNode getDirectoryTree(int maxDepth, boolean includeFiles) {
        Path rootDir = Paths.get(this.rootPath);
        return FolderStructureService.buildLocalTree(
            rootDir,
            maxDepth,
            includeFiles,
            p -> {
                try {
                    Path fn = p.getFileName();
                    if (fn != null && fn.toString().startsWith(".")) {
                        return false;
                    }
                    return Files.isReadable(p) && Files.isWritable(p);
                } catch (Exception e) {
                    return false;
                }
            }
        );
    }

    @Override
    public DUUIDirectoryNode getDirectoryTree(DUUIDirectoryNode node, int maxDepth, boolean includeFiles) {
        if (node == null) {
            return getDirectoryTree(maxDepth, includeFiles);
        }

        return FolderStructureService.buildLocalTree(node, maxDepth, includeFiles, this::defaultLocalFilter);
    }

    private boolean defaultLocalFilter(Path p) {
        try {
            Path fn = p.getFileName();
            if (fn != null && fn.toString().startsWith(".")) {
                return false;
            }
            return Files.isReadable(p) && Files.isWritable(p);
        } catch (Exception e) {
            return false;
        }
    }

    public static class FolderTreeBuilder {

        /**
         * Traverse starting at 'rootDir' and return the corresponding DUUIFolder tree.
         */
        public static DUUIFolder buildTree(Path rootDir) {
            // submit root task to common pool
            return ForkJoinPool.commonPool().invoke(new FolderTask(rootDir));
        }

        private static class FolderTask extends RecursiveTask<DUUIFolder> {
            private final Path dir;

            FolderTask(Path dir) {
                this.dir = dir;
            }

            @Override
            protected DUUIFolder compute() {
                // id = absolute path, name = last segment
                String id   = dir.toAbsolutePath().toString();
                String name = dir.getFileName() != null
                            ? dir.getFileName().toString()
                            : dir.toString();

                DUUIFolder folder = new DUUIFolder(id, name);
                List<FolderTask> subTasks = new ArrayList<>();

                // list entries, skip failures
                try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {
                    for (Path child : ds) {
                        boolean isDir;
                        try {
                            // default isDirectory follows symlinks
                            isDir = Files.isDirectory(child)
                                    && Files.isReadable(child)
                                    && Files.isWritable(child)
                                    && !child.getFileName().startsWith(".");
                        } catch (Exception ex) {
                            // skip paths we can’t check
                            continue;
                        }

                        if (isDir) {
                            // fork a task for each subdirectory
                            FolderTask task = new FolderTask(child);
                            task.fork();
                            subTasks.add(task);
                        }
                    }
                } catch (Exception ioe) {
                    // could not open this directory—just skip it
                }

                // join results and add as children
                for (FolderTask t : subTasks) {
                    DUUIFolder childFolder = t.join();
                    folder.addChild(childFolder);
                }

                return folder;
            }
        }
    }

    /**
     * Filters the given DUUIFolder tree, keeping only the nodes that are under the allowed roots.
     * 
     * @param root The root node of the tree to filter.
     * @param allowedRoots A list of allowed root paths.
     * @return A new DUUIFolder tree containing only the allowed nodes.
     */
    public DUUIFolder filterTree(DUUIFolder root, List<Path> allowedRoots) {
        if (root == null || allowedRoots == null || allowedRoots.isEmpty()) {
            return root;
        }

        DUUIFolder out = new DUUIFolder(root.id, root.name);
        Path p = Paths.get(root.id).toAbsolutePath();

        // System.out.println(p.toAbsolutePath() + " " + allowedRoots.stream().allMatch(ar -> 
        //        !p.toAbsolutePath().startsWith(ar.toAbsolutePath()) 
        //     && !ar.toAbsolutePath().startsWith(p.toAbsolutePath())));
        if (allowedRoots.stream().allMatch(ar -> 
               !p.toAbsolutePath().startsWith(ar.toAbsolutePath()) 
            && !ar.toAbsolutePath().startsWith(p.toAbsolutePath()))) {
            // Node is not allowed → skip it
            return null;
        }

        if (allowedRoots.stream().anyMatch(ar -> p.equals(ar.toAbsolutePath()))) {
            // Node itself is allowed → keep it (with its original subtree)
            return root;
        }

        for (DUUIFolder child : root.children) {
            var subtree = filterTree(child, allowedRoots);
            if (subtree == null) {
                continue;
            }

            out.addChild(subtree);
        }
        return out;
    }

}
