package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.IDUUIFolderPickerApi.DUUIFolder;

public class DUUILocalDrivesDocumentHandler extends DUUILocalDocumentHandler implements IDUUIFolderPickerApi{

    String rootPath;

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

        // FolderTreeBuilder folderTreeBuilder = new FolderTreeBuilder(4);
        // DUUIFolder root = folderTreeBuilder.build(Paths.get(this.rootPath));
//        DUUIFolder root = new DUUIFolder(this.rootPath, "Files");
        Path rootDir = Paths.get(this.rootPath);
        DUUIFolder tree = FolderTreeBuilder.buildTree(rootDir);

        return tree;
    }


    public class FolderTreeBuilder {

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
                            isDir = Files.isDirectory(child);
                        } catch (SecurityException ex) {
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
                } catch (IOException ioe) {
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
    
    // public DUUIFolder filterTree(DUUIFolder root, List<Path> allowedRoots) throws IOException {
    //     // Create a fresh root for the filtered tree
    //     DUUIFolder filteredRoot = new DUUIFolder(root.id, root.name);

    //     for (DUUIFolder child : root.children) {
    //         DUUIFolder kept = filterNode(child, allowedRoots);
    //         if (kept != null) {
    //             filteredRoot.addChild(kept);
    //         }
    //     }

    //     return filteredRoot;
    // }

    // private DUUIFolder filterNode(DUUIFolder node, List<Path> allowedRoots) throws IOException {
    //     Path nodeRealPath = Paths.get(node.id)
    //             .toRealPath(LinkOption.NOFOLLOW_LINKS);

    //     // 1) Check if this node itself is under any allowed root
    //     boolean isAllowed = allowedRoots.stream().anyMatch(rootPath -> {
    //         try {
    //             return nodeRealPath.startsWith(rootPath.toRealPath());
    //         } catch (IOException e) {
    //             return false;
    //         }
    //     });

    //     // 2) Recursively filter children
    //     List<DUUIFolder> keptChildren = new ArrayList<>();
    //     for (DUUIFolder child : node.children) {
    //         DUUIFolder keptChild = filterNode(child, allowedRoots);
    //         if (keptChild != null) {
    //             keptChildren.add(keptChild);
    //         }
    //     }

    //     // 3) If this node is allowed, or has any allowed descendants, keep it
    //     if (isAllowed || !keptChildren.isEmpty()) {
    //         DUUIFolder copy = new DUUIFolder(node.id, node.name);
    //         keptChildren.forEach(copy::addChild);
    //         return copy;
    //     } else {
    //         return null;  // prune this branch entirely
    //     }
    // }
    
    /**
     * Filters the given DUUIFolder tree, keeping only the nodes that are under the allowed roots.
     * 
     * @param node The root node of the tree to filter.
     * @param allowedRoots A list of allowed root paths.
     * @return A new DUUIFolder tree containing only the allowed nodes.
     */
    private DUUIFolder filterTree(DUUIFolder root, List<Path> allowedRoots) {
        DUUIFolder out = new DUUIFolder(root.id, root.name);
        for (DUUIFolder child : root.children) {
            for (DUUIFolder match : collectAllowed(child, allowedRoots)) {
            out.addChild(match);
            }
        }
        return out;
    }

    /**
     * Recursively walks 'node' and:
     *  - if node.id is in allowed, returns a singleton list containing node (with its full subtree)
     *  - otherwise, descends into its children and returns the concatenation of their matches
     */
    private List<DUUIFolder> collectAllowed(DUUIFolder node, List<Path> allowedRoots) {
        Path p = Paths.get(node.id).toAbsolutePath().normalize();
        if (allowedRoots.contains(p)) {
          // Node itself is allowed → keep it (with its original subtree)
          return List.of(node);
        }
      
        // Otherwise, recurse and flatten
        List<DUUIFolder> results = new ArrayList<>();
        for (DUUIFolder kid : node.children) {
          results.addAll(collectAllowed(kid, allowedRoots));
        }
        return results;
      }

    // public static void main(String[] args) throws IOException {
    //     long start = System.nanoTime();

    //     // String rootPath = System.getProperty("user.home");
    //     String rootPath = "/home";
    //     DUUILocalDrivesDocumentHandler handler = new DUUILocalDrivesDocumentHandler(rootPath);

    //     DUUIFolder folderStructure = handler.getFolderStructure();
    //     List<Path> allowedRoots = new ArrayList<>();
    //     allowedRoots.add(Path.of("/home/dater/projects/duui-dterefe/DockerUnifiedUIMAInterface/docs"));

    //     folderStructure = handler.filterTree(folderStructure, allowedRoots);

    //     JsonWriterSettings jsonWriterSettings = JsonWriterSettings.builder()
    //             .indent(true)
    //             .build();
    //     String fs = new Document(folderStructure.toJson()).toJson(jsonWriterSettings);
        
    //     System.out.println(fs);

    //     long end = System.nanoTime();
    //     double seconds = (end - start) / 1_000_000_000.0;
    //     System.out.printf("Elapsed: %.3f s%n", seconds);
    // }

}
