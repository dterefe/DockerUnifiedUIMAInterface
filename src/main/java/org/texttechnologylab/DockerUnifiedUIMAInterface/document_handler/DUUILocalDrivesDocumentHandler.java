package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

import org.bson.Document;
import org.bson.json.JsonWriterSettings;
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

        Path rootDir = Paths.get(this.rootPath);

        return FolderTreeBuilder.buildTree(rootDir);
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
                } catch (IOException | UncheckedIOException ioe) {
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

//     public static void main(String[] args) throws IOException {
//         long start = System.nanoTime();
//
//         // String rootPath = System.getProperty("user.home");
//         String rootPath = "/home/stud_homes";
//         DUUILocalDrivesDocumentHandler handler = new DUUILocalDrivesDocumentHandler(rootPath);
//
//         DUUIFolder folderStructure = handler.getFolderStructure();
//         List<Path> allowedRoots = new ArrayList<>();
//         allowedRoots.add(Path.of("/home/stud_homes/s0424382/projects/duui-fork/DockerUnifiedUIMAInterface/docs"));
//
//         folderStructure = handler.filterTree(folderStructure, allowedRoots);
//
//         JsonWriterSettings jsonWriterSettings = JsonWriterSettings.builder()
//                 .indent(true)
//                 .build();
//         String fs = new Document(folderStructure.toJson()).toJson(jsonWriterSettings);
//
////         System.out.println(fs);
//
//         long end = System.nanoTime();
//         double seconds = (end - start) / 1_000_000_000.0;
//         System.out.printf("Elapsed: %.3f s%n", seconds);
//     }

}
