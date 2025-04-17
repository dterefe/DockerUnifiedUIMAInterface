package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import org.bson.Document;
import org.bson.json.JsonWriterSettings;

import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

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

    public DUUIFolder getFolderStructure() {

        FolderTreeBuilder folderTreeBuilder = new FolderTreeBuilder(4);
        DUUIFolder root = folderTreeBuilder.build(Paths.get(this.rootPath));
//        DUUIFolder root = new DUUIFolder(this.rootPath, "Files");


        return root;
    }

    public static class FolderTask extends RecursiveTask<DUUIFolder> {
        private final Path path;

        public FolderTask(Path path) {
            this.path = path;
        }

        @Override
        protected DUUIFolder compute() {
            DUUIFolder node = new DUUIFolder(path.toString(), path.getFileName().toString());
            List<FolderTask> subTasks = new ArrayList<>();

            try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, Files::isDirectory)) {
                for (Path subdir : stream) {
                    FolderTask task = new FolderTask(subdir);
                    task.fork();                      // schedule it asynchronously
                    subTasks.add(task);
                }
                // join them and attach as children
                for (FolderTask t : subTasks) {
                    DUUIFolder child = t.join();
                    node.addChild(child);
                }
            } catch (IOException | SecurityException e) {
                // unable to read this folder; just return it empty
            }

            return node;
        }
    }

    // Utility to kick off the parallel traversal
    public static class FolderTreeBuilder {
        private final ForkJoinPool pool;

        public FolderTreeBuilder(int parallelism) {
            this.pool = new ForkJoinPool(parallelism);
        }

        public DUUIFolder build(Path rootPath) {
            return pool.invoke(new FolderTask(rootPath));
        }

        public void shutdown() {
            pool.shutdown();
        }
    }
    public DUUIFolder filterTree(DUUIFolder root, List<Path> allowedRoots) throws IOException {
        // Create a fresh root for the filtered tree
        DUUIFolder filteredRoot = new DUUIFolder(root.id, root.name);

        for (DUUIFolder child : root.children) {
            DUUIFolder kept = filterNode(child, allowedRoots);
            if (kept != null) {
                filteredRoot.addChild(kept);
            }
        }

        return filteredRoot;
    }

    private DUUIFolder filterNode(DUUIFolder node, List<Path> allowedRoots) throws IOException {
        Path nodeRealPath = Paths.get(node.id)
                .toRealPath(LinkOption.NOFOLLOW_LINKS);

        // 1) Check if this node itself is under any allowed root
        boolean isAllowed = allowedRoots.stream().anyMatch(rootPath -> {
            try {
                return nodeRealPath.startsWith(rootPath.toRealPath());
            } catch (IOException e) {
                return false;
            }
        });

        // 2) Recursively filter children
        List<DUUIFolder> keptChildren = new ArrayList<>();
        for (DUUIFolder child : node.children) {
            DUUIFolder keptChild = filterNode(child, allowedRoots);
            if (keptChild != null) {
                keptChildren.add(keptChild);
            }
        }

        // 3) If this node is allowed, or has any allowed descendants, keep it
        if (isAllowed || !keptChildren.isEmpty()) {
            DUUIFolder copy = new DUUIFolder(node.id, node.name);
            keptChildren.forEach(copy::addChild);
            return copy;
        } else {
            return null;  // prune this branch entirely
        }
    }


    public static void main(String[] args) throws IOException {
        String rootPath = System.getProperty("user.home");
        DUUILocalDrivesDocumentHandler handler = new DUUILocalDrivesDocumentHandler(rootPath);
        DUUIFolder folderStructure = handler.getFolderStructure();
        List<Path> allowedRoots = new ArrayList<>();
        allowedRoots.add(Path.of("/home/stud_homes/s0424382/Nextcloud"));
        allowedRoots.add(Path.of("/home/stud_homes/s0424382/Dokumente"));
        allowedRoots.add(Path.of("/home/stud_homes/s0424382/projects"));

        folderStructure = handler.filterTree(folderStructure, allowedRoots);

//        folderStructure.children.stream().map(f -> f.id).forEach(System.out::println);

        JsonWriterSettings jsonWriterSettings = JsonWriterSettings.builder()
                .indent(true)
                .build();
        String fs = new Document(folderStructure.toJson()).toJson(jsonWriterSettings);
        System.out.println(fs);
    }

}
