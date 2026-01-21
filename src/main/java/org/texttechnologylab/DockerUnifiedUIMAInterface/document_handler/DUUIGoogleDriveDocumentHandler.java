package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;

import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.SerDeUtils;
import com.google.api.services.drive.model.FileList;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;


public class DUUIGoogleDriveDocumentHandler implements IDUUIDocumentHandler, IDUUIFolderPickerApi {

    private static final String APPLICATION_NAME = "DUUI-Gateway";
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    private static String root = "";
    private final Drive service;
    private static final String FOLDER_MIME = "application/vnd.google-apps.folder";

    private volatile int directoryTreeMaxConcurrency = 32;

    public DUUIGoogleDriveDocumentHandler(Credential credential) throws GeneralSecurityException, IOException {
        final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
        service = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
                .setApplicationName(APPLICATION_NAME)
                .build();
        credential.refreshToken();

        createAppFolder(service);

    }

    public static void createAppFolder(Drive service) throws IOException, IllegalStateException {
        String folderName = "DUUI";
        String parentFolderName = "Apps";

        // Search for the Docker Unified UIMA Interface folder
        String parentQuery = "mimeType='application/vnd.google-apps.folder' and name=" + literal(parentFolderName);
        FileList parentResult = service.files().list()
                .setQ(parentQuery)
                .setFields("files(id)")
                .execute();

        List<File> parentFiles = parentResult.getFiles();

        Optional<File> maybeParentFolder = parentFiles.stream().filter(File::getIsAppAuthorized).findFirst();
        File parentFolder;
        if (maybeParentFolder.isEmpty()) {
            File appsFolder = new File()
                    .setName("Apps")
                    .setMimeType("application/vnd.google-apps.folder");

            File createdAppsFolder = service.files().create(appsFolder).setFields("id").execute();
            createdAppsFolder.setIsAppAuthorized(true);

            File duuiFolder = new File()
                    .setName(folderName)
                    .setMimeType("application/vnd.google-apps.folder")
                    .setParents(Collections.singletonList(createdAppsFolder.getId()));

            File createdDuuiFolder = service.files().create(duuiFolder).setFields("id").execute();
            createdDuuiFolder.setIsAppAuthorized(true);
            root = createdDuuiFolder.getId();
            return;
        } else {
            parentFolder = maybeParentFolder.get();
        }

        String query =
            "mimeType='application/vnd.google-apps.folder' and name=" + literal(folderName) +
            " and " + literal(maybeParentFolder.get().getId()) + " in parents";
        FileList result = service.files().list()
                .setQ(query)
                .setFields("files(id, name, parents)")
                .execute();

        List<File> files = result.getFiles();

        Optional<File> maybeFolder = files.stream().filter(File::getIsAppAuthorized).findFirst();

        if (maybeFolder.isEmpty()) {

            File duuiFolder = new File()
                    .setName(folderName)
                    .setMimeType("application/vnd.google-apps.folder")
                    .setParents(Collections.singletonList(parentFolder.getId()));

            File createdDuuiFolder = service.files().create(duuiFolder).setFields("id").execute();
            createdDuuiFolder.setIsAppAuthorized(true);
            root = createdDuuiFolder.getId();
        } else {
            root = maybeFolder.get().getId();
        }
    }

    public static void main(String... args) throws IOException, GeneralSecurityException {

//        String accessToken = "";
//        GoogleCredential credential = new GoogleCredential()
//                .setAccessToken(accessToken);
//
//        DUUIGoogleDriveDocumentHandler handler = new DUUIGoogleDriveDocumentHandler(credential);

//        System.out.println("Apps/Docker Unified UIMA Interface: " + root);
//        System.out.println(handler.getFolderStructure().toJson());
//        handler.listDocuments(root, "xmi").stream()
//                .map(DUUIDocument::getName)
//                .forEach(System.out::println);
//                .map(d -> {
//                    try {
//                        return handler.readDocument(d);
//                    } catch (IOException e) {
//                        throw new RuntimeException(e);
//                    }
//                })
//                .map(d -> new String(d.getBytes(), StandardCharsets.UTF_8))
//                .forEach(System.out::println);

//        DUUIGoogleDriveDocumentHandler handler = new DUUIGoogleDriveDocumentHandler();
//        DUUIDocument doc = handler.readDocument(handler.getFileId("firstpdf.pdf"));
            //
//
//        doc.setName("secondpdf.pdf");
//
//        handler.writeDocument(doc, handler.getFolderId("first"));

//        System.out.println(handler.getFolderStructure().toJson().toString());

    }



    @Override
    public void writeDocument(DUUIDocument document, String path) throws IOException {

        File file = new File();
        file.setParents(Collections.singletonList(path));
        file.setName(document.getName());

        document.setUploadProgress(0);
        service.files().create(file, new InputStreamContent(null, document.toInputStream()))
            .execute();
        document.setUploadProgress(100);
    }


    @Override
    public DUUIDocument readDocument(String path) throws IOException {

        File file = service.files().get(path).execute();

        DUUIDocument document = new DUUIDocument(file.getName(), file.getId());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        service.files().get(path).executeMediaAndDownloadTo(out);
        byte[] data = out.toByteArray();

        document.setName(file.getName());
        document.setPath(file.getId());
        String mimeType = file.getMimeType();
        if (mimeType != null && !mimeType.isBlank()) {
            document.setMimeType(mimeType.trim());
        }
        document.setSize(data.length);
        document.setBytes(data);

        SerDeUtils.ensureCanonicalMimeType(document);

        return document;
    }

    @Override
    public List<DUUIDocument> listDocuments(String path, String fileExtension, boolean recursive) throws IOException {
        return listDocuments(List.of(path), fileExtension, recursive);
    }

    @Override
    public List<DUUIDocument> listDocuments(List<String> paths, String fileExtension, boolean recursive) throws IOException {
        if (paths == null || paths.isEmpty()) {
            return List.of();
        }

        List<String> startingFolders = new ArrayList<>();
        for (String path : paths) {
            if (path != null && !path.isBlank()) {
                startingFolders.add(path);
            }
        }

        if (startingFolders.isEmpty()) {
            return List.of();
        }

        return collectDocuments(startingFolders, normalizeExtension(fileExtension), recursive);
    }

    private List<DUUIDocument> collectDocuments(List<String> folderIds, String normalizedExtension, boolean recursive) throws IOException {
        List<DUUIDocument> documents = new ArrayList<>();
        Deque<String> foldersToBrowse = new ArrayDeque<>(folderIds);

        while (!foldersToBrowse.isEmpty()) {
            String folderId = foldersToBrowse.removeFirst();
            String query = buildListQuery(folderId, normalizedExtension, recursive);

            String pageToken = null;
            do {
                FileList result = service.files().list()
                        .setQ(query)
                        .setFields("nextPageToken, files(id, name, size, mimeType, fileExtension)")
                        .setPageToken(pageToken)
                        .execute();

                List<File> files = result.getFiles();
                if (files != null) {
                    for (File file : files) {
                        if (file == null || !file.getIsAppAuthorized()) {
                            continue;
                        }
                        boolean isFolder = "application/vnd.google-apps.folder".equals(file.getMimeType());
                        if (isFolder) {
                            if (recursive && file.getId() != null) {
                                foldersToBrowse.addLast(file.getId());
                            }
                            continue;
                        }

                        String fileExt = file.getFileExtension();
                        if (!normalizedExtension.isEmpty() 
                            && (fileExt == null || !normalizedExtension.equalsIgnoreCase(fileExt))) {
                                continue;
                        }

                        documents.add(new DUUIDocument(file.getName(), file.getId(), file.getSize()));
                    }
                }

                pageToken = result.getNextPageToken();
            } while (pageToken != null);
        }

        return documents;
    }

    private static String buildListQuery(String folderId, String normalizedExtension, boolean recursive) {
        StringBuilder query = new StringBuilder();
        query.append(literal(folderId)).append(" in parents");
        if (!recursive) {
            query.append(" and mimeType != 'application/vnd.google-apps.folder'");
        }
        if (!normalizedExtension.isBlank()) {
            query.append(" and fileExtension = ").append(literal(normalizedExtension));
        }
        return query.toString();
    }

    private static String normalizeExtension(String extension) {
        if (extension == null) {
            return "";
        }
        String trimmed = extension.trim().toLowerCase(Locale.ROOT);
        if (trimmed.startsWith(".")) {
            trimmed = trimmed.substring(1);
        }
        return trimmed;
    }

    private static String literal(String value) {
        if (value == null) {
            return "''";
        }
        return "'" + value.replace("'", "\\'") + "'";
    }

    @Override
    public DUUIFolder getFolderStructure() {

        DUUIFolder root = new DUUIFolder(DUUIGoogleDriveDocumentHandler.root, "Files");

        return getFolderStructure(root);
    }

    @Override
    public int getDirectoryTreeMaxConcurrency() {
        return directoryTreeMaxConcurrency;
    }

    @Override
    public void setDirectoryTreeMaxConcurrency(int maxConcurrency) {
        directoryTreeMaxConcurrency = maxConcurrency <= 0 ? 32 : maxConcurrency;
    }
    
    public static String namespace() {
        return "gdrive";
    }

    public static String folderMime() {
        return "application/vnd.google-apps.folder";
    }

    @Override
    public DUUIDirectoryNode getDirectoryTree(DUUIDirectoryNode node, int maxDepth, boolean includeFiles) throws Exception {
        if (node == null) {
            node = DUUIDirectoryNode.from(
                "gdrive",
                root,
                "Files",
                true,
                0,
                0L,
                FOLDER_MIME,
                0L
            );
        }

        try (ExecutorService executor = FolderStructureService.newVirtualThreadExecutor()) {
            Semaphore semaphore = FolderStructureService.newSemaphore(getDirectoryTreeMaxConcurrency());
            return getDirectoryTree(node, maxDepth, includeFiles, executor, semaphore);
        }
    }

    private DUUIDirectoryNode getDirectoryTree(
        DUUIDirectoryNode node,
        int maxDepth,
        boolean includeFiles,
        ExecutorService executor,
        Semaphore semaphore
    ) throws Exception {
        if (!node.canTraverse(maxDepth)) return node;

        List<File> children = new ArrayList<>();
        try {
            listChildren(node.path(), node, children);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            node.state(DUUIDirectoryNode.TokenState.MAYBE_MORE_PAGES);
            return node;
        }

        if (children.isEmpty()) {
            node.setNoChildren();
            return node;
        }

        CompletionService<DUUIDirectoryNode> cs = new ExecutorCompletionService<>(executor);
        int submitted = 0;
        for (File f : children) {
            boolean isDirectory = isFolder(f);
            if (!includeFiles && !isDirectory) continue;

            var childNode = DUUIDirectoryNode.from(
                "gdrive",
                f.getId(),
                f.getName(),
                isDirectory,
                node.depth() + 1,
                0L,
                isDirectory 
                    ? FOLDER_MIME 
                    : SerDeUtils.Mime.inferMimeType(f.getMimeType(), f.getName()),
                f.getModifiedTime().getValue()
            );

            node.children().add(childNode);

            if (!isDirectory) continue;

            semaphore.acquire();
            
            cs.submit(() -> {
                try {
                    return getDirectoryTree(
                        childNode,
                        maxDepth,
                        includeFiles,
                        executor,
                        semaphore
                    );
                } finally {
                    semaphore.release();
                }
            });
            submitted++;
        }

        for (int i = 0; i < submitted; i++) {
            try {
                cs.take().get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                node.state(DUUIDirectoryNode.TokenState.MAYBE_MORE_PAGES);
            }
        }

        node.state(DUUIDirectoryNode.TokenState.NO_MORE_PAGES);

        return node;
    }

    private void listChildren(String parentId, DUUIDirectoryNode node, List<File> out) throws InterruptedException {

        do {
            FileList result;

            int attempt = 0;
            int maxAttempts = 5;
            long backoffMs = 300;
            while (true) {
                try {
                    var list = service.files().list()
                        .setQ(String.format("'%s' in parents and trashed = false", parentId))
                        .setOrderBy("folder,name")
                        .setFields("nextPageToken, files(id, name, size, mimeType, modifiedTime, isAppAuthorized)");
                    if (node.hasNextToken()) list.setPageToken(node.nextToken());

                    result = list.execute();
                    node.nextToken(result.getNextPageToken());
                    break;
                } catch (com.google.api.client.googleapis.json.GoogleJsonResponseException e) {
                    int code = e.getStatusCode();
                    boolean retryable = code == 429 || code == 500 || code == 502 || code == 503 || code == 504;
                    if (!retryable || attempt >= maxAttempts) {
                        if (code == 400) {
                            String msg = e.getDetails() == null ? "" : String.valueOf(e.getDetails().getMessage());
                            if (msg.toLowerCase().contains("page token")) {
                                node.nextToken(null, DUUIDirectoryNode.TokenState.INVALID_PAGE_TOKEN.toString());
                                if (attempt > 0) return;
                                node.children().clear();
                            }
                        }
                        node.state(DUUIDirectoryNode.TokenState.MAYBE_MORE_PAGES);
                        return;
                    }
                } catch (java.net.ConnectException | java.io.InterruptedIOException e) {
                    if (attempt >= maxAttempts) {
                        node.state(DUUIDirectoryNode.TokenState.MAYBE_MORE_PAGES);
                        return;
                    }
                } catch (Exception e) {
                    node.state(DUUIDirectoryNode.TokenState.MAYBE_MORE_PAGES);
                    return;
                }

                long jitter = java.util.concurrent.ThreadLocalRandom.current().nextLong(0, 200);
                Thread.sleep(Math.min(2000, backoffMs) + jitter);

                attempt++;
                backoffMs *= 2;
            }

            List<File> files = result.getFiles();
            if (files != null && !files.isEmpty()) {
                out.addAll(files.stream().filter(f -> Boolean.TRUE.equals(f.getIsAppAuthorized())).toList());
            }

            
        } while (node.hasNextToken());
    }

    private static boolean isFolder(File f) {
        String mt = f == null ? null : f.getMimeType();
        return FOLDER_MIME.equals(mt);
    }

    public DUUIFolder getFolderStructure(DUUIFolder root) {

        FileList result = null;
        try {
            result = service.files().list()
                    .setQ(String.format("'%s' in parents", root.id) + " and mimeType = 'application/vnd.google-apps.folder'")
                    .setFields("files(parents, id, name)")
                    .execute();
        } catch (IOException e) {
            return root;
        }

        List<File> files =  result.getFiles();

        for (File file : files) {
            DUUIFolder f = new DUUIFolder(file.getId(), file.getName());
            getFolderStructure(f);
            root.addChild(f);
        }

        return root;
    }
}
