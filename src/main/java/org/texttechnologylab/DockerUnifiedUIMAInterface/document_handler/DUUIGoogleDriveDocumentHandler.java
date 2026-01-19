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
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.folder.DUUIDirectoryNode;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.folder.FolderStructureService;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.ArrayList;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;


public class DUUIGoogleDriveDocumentHandler implements IDUUIDocumentHandler, IDUUIFolderPickerApi {

    private static final String APPLICATION_NAME = "DUUI-Gateway";
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    private static String root = "";
    private final Drive service;

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
        String parentQuery = "mimeType='application/vnd.google-apps.folder' and name='" + parentFolderName + "'";
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
            "mimeType='application/vnd.google-apps.folder' and name='" + folderName + "' and '" + maybeParentFolder.get().getId() + "' in parents";
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

    private String getFolderId(String folderName) {

        FileList result = null;

        try {
            result = service.files().list()
                    .setQ(String.format("name = '%s' and mimeType = 'application/vnd.google-apps.folder'", folderName))
                    .setFields("files(parents, id, name)")
                    .execute();

        } catch (IOException e) {
            return "";
        }

        List<File> files = result.getFiles();

        if (files.isEmpty()) return "";

        return files.get(0).getId();
    }

    private String getFileId(String fileName) {

        FileList result = null;

        try {
            result = service.files().list()
                    .setQ(String.format("name = '%s'", fileName))
                    .setFields("files(parents, id, name)")
                    .execute();

        } catch (IOException e) {
            return "";
        }

        List<File> files = result.getFiles();

        if (files.isEmpty()) return "";

        return files.get(0).getId();
    }

    private String getAllSubFolders(String parent)  {

        FileList result = null;
        try {
            result = service.files().list()
                    .setQ(String.format("'%s' in parents", parent) + " and mimeType = 'application/vnd.google-apps.folder'")
                    .setFields("files(parents, id, name)")
                    .execute();
        } catch (IOException e) {
            return String.format("'%s' in parents ", parent);
        }

        List<File> files =  result.getFiles();

        String subfolders = files.stream()
                .map(File::getId)
                .map(this::getAllSubFolders)
                .collect(Collectors.joining(" or "));

        String addOn = !files.isEmpty() ? " or " + subfolders : "";

        return String.format("'%s' in parents", parent) + addOn;
    }

    @Override
    public List<DUUIDocument> listDocuments(String path, String fileExtension, boolean recursive) throws IOException {

        String searchPath = recursive ? getAllSubFolders(path) : String.format("'%s' in parents ", path);

        return listDocuments_(searchPath, fileExtension);
    }

    @Override
    public List<DUUIDocument> listDocuments(List<String> paths, String fileExtension, boolean recursive) throws IOException {

        String searchPath = paths.stream()
                .map(path -> String.format("'%s' in parents ", path))
                .collect(Collectors.joining(" or "));

        return listDocuments_(searchPath, fileExtension);
    }

    public List<DUUIDocument> listDocuments_(String searchPath, String fileExtension) throws IOException {

        String fileExtension_ = fileExtension.isEmpty() ?
                "" : String.format("and fileExtension = '%s'", fileExtension.replace(".", ""));
        FileList result = service.files().list()
                .setQ(searchPath + " and mimeType != 'application/vnd.google-apps.folder' " + fileExtension_)
            .setFields("files(id, name, size, mimeType, isAppAuthorized)")
                .execute();

        List<File> files =  result.getFiles();

        List<DUUIDocument> documents;

        if (files == null || files.isEmpty()) {
            documents = List.of();
        } else {
            documents = files.stream()
                .filter(File::getIsAppAuthorized)
                .map(f -> {
                    DUUIDocument d = new DUUIDocument(f.getName(), f.getId(), f.getSize());
                    String mimeType = f.getMimeType();
                    if (mimeType != null && !mimeType.isBlank()) {
                        d.setMimeType(mimeType.trim());
                    }
                    return d;
                })
                .collect(Collectors.toList());
        }

        return documents;
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

    @Override
    public DUUIDirectoryNode getDirectoryTree(int maxDepth, boolean includeFiles) {
        try (ExecutorService executor = FolderStructureService.newVirtualThreadExecutor()) {
            Semaphore semaphore = FolderStructureService.newSemaphore(getDirectoryTreeMaxConcurrency());
            return getDirectoryTree0(root, "Files", 0, maxDepth, includeFiles, executor, semaphore);
        }
    }

    private DUUIDirectoryNode getDirectoryTree0(
        String folderId,
        String name,
        int depth,
        int maxDepth,
        boolean includeFiles,
        ExecutorService executor,
        Semaphore semaphore
    ) {
        if (maxDepth >= 0 && depth >= maxDepth) {
            return DUUIDirectoryNode.from(
                "gdrive",
                folderId,
                name,
                DUUIDirectoryNode.Type.DIR,
                depth,
                true,
                null,
                "application/vnd.google-apps.folder",
                0L,
                List.of()
            );
        }

        List<File> folderChildren = listChildren(folderId, true);
        List<File> fileChildren = includeFiles ? listChildren(folderId, false) : List.of();

        List<DUUIDirectoryNode> children = new ArrayList<>(folderChildren.size() + fileChildren.size());

        CompletionService<DUUIDirectoryNode> cs = new ExecutorCompletionService<>(executor);
        int submitted = 0;
        for (File f : folderChildren) {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            cs.submit(() -> {
                try {
                    return getDirectoryTree0(
                        f.getId(),
                        f.getName(),
                        depth + 1,
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
                DUUIDirectoryNode child = cs.take().get();
                if (child != null) {
                    children.add(child);
                }
            } catch (Exception ignored) {
            }
        }

        for (File f : fileChildren) {
            long mtime = f.getModifiedTime() == null ? 0L : f.getModifiedTime().getValue();
            Long size = f.getSize();
            String mimeType = f.getMimeType();
            if (mimeType != null && !mimeType.isBlank()) {
                mimeType = mimeType.trim();
            }
            children.add(
                DUUIDirectoryNode.from(
                    "gdrive",
                    f.getId(),
                    f.getName(),
                    DUUIDirectoryNode.Type.FILE,
                    depth + 1,
                    false,
                    size,
                    mimeType,
                    mtime,
                    List.of()
                )
            );
        }

        boolean hasChildren = !children.isEmpty();
        return DUUIDirectoryNode.from(
            "gdrive",
            folderId,
            name,
            DUUIDirectoryNode.Type.DIR,
            depth,
            hasChildren,
            null,
            "application/vnd.google-apps.folder",
            0L,
            children
        );
    }

    private List<File> listChildren(String parentId, boolean foldersOnly) {
        String mimeCond = foldersOnly
            ? "mimeType = 'application/vnd.google-apps.folder'"
            : "mimeType != 'application/vnd.google-apps.folder'";

        String q = String.format("'%s' in parents and trashed = false and %s", parentId, mimeCond);
        List<File> out = new ArrayList<>();

        String pageToken = null;
        do {
            FileList result;
            try {
                result = service.files().list()
                    .setQ(q)
                    .setFields("nextPageToken, files(id, name, size, mimeType, modifiedTime, isAppAuthorized)")
                    .setPageToken(pageToken)
                    .execute();
            } catch (IOException e) {
                return out;
            }

            List<File> files = result.getFiles();
            if (files != null && !files.isEmpty()) {
                out.addAll(
                    files.stream()
                        .filter(f -> Boolean.TRUE.equals(f.getIsAppAuthorized()))
                        .toList()
                );
            }
            pageToken = result.getNextPageToken();
        } while (pageToken != null && !pageToken.isBlank());

        return out;
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
