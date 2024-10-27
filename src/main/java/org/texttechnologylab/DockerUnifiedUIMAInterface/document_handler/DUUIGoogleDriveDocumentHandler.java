package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIStatus;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;


public class DUUIGoogleDriveDocumentHandler implements IDUUIDocumentHandler, IDUUIFolderPickerApi {

    private static final String APPLICATION_NAME = "DUUI-Gateway";
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    private static String root = "";
    private final Drive service;
    public DUUIGoogleDriveDocumentHandler(Credential credential) throws GeneralSecurityException, IOException {
        final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
        service = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
                .setApplicationName(APPLICATION_NAME)
                .build();

        if (!folderExists(service)) {
            File appsFolder = new File()
                    .setName("Apps")
                    .setMimeType("application/vnd.google-apps.folder");

            File createdAppsFolder = service.files().create(appsFolder).setFields("id").execute();

            File duuiFolder = new File()
                    .setName("Docker Unified UIMA Interface")
                    .setMimeType("application/vnd.google-apps.folder")
                    .setParents(Collections.singletonList(createdAppsFolder.getId()));

            File createdDuuiFolder = service.files().create(duuiFolder).setFields("id").execute();

            root = createdDuuiFolder.getId();
        }

    }

    public static boolean folderExists(Drive service) throws IOException, IllegalStateException {
        String folderName = "Docker Unified UIMA Interface";
        String parentFolderName = "Apps";

        // Search for the Docker Unified UIMA Interface folder
        String query = "mimeType='application/vnd.google-apps.folder' and name='" + folderName + "'";
        FileList result = service.files().list()
                .setQ(query)
                .setFields("files(id, name, parents)")
                .execute();

        List<File> files = result.getFiles();

        if (files.isEmpty()) {
            return false;
        }

        if (files.size() > 1) {
            throw new IllegalStateException("Multiple folders named '" + folderName + "' found. Please delete all (including the ones contained in the trash) but one.");
        }

        File duuiFolder = files.get(0);

        if (duuiFolder.getParents() == null || duuiFolder.getParents().isEmpty()) {
            throw new IllegalStateException("The folder '" + folderName + "' has no parent. This is not allowed.");
        }

        if (duuiFolder.getParents().size() > 1) {
            throw new IllegalStateException("The folder '" + folderName + "' has multiple parents. This is not allowed.");
        }

        String parentId = duuiFolder.getParents().get(0);
        File parentFolder = service.files().get(parentId)
                .setFields("id, name")
                .execute();

        if (!parentFolder.getName().equalsIgnoreCase(parentFolderName)) {
            throw new IllegalStateException("The parent folder is not '" + parentFolderName + "'. Please check the folder structure.");
        }

        root = duuiFolder.getId();

        return true;
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

        document.setStatus(DUUIStatus.OUTPUT);

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
        document.setSize(data.length);
        document.setBytes(data);

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
                .setFields("files(id, name, size)")
                .execute();

        List<File> files =  result.getFiles();

        List<DUUIDocument> documents;

        if (files == null || files.isEmpty()) {
            documents = List.of();
        } else {
            documents = files.stream()
                .map(f -> new DUUIDocument(f.getName(), f.getId(), f.getSize()))
                .collect(Collectors.toList());
        }

        return documents;
    }

    @Override
    public DUUIFolder getFolderStructure() {

        DUUIFolder root = new DUUIFolder(this.root, "Files");

        return getFolderStructure(root);
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
