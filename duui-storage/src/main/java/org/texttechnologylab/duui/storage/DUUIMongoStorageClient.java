package org.texttechnologylab.duui.storage;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.texttechnologylab.duui.clients.handle.DUUIAddress;
import org.texttechnologylab.duui.filesystem.DUUIDocumentClient;
import org.texttechnologylab.duui.filesystem.DUUIExplorer;
import org.texttechnologylab.duui.filesystem.DUUIDirectory;
import org.texttechnologylab.duui.filesystem.DUUIFile;
import org.texttechnologylab.duui.filesystem.DUUIFileMetadata;
import org.texttechnologylab.duui.filesystem.DUUIFileSystemObject;
import org.texttechnologylab.duui.filesystem.DUUIStream;

import java.io.InputStream;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * MongoDB-backed implementation of {@link DUUIDocumentClient}.
 */
public final class DUUIMongoStorageClient implements DUUIDocumentClient {

    private final MongoClient mongoClient;
    private final MongoDatabase database;
    private final MongoCollection<Document> collection;
    private final boolean ownsClient;

    public DUUIMongoStorageClient() {
        this("mongodb://localhost:27017", "duui_storage", "documents");
    }

    public DUUIMongoStorageClient(String connectionString, String databaseName, String collectionName) {
        this(MongoClients.create(Objects.requireNonNull(connectionString, "connectionString")),
                databaseName, collectionName, true);
    }

    public DUUIMongoStorageClient(MongoClient mongoClient, String databaseName, String collectionName) {
        this(mongoClient, databaseName, collectionName, false);
    }

    private DUUIMongoStorageClient(MongoClient mongoClient, String databaseName,
            String collectionName, boolean ownsClient) {
        this.mongoClient = Objects.requireNonNull(mongoClient, "mongoClient");
        this.database = mongoClient.getDatabase(Objects.requireNonNull(databaseName, "databaseName"));
        this.collection = database.getCollection(Objects.requireNonNull(collectionName, "collectionName"));
        this.ownsClient = ownsClient;
    }

    @Override
    public DUUIFile file(DUUIAddress address) {
        return new MongoFile(addressPath(address));
    }

    @Override
    public DUUIDirectory directory(DUUIAddress address) {
        return new MongoDirectory(addressPath(address));
    }

    @Override
    public DUUIExplorer explorer(DUUIDirectory directory) {
        if (!(directory instanceof MongoDirectory)) {
            throw new IllegalArgumentException("Expected MongoDirectory");
        }
        return new MongoExplorer((MongoDirectory) directory);
    }

    @Override
    public DUUIFileSystemObject proxy(DUUIAddress address) {
        return new MongoFile(addressPath(address));
    }

    @Override
    public void shutdown() {
        if (ownsClient && mongoClient != null) {
            mongoClient.close();
        }
    }

    private static String addressPath(DUUIAddress address) {
        String path = Objects.requireNonNull(address.path(), "address.path");
        return path.startsWith("/") ? path.substring(1) : path;
    }

    private Document findDocument(String id) {
        return collection.find(Filters.eq("_id", id)).first();
    }

    static DUUIFileMetadata mongoMetadata(String id, Document doc) {
        boolean exists = doc != null;
        org.bson.types.Binary data = doc != null ? doc.get("data", org.bson.types.Binary.class) : null;
        long size = data != null ? data.length() : 0;
        String name = id.contains("/") ? id.substring(id.lastIndexOf('/') + 1) : id;
        FileTime now = FileTime.from(Instant.now());
        return new DUUIFileMetadata(name, id, "", "application/octet-stream",
                size, exists, true, false, false, false, true, true, false,
                now, now, now, "mongo", Map.of());
    }

    static DUUIFileMetadata mongoDirMetadata(String prefix) {
        String name = prefix.endsWith("/") && prefix.length() > 1
                ? prefix.substring(0, prefix.length() - 1) : prefix;
        name = name.contains("/") ? name.substring(name.lastIndexOf('/') + 1) : name;
        FileTime now = FileTime.from(Instant.now());
        return new DUUIFileMetadata(name, prefix, "", "application/octet-stream",
                0, true, false, true, false, false, true, true, false,
                now, now, now, "mongo", Map.of());
    }

    class MongoFile implements DUUIFile {
        final String id;

        MongoFile(String id) {
            this.id = Objects.requireNonNull(id, "id");
        }

        @Override
        public DUUIFileMetadata metadata() {
            return mongoMetadata(id, findDocument(id));
        }

        @Override
        public String name() {
            return metadata().name();
        }

        @Override
        public DUUIAddress address() {
            return new DUUIAddress("mongo", "file", id, null, null);
        }

        @Override
        public DUUIStream<InputStream> read() {
            return new MongoReadStream(id);
        }
    }

    class MongoDirectory implements DUUIDirectory {
        final String prefix;

        MongoDirectory(String prefix) {
            this.prefix = prefix.endsWith("/") ? prefix : prefix + "/";
        }

        @Override
        public DUUIFileMetadata metadata() {
            return mongoDirMetadata(prefix);
        }

        @Override
        public String name() {
            return metadata().name();
        }

        @Override
        public DUUIAddress address() {
            return new DUUIAddress("mongo", "directory", prefix, null, null);
        }

        @Override
        public DUUIExplorer explorer() {
            return new MongoExplorer(this);
        }

        @Override
        public Stream<DUUIFileSystemObject> children() {
            return explorer().current();
        }
    }

    class MongoExplorer implements DUUIExplorer {
        private final MongoDirectory directory;

        MongoExplorer(MongoDirectory directory) {
            this.directory = directory;
        }

        @Override
        public DUUIDirectory directory() {
            return directory;
        }

        @Override
        public DUUIAddress address() {
            return directory.address();
        }

        @Override
        public Stream<DUUIFileSystemObject> current() {
            var filter = Filters.regex("_id", "^" + java.util.regex.Pattern.quote(directory.prefix));
            return StreamSupport.stream(collection.find(filter).spliterator(), false)
                    .map(doc -> (DUUIFileSystemObject) new MongoFile(doc.getString("_id")));
        }

        @Override
        public Stream<DUUIFileSystemObject> complete() { return current(); }

        @Override
        public Stream<DUUIFileSystemObject> breadthFirst() { return current(); }

        @Override
        public Stream<DUUIFileSystemObject> breadthFirst(int depth) { return current(); }

        @Override
        public Stream<DUUIFileSystemObject> search(String name) {
            return current().filter(o -> o.name().contains(name));
        }

        @Override
        public Stream<DUUIFileSystemObject> search(Map<String, String> attributes) {
            return current();
        }
    }

    class MongoReadStream implements DUUIStream<InputStream> {
        private final String id;
        private volatile boolean cancelled;

        MongoReadStream(String id) {
            this.id = id;
        }

        @Override
        public Stream<InputStream> stream() {
            Document doc = findDocument(id);
            if (doc == null) return Stream.empty();
            org.bson.types.Binary data = doc.get("data", org.bson.types.Binary.class);
            if (data == null) return Stream.empty();
            return Stream.of(new java.io.ByteArrayInputStream(data.getData()));
        }

        @Override
        public void cancel() { cancelled = true; }

        @Override
        public boolean cancelled() { return cancelled; }
    }
}
