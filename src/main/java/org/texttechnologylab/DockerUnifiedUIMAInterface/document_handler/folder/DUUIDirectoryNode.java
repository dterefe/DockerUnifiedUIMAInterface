package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.folder;

import org.bson.Document;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public record DUUIDirectoryNode(
    String id,
    String name,
    String path,
    Type type,
    int depth,
    boolean hasChildren,
    Long size,
    String mimeType,
    long mtime,
    List<DUUIDirectoryNode> children
) {

    public enum Type { DIR, FILE }

    public DUUIDirectoryNode {
        if (id == null || id.isBlank()) throw new IllegalArgumentException("id");
        if (name == null) throw new IllegalArgumentException("name");
        if (path == null || path.isBlank()) throw new IllegalArgumentException("path");
        if (type == null) throw new IllegalArgumentException("type");
        children = List.copyOf(children == null ? List.of() : children);
    }

    public Document toDocument() {
        Document out = new Document()
            .append("id", id)
            .append("name", name)
            .append("path", path)
            .append("type", type.name())
            .append("depth", depth)
            .append("hasChildren", hasChildren);
        if (size != null) out.append("size", size);
        if (mimeType != null) out.append("mimeType", mimeType);
        out.append("mtime", mtime);
        out.append("children", children.stream().map(DUUIDirectoryNode::toDocument).collect(Collectors.toList()));
        return out;
    }

    public static DUUIDirectoryNode from(
        Path entry,
        Type type,
        int depth,
        boolean hasChildren,
        Long size,
        String mimeType,
        long mtime,
        List<DUUIDirectoryNode> children
    ) {
        String raw = normalizeFs(entry);
        return from(
            "local",
            raw,
            entry.getFileName() == null ? raw : entry.getFileName().toString(),
            type,
            depth,
            hasChildren,
            size,
            mimeType,
            mtime,
            children
        );
    }

    public static DUUIDirectoryNode from(
        String namespace,
        String raw,
        String name,
        Type type,
        int depth,
        boolean hasChildren,
        Long size,
        String mimeType,
        long mtime,
        List<DUUIDirectoryNode> children
    ) {
        String ns = (namespace == null || namespace.isBlank()) ? "default" : namespace.trim();
        String canon = ns + ":" + normalizeRaw(raw);

        return new DUUIDirectoryNode(
            stableId(canon),
            (name == null || name.isEmpty()) ? canon : name,
            canon,
            type,
            depth,
            hasChildren,
            size,
            mimeType,
            mtime,
            children
        );
    }

    public DUUIDirectoryNode pruneToDepth(int maxDepth) {
        if (maxDepth < 0) return this;
        if (depth >= maxDepth) {
            return new DUUIDirectoryNode(id, name, path, type, depth, hasChildren, size, mimeType, mtime, List.of());
        }
        List<DUUIDirectoryNode> pruned = children.stream()
            .map(c -> c.pruneToDepth(maxDepth))
            .collect(Collectors.toList());
        return new DUUIDirectoryNode(id, name, path, type, depth, !pruned.isEmpty(), size, mimeType, mtime, pruned);
    }

    public DUUIDirectoryNode filter(Predicate<Path> predicate) {
        if (predicate == null) return this;
        Path p = Paths.get(stripNamespace(path));
        if (!predicate.test(p)) return null;

        List<DUUIDirectoryNode> filtered = children.stream()
            .map(c -> c.filter(predicate))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

        return new DUUIDirectoryNode(id, name, path, type, depth, !filtered.isEmpty(), size, mimeType, mtime, filtered);
    }

    public static String normalizeFs(Path p) {
        return p.toAbsolutePath().normalize().toString().replace('\\', '/');
    }

    public static String normalizeRaw(String raw) {
        String s = raw == null ? "" : raw.trim().replace('\\', '/');
        if (s.isEmpty()) return "/";

        boolean looksOpaque = !s.contains("/");
        if (looksOpaque) return s;

        s = s.replaceAll("/+", "/");
        if (!s.startsWith("/")) s = "/" + s;
        if (s.length() > 1 && s.endsWith("/")) s = s.substring(0, s.length() - 1);
        return s;
    }

    public static String stableId(String normalizedPath) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] dig = md.digest(normalizedPath.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(dig);
        } catch (Exception e) {
            return Integer.toHexString(normalizedPath.hashCode());
        }
    }

    private static String stripNamespace(String canon) {
        if (canon == null) return "";
        int idx = canon.indexOf(':');
        if (idx < 0) return canon;
        return canon.substring(idx + 1);
    }
}
