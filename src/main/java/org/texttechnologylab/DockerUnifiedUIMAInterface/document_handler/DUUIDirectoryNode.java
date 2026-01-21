package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public record DUUIDirectoryNode(
    String id,
    String name,
    String path,
    Type type,
    int depth,
    Long size,
    String mimeType,
    long mtime,
    Paging paging
) {

    public enum TokenState { 
        NO_MORE_PAGES, MAYBE_MORE_PAGES, UNKNOWN, INVALID_PAGE_TOKEN;

        public static TokenState state(String token) {
            return StringUtils.isNotBlank(token)
                ? UNKNOWN
                : NO_MORE_PAGES;
        }

    }

    public enum Type { DIR, FILE }

    public record PageToken(String[] arr) {

        public PageToken(Document node) {
            this(
                node.getString("nextToken"), 
                node.getString("tokenState") 
            );
        }

        public PageToken(String pageToken, String state) {
            this(
                new String[]{
                        StringUtils.trimToNull(pageToken),
                        StringUtils.trimToNull(state)
                    }
            );
        }

        public PageToken {
            arr = arr == null || arr.length != 2 
                ? new String[]{null, null} 
                : arr;
            arr[0] = StringUtils.trimToNull(arr[0]);
            arr[1] = StringUtils.trimToNull(arr[1]);
        }

        private String pageToken() {
            return arr[0];
        }

        private String state() {
            return arr[1];
        }

        private void pageToken(String newValue) {
            arr[0] = StringUtils.trimToNull(newValue);
        }

        private void state(String newValue) {
            arr[1] = StringUtils.trimToNull(newValue);
        }
    }

    public record Paging(List<DUUIDirectoryNode> children, PageToken nextToken) {

        public Paging(Document node) {
            this(
                node.getList("children", Document.class)
                    .stream()
                    .map(DUUIDirectoryNode::new)
                    .toList(),
                new PageToken(node)
            );
        }

        public Paging {
            children = (children == null) ? new ArrayList<>() : children instanceof ArrayList ? children : new ArrayList<>(children);
            nextToken = nextToken == null ? new PageToken(null, TokenState.UNKNOWN.toString()) : nextToken;
        }

        public Paging(boolean isDirectory) {
            this(
                new ArrayList<>(), 
                new PageToken(
                    null,
                    isDirectory 
                    ? TokenState.MAYBE_MORE_PAGES.toString()
                    : TokenState.NO_MORE_PAGES.toString()
                )
            );
        }

        public Paging() {
            this(new ArrayList<>(), null);
        }
        
        public boolean hasNextToken() {
            return StringUtils.isNotBlank(pageToken());
        }
        public String pageToken() {
            return StringUtils.trimToNull(nextToken.pageToken());
        }

    }

    public DUUIDirectoryNode(Document node) {
        this(
            node.getString("id"), 
            node.getString("name"), 
            node.getString("path"), 
            Boolean.TRUE.equals(node.getBoolean("isDirectory")) ? Type.DIR : Type.FILE, 
            node.getInteger("depth", 0),
            (Long)node.getOrDefault("size", Long.valueOf(0L)), 
            node.getString("mimeType"), 
            (Long)node.getOrDefault("mtime", Long.valueOf(0L)), 
            new Paging(node)
        );
    }
    
    public DUUIDirectoryNode {
        if (id == null || id.isBlank()) throw new IllegalArgumentException("id");
        if (name == null) throw new IllegalArgumentException("name");
        if (path == null || path.isBlank()) throw new IllegalArgumentException("path");
        if (type == null) throw new IllegalArgumentException("type");
        paging = (paging == null) ? new Paging() : paging;
    }

    public List<DUUIDirectoryNode> children() {
        return paging.children;
    }

    public boolean canTraverse(int maxDepth) {
        if (hasAllChildren()) return false;
        if (maxDepth >= 0 && this.depth() >= maxDepth) return false;
        if (this.isFile() || children().isEmpty()) {
            if (this.isFile()) this.setNoChildren();
            return false; 
        }
        return true;
    }

    public boolean hasAllChildren() {
        return TokenState.NO_MORE_PAGES.toString().equals(paging.nextToken.state());
    }

    public void setNoChildren() {
        nextToken(null);
        state(TokenState.NO_MORE_PAGES);
    }

    public boolean hasChildren() {
        return !children().isEmpty();
    }

    public boolean hasNextToken() {
        return paging.hasNextToken();
    }
    
    public void state(TokenState e) {
        paging.nextToken.state(e.toString());
    }
    public void nextToken(String next, Exception e) {
        nextToken(next, String.format("%s: %s", e.getClass().getSimpleName(), e.getMessage()));
    }

    public void nextToken(String next) {
        paging.nextToken.pageToken(next);
        if (StringUtils.isBlank(next)) state(TokenState.NO_MORE_PAGES);
    }

    public void nextToken(String next, String state) {
        paging.nextToken.pageToken(next);
        paging.nextToken.state(state);
    }

    public String nextToken() {
        return paging.pageToken();
    }
    public boolean isFile() {
        return type.equals(Type.FILE);
    }

    public boolean isDirectory() {
        return type.equals(Type.DIR);
    }

    public String path() {
        return stripNamespace(path);
    }

    public Document toDocument() {
        Document out = new Document()
            .append("id", id)
            .append("name", name)
            .append("path", path)
            .append("isDirectory", isDirectory())
            .append("depth", depth)
            .append("hasMoreChildren", !hasAllChildren())
            .append("mtime", mtime);
        if (size != null) out.append("size", size);
        if (mimeType != null) out.append("mimeType", mimeType);
        out.append("children", children().stream().map(DUUIDirectoryNode::toDocument).collect(Collectors.toList()));

        if (hasNextToken()){ 
            out.append("nextToken", paging.nextToken().pageToken());
            out.append("tokenState", paging.nextToken().state());
        }

        return out;
    }


    public static DUUIDirectoryNode from(
        Path entry,
        boolean isDirectory,
        int depth,
        Long size,
        String mimeType,
        long mtime
    ) {
        String raw = normalizeFs(entry);
        return from(
            "local",
            raw,
            entry.getFileName() == null ? raw : entry.getFileName().toString(),
            isDirectory,
            depth,
            size,
            mimeType,
            mtime,
            new Paging(isDirectory)
        );
    }

    public static DUUIDirectoryNode from(
        String namespace,
        String raw,
        String name,
        boolean isDirectory,
        int depth,
        Long size,
        String mimeType,
        long mtime
    ) {
        return from(namespace, raw, name, isDirectory, depth, size, mimeType, mtime, new Paging(isDirectory));
    }

    public static DUUIDirectoryNode from(
        String namespace,
        String raw,
        String name,
        boolean isDirectory,
        int depth,
        Long size,
        String mimeType,
        long mtime,
        Paging paging
    ) {
        String ns = (namespace == null || namespace.isBlank()) ? "default" : namespace.trim();
        String canon = ns + ":" + normalizeRaw(raw);

        return new DUUIDirectoryNode(
            stableId(canon),
            (name == null || name.isEmpty()) ? canon : name,
            canon,
            isDirectory ? DUUIDirectoryNode.Type.DIR : DUUIDirectoryNode.Type.FILE,
            depth,
            size,
            mimeType,
            mtime,
            paging
        );
    }

    public DUUIDirectoryNode pruneToDepth(int maxDepth) {
        if (maxDepth < 0) return this;
        
        if (depth >= maxDepth) {
            this.children().clear();
            return this;
        }

        children().forEach(c -> c.pruneToDepth(maxDepth));
        
        return this;
    }
    public DUUIDirectoryNode filter(Predicate<DUUIDirectoryNode> predicate) {
        if (predicate == null) return this;
        if (!predicate.test(this)) return null;

        List<DUUIDirectoryNode> newChildren = new ArrayList<>();
        for (DUUIDirectoryNode c : children()) {
            DUUIDirectoryNode kept = c.filter(predicate);
            if (kept != null) {
                newChildren.add(kept);
            }
        }

        children().clear();
        children().addAll(newChildren);
        return this;
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

    public static String stripNamespace(String canon) {
        if (canon == null) return "";
        int idx = canon.indexOf(':');
        if (idx < 0) return canon;
        return canon.substring(idx + 1);
    }
}
