package org.texttechnologylab.duui.gateway;

import org.texttechnologylab.duui.gateway.store.GatewayStorage;
import org.texttechnologylab.duui.storage.DUUIStoredCorpus;
import org.texttechnologylab.duui.storage.DUUIStoredDocument;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

public final class GatewayCorpusService {
    private static final Pattern SOFA = Pattern.compile("sofaString=\"([^\"]*)\"", Pattern.DOTALL);
    private static final Pattern BEGIN_END = Pattern.compile("<([^\\s/>]+)[^>]*\\sbegin=\"(\\d+)\"[^>]*\\send=\"(\\d+)\"[^>]*/?>");
    private static final Pattern TYPE_NAME = Pattern.compile("<name>([^<]+)</name>");
    private static final Pattern ATTR = Pattern.compile("([A-Za-z_:][\\w:.-]*)=\"([^\"]*)\"");

    private final GatewayStorage storage;
    private final Path root;
    private volatile Map<String, Object> cachedTree;

    public GatewayCorpusService(GatewayStorage storage) {
        this.storage = Objects.requireNonNull(storage, "storage");
        this.root = Path.of(System.getProperty(
                "duui.gateway.sampleRoot",
                env("DUUI_GATEWAY_SAMPLE_ROOT", "/home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/preprocessed_corpora_sample_1000")
        )).toAbsolutePath().normalize();
        indexRoot();
    }

    public Map<String, Object> tree() {
        Map<String, Object> tree = cachedTree;
        if (tree == null) {
            tree = buildTree();
            cachedTree = tree;
        }
        return tree;
    }

    public List<Map<String, Object>> samples() {
        if (!Files.isDirectory(root)) return List.of();
        try (Stream<Path> paths = Files.walk(root)) {
            return paths.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".xmi.gz"))
                    .sorted()
                    .limit(1000)
                    .map(path -> map("name", relative(path), "path", relative(path), "size", size(path), "kind", "xmi"))
                    .toList();
        } catch (IOException error) {
            return List.of();
        }
    }

    public Map<String, Object> artifact(String rawPath) {
        String safe = safePath(rawPath);
        Path path = safe.isBlank() ? root : root.resolve(safe).normalize();
        if (!path.startsWith(root) || !Files.exists(path)) {
            return map("artifact", map("kind", "missing", "name", safe, "address", address(safe)), "children", List.of());
        }
        if (Files.isDirectory(path)) {
            return directoryArtifact(path, safe);
        }
        return fileArtifact(path, safe);
    }

    public Map<String, Object> selection(List<String> paths) {
        List<Map<String, Object>> artifacts = (paths == null ? List.<String>of() : paths).stream().map(this::artifact).toList();
        long documents = artifacts.stream().filter(item -> "file".equals(nested(item, "artifact", "kind"))).count();
        return map("documentCount", documents, "artifactCount", artifacts.size(), "commonViews", documents > 0 ? List.of("_InitialView") : List.of(), "selected", paths == null ? List.of() : paths);
    }

    public Map<String, Object> typesystem() {
        Path file = Files.exists(root.resolve("TypeSystem.xml.gz")) ? root.resolve("TypeSystem.xml.gz") : root.resolve("TypeSystem.xml");
        String xml = readText(file, 2_000_000);
        List<String> names = new ArrayList<>();
        Matcher matcher = TYPE_NAME.matcher(xml);
        while (matcher.find() && names.size() < 800) {
            String name = xmlUnescape(matcher.group(1));
            if (name.contains(".")) names.add(name);
        }
        if (names.isEmpty()) names = List.of("uima.cas.TOP", "uima.tcas.Annotation");
        List<Map<String, Object>> types = names.stream().sorted().map(name -> map("name", name, "label", label(name), "featureCount", 0)).toList();
        return map("types", types, "tree", namespaceTree(types));
    }

    public Map<String, Object> playgroundModel() {
        return map(
                "components", List.of(
                        map("id", "text-reader", "label", "Reader", "kind", "canvas", "modality", "text", "requires", List.of(
                                map("parameter", "view", "type", "DUAView", "constraint", "Sofa media modality is text")
                        )),
                        map("id", "highlight-layer", "label", "Highlight layer", "kind", "annotation-layer", "modality", "text", "requires", List.of(
                                map("parameter", "annotationType", "type", "DUAType", "constraint", "type provides mappable integer begin/end features"),
                                map("parameter", "begin", "type", "DUAFeature", "constraint", "functionalProperty includes range-start"),
                                map("parameter", "end", "type", "DUAFeature", "constraint", "functionalProperty includes range-end")
                        )),
                        map("id", "dependency-arcs", "label", "Reference arcs", "kind", "annotation-layer", "modality", "text", "requires", List.of(
                                map("parameter", "annotationType", "type", "DUAType", "constraint", "source annotations are addressable in text"),
                                map("parameter", "target", "type", "DUAFeature", "constraint", "functionalProperty includes reference-target")
                        )),
                        map("id", "annotation-network", "label", "Association network", "kind", "canvas", "modality", "annotation", "requires", List.of(
                                map("parameter", "nodeType", "type", "DUAType", "constraint", "annotations project to DUA domain units"),
                                map("parameter", "association", "type", "DUAAssociation", "constraint", "Reference, Sequence, Membership, or Equivalence semantics")
                        )),
                        map("id", "annotation-chart", "label", "Annotation chart", "kind", "canvas", "modality", "annotation", "requires", List.of(
                                map("parameter", "dimension", "type", "DUAFeature", "constraint", "label, ordinal, or categorical feature"),
                                map("parameter", "value", "type", "DUAFeature", "constraint", "numeric measure or count aggregation")
                        ))
                ),
                "dimensions", List.of(
                        map("name", "modality", "values", List.of("text", "audio", "video", "annotation")),
                        map("name", "scope", "values", List.of("single-document view", "multi-document annotation projection")),
                        map("name", "featureProperty", "values", List.of("range-start", "range-end", "reference-target", "ordinal", "label", "weight", "association-role")),
                        map("name", "association", "values", List.of("Reference", "Sequence", "Membership", "Equivalence")),
                        map("name", "componentKind", "values", List.of("full canvas", "annotation layer")),
                        map("name", "addressability", "values", List.of("corpus", "directory", "file", "view", "feature-structure", "type", "feature", "domain-unit"))
                ),
                "uml", """
                        classDiagram
                        DUAUniverse "1" o-- "many" DUACorpus
                        DUACorpus "1" o-- "many" DUADocument
                        DUADocument "1" o-- "many" DUAView
                        DUAView "1" o-- "many" DUAFeatureStructure
                        DUAFeatureStructure --> DUAType
                        DUAType "1" o-- "many" DUAFeature
                        DUAFeatureStructure --> DUADomainUnit : projection
                        DUADomainUnit --> DUAAssociation : Reference Sequence Membership Equivalence
                        DUAInspectorBinding --> DUAFeature : typed slot mapping
                        """
        );
    }

    private void indexRoot() {
        if (!Files.isDirectory(root) || storage.corpora().get("sample-corpus").isPresent()) return;
        storage.bulkUpdate(() -> {
            Instant now = Instant.now();
            List<Map<String, Object>> samples = samples();
            List<String> ids = samples.stream().map(sample -> "sample-corpus:" + sample.get("path")).toList();
            storage.corpora().put("sample-corpus", new DUUIStoredCorpus("sample-corpus", root.getFileName().toString(), root.toUri().toString(), "application/xmi+xml", ids, now, now, map("path", root.toString())));
            for (Map<String, Object> sample : samples) {
                String relative = String.valueOf(sample.get("path"));
                storage.documents().put("sample-corpus:" + relative, new DUUIStoredDocument("sample-corpus:" + relative, "sample-corpus", relative, root.resolve(relative).toUri().toString(), "application/xmi+xml", "gzip", List.of("_InitialView"), now, now, map("bytes", sample.get("size"))));
            }
        });
    }

    private Map<String, Object> buildTree() {
        if (!Files.isDirectory(root)) return map("root", map("name", "No corpus root", "path", ""), "tree", map("kind", "corpus", "name", "No corpus root", "path", "", "dirs", List.of(), "files", List.of()), "totals", map("xmi", 0, "directories", 0, "files", 0));
        TreeStats stats = new TreeStats();
        Map<String, Object> node = directoryNode(root, "", stats, 0);
        return map("root", map("name", root.getFileName().toString(), "path", "", "address", "corpus:///sample-corpus"), "tree", node, "totals", map("xmi", stats.xmi, "directories", stats.directories, "files", stats.files), "rootPath", root.toString());
    }

    private Map<String, Object> directoryNode(Path dir, String relative, TreeStats stats, int depth) {
        stats.directories++;
        List<Map<String, Object>> dirs = new ArrayList<>();
        List<Map<String, Object>> files = new ArrayList<>();
        try (Stream<Path> children = Files.list(dir)) {
            children.sorted(Comparator.comparing(path -> path.getFileName().toString())).forEach(path -> {
                String child = relative.isBlank() ? path.getFileName().toString() : relative + "/" + path.getFileName();
                if (Files.isDirectory(path) && depth < 8) dirs.add(directoryNode(path, child, stats, depth + 1));
                else if (Files.isRegularFile(path)) {
                    stats.files++;
                    boolean xmi = path.getFileName().toString().endsWith(".xmi.gz");
                    if (xmi) stats.xmi++;
                    files.add(map("kind", "file", "name", path.getFileName().toString(), "path", child, "size", size(path), "mediaType", mediaType(path), "compression", path.getFileName().toString().endsWith(".gz") ? "gzip" : "none", "xmiCount", xmi ? 1 : 0));
                }
            });
        } catch (IOException ignored) {
        }
        int xmiCount = files.stream().mapToInt(file -> ((Number) file.getOrDefault("xmiCount", 0)).intValue()).sum()
                + dirs.stream().mapToInt(child -> ((Number) child.getOrDefault("xmiCount", 0)).intValue()).sum();
        return map("kind", relative.isBlank() ? "corpus" : "directory", "name", relative.isBlank() ? root.getFileName().toString() : dir.getFileName().toString(), "path", relative, "address", address(relative), "dirs", dirs, "files", files, "xmiCount", xmiCount);
    }

    private Map<String, Object> directoryArtifact(Path path, String safe) {
        List<Map<String, Object>> children = new ArrayList<>();
        try (Stream<Path> stream = Files.list(path)) {
            stream.sorted(Comparator.comparing(item -> item.getFileName().toString())).limit(300).forEach(child -> children.add(map("kind", Files.isDirectory(child) ? "directory" : "file", "name", child.getFileName().toString(), "path", relative(child), "address", address(relative(child)))));
        } catch (IOException ignored) {
        }
        return map("artifact", map("kind", safe.isBlank() ? "corpus" : "directory", "name", path.getFileName().toString(), "address", address(safe)), "media", map("modality", "collection", "mediaType", "inode/directory"), "children", children, "descendantXmi", countXmi(path));
    }

    private Map<String, Object> fileArtifact(Path path, String safe) {
        String xml = readText(path, 4_000_000);
        String text = firstSofa(xml);
        List<Map<String, Object>> annotations = annotations(xml, text);
        Map<String, Long> counts = new LinkedHashMap<>();
        Map<String, java.util.Set<String>> featureSamples = new LinkedHashMap<>();
        for (Map<String, Object> annotation : annotations) {
            String type = String.valueOf(annotation.get("type"));
            counts.merge(type, 1L, Long::sum);
            Object features = annotation.get("features");
            if (features instanceof Map<?, ?> map) {
                featureSamples.computeIfAbsent(type, ignored -> new java.util.TreeSet<>()).addAll(map.keySet().stream().map(String::valueOf).toList());
            }
        }
        return map("artifact", map("kind", "file", "name", path.getFileName().toString(), "address", address(safe), "path", safe), "media", map("modality", path.getFileName().toString().endsWith(".xmi.gz") ? "text" : "bytes", "mediaType", mediaType(path), "compression", path.getFileName().toString().endsWith(".gz") ? "gzip" : "none"), "views", List.of(map("id", "1", "name", "_InitialView", "media", map("modality", "text", "mediaType", "text/plain"), "text", text)), "text", text, "annotations", annotations, "typeCounts", counts.entrySet().stream().map(entry -> map("type", entry.getKey(), "count", entry.getValue(), "features", List.copyOf(featureSamples.getOrDefault(entry.getKey(), java.util.Set.of())))).toList());
    }

    private List<Map<String, Object>> annotations(String xml, String text) {
        List<Map<String, Object>> values = new ArrayList<>();
        Matcher matcher = BEGIN_END.matcher(xml);
        while (matcher.find() && values.size() < 500) {
            int begin = Integer.parseInt(matcher.group(2));
            int end = Integer.parseInt(matcher.group(3));
            if (end < begin) continue;
            Map<String, String> attrs = attrs(matcher.group(0));
            Map<String, String> features = new LinkedHashMap<>();
            attrs.forEach((key, value) -> {
                if (!List.of("id", "begin", "end", "sofa").contains(key)) features.put(key, xmlUnescape(value));
            });
            String id = attrs.getOrDefault("id", localXmlName(matcher.group(1)) + "-" + values.size());
            values.add(map(
                    "id", id,
                    "type", localXmlName(matcher.group(1)),
                    "begin", begin,
                    "end", end,
                    "sofa", attrs.getOrDefault("sofa", "1"),
                    "features", features,
                    "coveredText", begin >= 0 && end <= text.length() ? text.substring(begin, end) : ""
            ));
        }
        return values;
    }

    private String readText(Path path, int maxBytes) {
        if (!Files.isRegularFile(path)) return "";
        try (InputStream file = Files.newInputStream(path); InputStream input = path.getFileName().toString().endsWith(".gz") ? new GZIPInputStream(file) : file) {
            return new String(input.readNBytes(maxBytes), StandardCharsets.UTF_8);
        } catch (IOException error) {
            return "";
        }
    }

    private String firstSofa(String xml) {
        Matcher matcher = SOFA.matcher(xml);
        return matcher.find() ? xmlUnescape(matcher.group(1)) : "";
    }

    private long countXmi(Path path) {
        try (Stream<Path> stream = Files.walk(path)) {
            return stream.filter(Files::isRegularFile).filter(item -> item.getFileName().toString().endsWith(".xmi.gz")).count();
        } catch (IOException error) {
            return 0;
        }
    }

    private String safePath(String raw) {
        if (raw == null || raw.isBlank()) return "";
        Path resolved = root.resolve(raw).normalize();
        return resolved.startsWith(root) ? relative(resolved) : "";
    }

    private String relative(Path path) {
        return root.relativize(path.toAbsolutePath().normalize()).toString().replace('\\', '/');
    }

    private static Object nested(Map<String, Object> value, String key, String nested) {
        Object child = value.get(key);
        return child instanceof Map<?, ?> map ? map.get(nested) : null;
    }

    private static String address(String path) {
        return path == null || path.isBlank() ? "corpus:///sample-corpus" : "corpus:///sample-corpus/" + path;
    }

    private static String mediaType(Path path) {
        String name = path.getFileName().toString();
        if (name.endsWith(".xmi.gz") || name.endsWith(".xmi")) return "application/xmi+xml";
        if (name.endsWith(".xml.gz") || name.endsWith(".xml")) return "application/xml";
        if (name.endsWith(".txt") || name.endsWith(".tsv")) return "text/plain";
        return "application/octet-stream";
    }

    private static long size(Path path) {
        try { return Files.size(path); } catch (IOException error) { return 0L; }
    }

    private static String label(String name) {
        return name.substring(name.lastIndexOf('.') + 1);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> namespaceTree(List<Map<String, Object>> types) {
        Map<String, Object> root = map("kind", "typesystem", "name", "TypeSystem", "children", new ArrayList<Map<String, Object>>(), "types", new ArrayList<Map<String, Object>>());
        Map<String, Map<String, Object>> byPath = new LinkedHashMap<>();
        byPath.put("", root);
        for (Map<String, Object> type : types) {
            String name = String.valueOf(type.get("name"));
            String[] parts = name.split("\\.");
            String path = "";
            Map<String, Object> current = root;
            for (int index = 0; index < parts.length - 1; index++) {
                path = path.isBlank() ? parts[index] : path + "." + parts[index];
                Map<String, Object> next = byPath.get(path);
                if (next == null) {
                    next = map("kind", "namespace", "name", parts[index], "children", new ArrayList<Map<String, Object>>(), "types", new ArrayList<Map<String, Object>>());
                    ((List<Map<String, Object>>) current.get("children")).add(next);
                    byPath.put(path, next);
                }
                current = next;
            }
            ((List<Map<String, Object>>) current.get("types")).add(type);
        }
        return root;
    }

    private static String localXmlName(String name) {
        String value = name.contains(":") ? name.substring(name.indexOf(':') + 1) : name;
        return value.contains("}") ? value.substring(value.indexOf('}') + 1) : value;
    }

    private static Map<String, String> attrs(String tag) {
        Map<String, String> values = new LinkedHashMap<>();
        Matcher matcher = ATTR.matcher(tag);
        while (matcher.find()) values.put(localXmlName(matcher.group(1)), matcher.group(2));
        return values;
    }

    private static String xmlUnescape(String value) {
        return value.replace("&quot;", "\"").replace("&apos;", "'").replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&");
    }

    private static Map<String, Object> map(Object... pairs) {
        Map<String, Object> value = new LinkedHashMap<>();
        for (int index = 0; index + 1 < pairs.length; index += 2) value.put(String.valueOf(pairs[index]), pairs[index + 1]);
        return value;
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static final class TreeStats {
        private int directories;
        private int files;
        private int xmi;
    }
}
