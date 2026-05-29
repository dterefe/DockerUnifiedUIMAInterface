package org.texttechnologylab.duui.rework;

import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.Type;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.XMLInputSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.texttechnologylab.duui.event.DUUIEvent;
import org.texttechnologylab.duui.event.DUUIEventService;
import org.texttechnologylab.duui.event.DUUIInMemoryEventSink;
import org.texttechnologylab.duui.exception.DUUIExecutionResult;
import org.texttechnologylab.duui.orchestration.DUUIOrchestrationResult;
import org.texttechnologylab.duui.pipeline.io.DUUIXmiCollectionReader;
import org.texttechnologylab.duui.runtime.DUUI;
import org.texttechnologylab.duui.runtime.DUUIGeneratorScope;
import org.texttechnologylab.duui.runtime.DUUIPipelineScope;
import org.texttechnologylab.duui.runtime.DUUIStageScope;
import org.texttechnologylab.duui.runtime.DUUISystemScope;
import org.texttechnologylab.duui.runtime.DUUIV1ComponentBuilder;

import java.io.InputStream;
import java.net.http.HttpClient;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Modern-only v2 pipeline test: composer + orchestrator with gnfinder, spacy,
 * taxonerd, gazetteer via Podman. NO legacy comparisons.
 */
class DUUIV2AnnotatorMatrixTest {
    private static final String EXAMPLES = "../../duui-py/examples";
    private static final HttpClient CLIENT = HttpClient.newHttpClient();
    private static final Random RNG = new Random();

    @TempDir
    Path tempDir;

    // ---- @Test entry points ----

    @Test
    void spacyModernMsgpackLuaPodman() throws Exception {
        String image = System.getProperty("duui.py.spacy.async.image", "localhost/duui-py-spacy-msgpack-lua:latest");
        Map<String, String> params = Map.of(
                "spacy_model_size", System.getProperty("duui.py.spacy.model_size", "trf"),
                "spacy_batch_size", System.getProperty("duui.py.spacy.batch_size", "32"),
                "use_existing_sentences", "false",
                "spacy_language", "de"
        );
        BatchRun run = runPodmanBatch("SPACY_V2", "spacy", image, false, false,
                params, null, spacyDocuments(), localTypeSystemFor("spacy"),
                SPACY_OUTPUT_ROOT_TYPES);

        for (var artifact : run.artifacts().values()) {
            int tokens = artifact.typeCounts.getOrDefault(
                    "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token", 0);
            assertTrue(tokens > 0,
                    () -> "spacy produced no tokens for " + artifact.id());
        }
    }

    @Test
    void taxonerdModernMsgpackLuaPodman() throws Exception {
        String image = System.getProperty("duui.py.taxonerd.async.image", "localhost/duui-py-taxonerd-msgpack-lua:latest");
        Map<String, String> params = Map.of(
                "model", System.getProperty("duui.py.taxonerd.model", "en_ner_eco_md"),
                "linking", System.getProperty("duui.py.taxonerd.linking", "gbif_backbone"),
                "threshold", System.getProperty("duui.py.taxonerd.threshold", "0.7"),
                "input_strategy", System.getProperty("duui.py.taxonerd.input_strategy", "legacy-procedure"),
                "linker_strategy", System.getProperty("duui.py.taxonerd.linker_strategy", "ann-original"),
                "allow_unlinked", System.getProperty("duui.py.taxonerd.allow_unlinked", "false"),
                "prefer_gpu", "true",
                "timeout", System.getProperty("duui.py.taxonerd.timeout", "600")
        );
        runPodmanBatch("TAXONERD_V2", "taxonerd", image, false, false,
                params, null, taxonerdDocuments(), localTypeSystemFor("taxonerd"),
                List.of("org.texttechnologylab.annotation.type.Taxon"));
    }

    @Test
    void gazetteerModernMsgpackLuaPodman() throws Exception {
        String image = System.getProperty("duui.py.gazetteer.async.image", "localhost/duui-py-gazetteer-msgpack-lua:latest");
        runPodmanBatch("GAZETTEER_V2", "gazetteer", image, false, false,
                Map.of("timeout", System.getProperty("duui.py.gazetteer.timeout", "120")),
                null, gazetteerDocuments(), localTypeSystemFor("gazetteer"),
                List.of("org.texttechnologylab.annotation.GazetteerEntity"));
    }

    @Test
    void gnfinderModernMsgpackLuaPodman() throws Exception {
        String image = System.getProperty("duui.py.gnfinder.async.image", "localhost/duui-py-gnfinder-msgpack-lua:latest");
        Map<String, String> params = Map.of(
                "lang", System.getProperty("duui.py.gnfinder.lang", "de"),
                "verify", System.getProperty("duui.py.gnfinder.verify", "true"),
                "utf8_input", System.getProperty("duui.py.gnfinder.utf8_input", "true")
        );
        runPodmanBatch("GNFINDER_V2", "gnfinder", image, false, false,
                params, "application/vnd.apache.uima.xmi+xml",
                gnfinderDocuments(), localTypeSystemFor("gnfinder"),
                List.of("org.texttechnologylab.annotation.biofid.gnfinder.Taxon"));
    }

    // ---- Spacy output types ----

    private static final List<String> SPACY_OUTPUT_ROOT_TYPES = List.of(
            "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence",
            "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token",
            "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma",
            "de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS",
            "de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.morph.MorphologicalFeatures",
            "de.tudarmstadt.ukp.dkpro.core.api.syntax.type.dependency.Dependency",
            "de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity"
    );

    // ---- Podman pipeline runner ----

    private BatchRun runPodmanBatch(
            String label, String id, String image, boolean streaming, boolean gpu,
            Map<String, String> parameters, String contentType, List<Path> documents,
            TypeSystemDescription typeSystem, List<String> countTypes
    ) throws Exception {
        Path input = materializeXmiDirectory(id, typeSystem, documents);
        DUUIInMemoryEventSink sink = new DUUIInMemoryEventSink();
        DUUIEventService events = new DUUIEventService(List.of(sink));
        DUUIOrchestrationResult result;
        try (DUUISystemScope system = DUUI.system("v2-matrix-" + id).events(events)) {
            try (DUUIPipelineScope pipeline = system.pipeline(id + "-pipeline")) {
                try (DUUIGeneratorScope<JCas> source = DUUIXmiCollectionReader.builder()
                        .typeSystem(typeSystem)
                        .source(input)
                        .open(pipeline)) {
                    try (DUUIStageScope<JCas> stage = source.linear(id + "-stage")) {
                        DUUIV1ComponentBuilder component = stage.v1(id)
                                .podman()
                                .image(image)
                                .imageFetching(Boolean.getBoolean("duui.py.matrix.image.fetching"))
                                .sourceView("_InitialView")
                                .targetView("_InitialView")
                                .telemetrySink(sink)
                                .parameters(parameters)
                                .timeoutSeconds(Long.getLong("duui.py.matrix.timeout.seconds", 7200L))
                                .scale(Integer.getInteger("duui.py.matrix.scale", 1))
                                .concurrency(Integer.getInteger("duui.py.matrix.concurrency", 1));
                        component.gpu(gpu);
                        component.streamingTransport(streaming);
                        if (!streaming && contentType != null) {
                            component.contentType(contentType);
                        }
                    }
                }
            }
            result = system.run(id + "-pipeline");
            assertFalse(result.hasFailures(), () -> describeFailures(label, id, image, result));
            assertEquals(0, result.unroutableArtifacts().size(),
                    () -> label + " unroutable artifacts for " + id + " image=" + image);
        }

        Map<String, ResultArtifact> artifacts = new LinkedHashMap<>();
        for (DUUIExecutionResult<?> execution : result.results()) {
            if (!(execution.artifact().payload() instanceof JCas cas)) {
                continue;
            }
            String artifactId = execution.artifact().gid().toString();
            List<DUUIEvent> observed = waitForMetrics(sink, artifactId, expectedHttpMetrics(streaming));
            ResultArtifact artifact = new ResultArtifact(
                    artifactId, execution.durationMs(), cas,
                    cas.getDocumentText() == null ? 0 : cas.getDocumentText().length(),
                    typeSummary(cas, countTypes), observed);
            artifacts.put(textKey(cas), artifact);
        }
        assertEquals(documents.size(), artifacts.size(),
                label + " did not produce one result artifact per input document for " + id
                        + " (expected " + documents.size() + " got " + artifacts.size() + ")");
        return new BatchRun(id, artifacts);
    }

    // ---- Type system helpers ----

    private static TypeSystemDescription localTypeSystemFor(String idPrefix) throws Exception {
        return switch (idPrefix) {
            case "taxonerd" -> localExampleTypeSystem(
                    "taxonerd-msgpack-lua/TypeSystemTaxoNERD.xml");
            case "gazetteer" -> localExampleTypeSystem(
                    "gazetteer-msgpack-lua/TypeSystemGazetteer.xml");
            case "gnfinder" -> localExampleTypeSystem(
                    "gnfinder-msgpack-lua/TypeSystemGNFinder.xml");
            case "spacy" -> localExampleTypeSystem(
                    "spacy-lua-msgpack/TypeSystemSpacyMsgpack.xml");
            default -> throw new IllegalArgumentException("No type system for " + idPrefix);
        };
    }

    private static TypeSystemDescription localExampleTypeSystem(String... relativePaths) throws Exception {
        List<TypeSystemDescription> descriptions = new ArrayList<>();
        descriptions.add(TypeSystemDescriptionFactory.createTypeSystemDescription());
        for (String relative : relativePaths) {
            var file = Path.of(EXAMPLES, relative).toFile();
            if (!file.exists()) {
                continue;
            }
            descriptions.add(UIMAFramework.getXMLParser().parseTypeSystemDescription(
                    new XMLInputSource(file)));
        }
        return CasCreationUtils.mergeTypeSystems(descriptions);
    }

    // ---- XMI helpers ----

    private Path materializeXmiDirectory(String id, TypeSystemDescription typeSystem, List<Path> documents) throws Exception {
        Path input = tempDir.resolve(id + "-input");
        Files.createDirectories(input);
        for (int index = 0; index < documents.size(); index++) {
            JCas cas = xmi(typeSystem, documents.get(index));
            Path output = input.resolve(String.format(Locale.ROOT, "%04d-%s.xmi", index, stem(documents.get(index))));
            try (java.io.OutputStream stream = Files.newOutputStream(output)) {
                org.apache.uima.util.CasIOUtils.save(cas.getCas(), stream, org.apache.uima.cas.SerialFormat.XMI_1_1);
            }
        }
        return input;
    }

    private static JCas xmi(TypeSystemDescription typeSystem, Path path) throws Exception {
        JCas cas = JCasFactory.createJCas(typeSystem);
        try (InputStream file = Files.newInputStream(path);
             InputStream input = path.getFileName().toString().endsWith(".gz") ? new GZIPInputStream(file) : file) {
            org.apache.uima.cas.impl.XmiCasDeserializer.deserialize(input, cas.getCas(), true);
        }
        cas.setDocumentLanguage("de");
        return cas;
    }

    // ---- Document resolution ----

    private static List<Path> resolveDocumentPaths(String annotatorKey, String defaultPaths) {
        String configured = System.getProperty("duui.py." + annotatorKey + ".documents", defaultPaths);
        List<Path> candidates = paths(configured);
        if (candidates.size() > 3) {
            candidates = sampleRandom(candidates,
                    Integer.getInteger("duui.py.matrix.documents.max", 5));
        }
        return expandGlobs(candidates);
    }

    private static List<Path> spacyDocuments() {
        return resolveDocumentPaths("spacy", defaultDocumentPaths("spacy"));
    }

    private static List<Path> taxonerdDocuments() {
        return resolveDocumentPaths("taxonerd", defaultDocumentPaths("taxonerd"));
    }

    private static List<Path> gazetteerDocuments() {
        return resolveDocumentPaths("gazetteer", defaultDocumentPaths("gazetteer"));
    }

    private static List<Path> gnfinderDocuments() {
        return resolveDocumentPaths("gnfinder", defaultDocumentPaths("gnfinder"));
    }

    private static String defaultDocumentPaths(String annotatorKey) {
        return switch (annotatorKey) {
            case "spacy" -> String.join(",",
                    "/home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/preprocessed_corpora_sample_1000/Ornithologische_Mitteilungen_Monatsschrift_für_Vogelbeobachtung__Feldornithologie_und_Avifaunistik/1999/10778329.xmi.gz",
                    "/home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/preprocessed_corpora_sample_1000/Der_Palmengarten/1979/12458605.xmi.gz",
                    "/home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/preprocessed_corpora_sample_1000/Nerthus_ill__Wochenschr__für_Tier-_u__Pflanzenfreunde_;_Organ_für_Sammler_u__Freunde_aller_naturwiss__Zweige/1901/4527026.xmi.gz"
            );
            case "taxonerd" -> String.join(",",
                    "/home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/preprocessed_corpora_sample_1000/General-Doubletten-Verzeichniss_des_Schlesischen_Botanischen_Tausch-Vereins_____Tauschjahr____/1883/4513701.xmi.gz",
                    "/home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/preprocessed_corpora_sample_1000/General-Doubletten-Verzeichniss_des_Schlesischen_Botanischen_Tausch-Vereins_____Tauschjahr____/1886/4566707.xmi.gz"
            );
            case "gazetteer" -> String.join(",",
                    "/home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/preprocessed_corpora_sample_1000/General-Doubletten-Verzeichniss_des_Schlesischen_Botanischen_Tausch-Vereins_____Tauschjahr____/1883/4513701.xmi.gz",
                    "/home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/preprocessed_corpora_sample_1000/General-Doubletten-Verzeichniss_des_Schlesischen_Botanischen_Tausch-Vereins_____Tauschjahr____/1886/4566707.xmi.gz",
                    "/home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/preprocessed_corpora_sample_1000/Botanisches_Literaturblatt_Organ_für_Autor-_und_Instituts-Referate_aus_dem_Gesamtgebiete_der_botan__Literatur/1903/4544734.xmi.gz"
            );
            case "gnfinder" -> String.join(",",
                    "/home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/preprocessed_corpora_sample_1000/General-Doubletten-Verzeichniss_des_Schlesischen_Botanischen_Tausch-Vereins_____Tauschjahr____/1883/4513701.xmi.gz",
                    "/home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/preprocessed_corpora_sample_1000/General-Doubletten-Verzeichniss_des_Schlesischen_Botanischen_Tausch-Vereins_____Tauschjahr____/1886/4566707.xmi.gz",
                    "/home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/preprocessed_corpora_sample_1000/Botanisches_Literaturblatt_Organ_für_Autor-_und_Instituts-Referate_aus_dem_Gesamtgebiete_der_botan__Literatur/1903/4544734.xmi.gz"
            );
            default -> "";
        };
    }

    private static List<Path> paths(String configured) {
        return java.util.Arrays.stream(configured.split(","))
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .flatMap(value -> {
                    try {
                        return java.util.stream.Stream.of(Path.of(value));
                    } catch (java.nio.file.InvalidPathException e) {
                        return java.util.stream.Stream.empty();
                    }
                })
                .toList();
    }

    private static List<Path> expandGlobs(List<Path> paths) {
        List<Path> expanded = new ArrayList<>();
        for (Path path : paths) {
            String fileName = path.getFileName() != null ? path.getFileName().toString() : "";
            if (fileName.contains("*") || fileName.contains("?")) {
                Path parent = path.getParent();
                if (parent != null && Files.isDirectory(parent)) {
                    PathMatcher matcher = java.nio.file.FileSystems.getDefault().getPathMatcher("glob:" + fileName);
                    try (DirectoryStream<Path> stream = Files.newDirectoryStream(parent)) {
                        for (Path entry : stream) {
                            if (matcher.matches(entry.getFileName())) {
                                expanded.add(entry);
                            }
                        }
                    } catch (Exception ignored) {
                        expanded.add(path);
                    }
                } else {
                    expanded.add(path);
                }
            } else {
                expanded.add(path);
            }
        }
        return expanded;
    }

    private static List<Path> sampleRandom(List<Path> pool, int count) {
        if (pool.size() <= count) return new ArrayList<>(pool);
        List<Path> copy = new ArrayList<>(pool);
        Collections.shuffle(copy, RNG);
        return copy.subList(0, count);
    }

    private static Map<String, Integer> typeSummary(JCas cas, List<String> typeNames) throws Exception {
        CAS view = cas.getView("_InitialView").getCas();
        Map<String, Integer> counts = new LinkedHashMap<>();
        List<String> allTypes = new ArrayList<>(typeNames);
        allTypes.add("org.texttechnologylab.annotation.AnnotationComment");
        allTypes.add("org.texttechnologylab.annotation.AnnotatorMetaData");
        allTypes.add("org.texttechnologylab.annotation.DocumentModification");
        for (String typeName : allTypes) {
            Type type = view.getTypeSystem().getType(typeName);
            if (type == null) continue;
            int count = countIndexed(view, type);
            if (count > 0) {
                counts.put(typeName.substring(typeName.lastIndexOf('.') + 1), count);
            }
        }
        return counts;
    }

    private static int countIndexed(CAS view, Type type) {
        int count = 0;
        org.apache.uima.cas.FSIterator<org.apache.uima.cas.FeatureStructure> iterator =
                view.getIndexRepository().getAllIndexedFS(type);
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        return count;
    }

    // ---- Metrics helpers ----

    private static List<String> expectedHttpMetrics(boolean streaming) {
        if (streaming) {
            return List.of("duui.http.serialize_ms", "duui.http.request_bytes",
                    "duui.http.response_decode_ms", "duui.http.request_duration_ms",
                    "duui.http.response_bytes");
        }
        return List.of("duui.http.serialize_ms", "duui.http.request_bytes",
                "duui.http.response_receive_ms", "duui.http.response_decode_ms",
                "duui.http.request_duration_ms", "duui.http.response_bytes");
    }

    private static List<DUUIEvent> waitForMetrics(DUUIInMemoryEventSink sink, String id, List<String> names)
            throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        List<DUUIEvent> events = eventsForArtifact(sink.events(), id);
        while (!hasMetrics(events, names) && System.nanoTime() < deadline) {
            Thread.sleep(20L);
            events = eventsForArtifact(sink.events(), id);
        }
        return events;
    }

    private static List<DUUIEvent> eventsForArtifact(List<DUUIEvent> events, String artifactId) {
        return events.stream().filter(event -> artifactId.equals(event.artifactId())).toList();
    }

    private static boolean hasMetrics(List<DUUIEvent> events, List<String> names) {
        for (String name : names) {
            boolean present = events.stream()
                    .anyMatch(event -> name.equals(event.metricName()) && event.metricValue() != null);
            if (!present) return false;
        }
        return true;
    }

    // ---- Diagnostics ----

    private static String describeFailures(String label, String id, String image, DUUIOrchestrationResult result) {
        StringBuilder builder = new StringBuilder();
        builder.append(label).append(" pipeline ").append(id).append(" image=").append(image).append(" failures:\n");
        result.results().stream()
                .map(DUUIExecutionResult::failure)
                .filter(Objects::nonNull)
                .forEach(failure -> builder.append("  message=").append(failure.message())
                        .append(" cause=").append(failure.cause()).append('\n'));
        return builder.toString();
    }

    private static String textKey(JCas cas) {
        String text = cas.getDocumentText();
        return Integer.toHexString(text == null ? 0 : text.hashCode()) + ":"
                + (text == null ? 0 : text.length());
    }

    private static String stem(Path path) {
        String name = path.getFileName().toString();
        if (name.endsWith(".xmi.gz")) return name.substring(0, name.length() - ".xmi.gz".length());
        if (name.endsWith(".xmi")) return name.substring(0, name.length() - ".xmi".length());
        return name.replaceAll("[^A-Za-z0-9_.-]", "_");
    }

    private static String typeSummary(Map<String, Integer> counts) {
        return counts.entrySet().stream()
                .map(entry -> entry.getKey() + ":" + entry.getValue())
                .collect(Collectors.joining("|"));
    }

    // ---- Data records ----

    private record BatchRun(String id, Map<String, ResultArtifact> artifacts) {
        ResultArtifact required(String textKey) {
            ResultArtifact artifact = artifacts.get(textKey);
            if (artifact == null) {
                throw new AssertionError(
                        "No result artifact for text key " + textKey + " in " + id + "; keys=" + artifacts.keySet());
            }
            return artifact;
        }
    }

    private record ResultArtifact(String id, long durationMs, JCas cas, int characters,
                                  Map<String, Integer> typeCounts, List<DUUIEvent> events) {
        double metric(String name) {
            return events.stream()
                    .filter(event -> name.equals(event.metricName()))
                    .map(DUUIEvent::metricValue)
                    .filter(Objects::nonNull)
                    .reduce((first, second) -> second)
                    .orElse(Double.NaN);
        }

        double metricRequired(String name) {
            double value = metric(name);
            if (Double.isNaN(value)) {
                throw new AssertionError(
                        "missing metric " + name + " for " + id + "; emitted=" + metricNames());
            }
            return value;
        }

        double metricRequiredAny(String... names) {
            for (String name : names) {
                double value = metric(name);
                if (!Double.isNaN(value)) return value;
            }
            throw new AssertionError(
                    "missing any metric " + List.of(names) + " for " + id + "; emitted=" + metricNames());
        }

        String metricNames() {
            return events.stream()
                    .map(DUUIEvent::metricName)
                    .filter(Objects::nonNull)
                    .collect(Collectors.joining(", "));
        }
    }
}
