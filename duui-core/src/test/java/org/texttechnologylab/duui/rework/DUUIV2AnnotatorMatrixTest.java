package org.texttechnologylab.duui.rework;

import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.Type;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.CasIOUtils;
import org.apache.uima.util.XMLInputSource;
import org.apache.uima.cas.SerialFormat;
import org.apache.uima.cas.impl.XmiCasDeserializer;
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
import java.io.OutputStream;
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
 * Clean v2 pipeline tests: composer + orchestrator with gnfinder, spacy,
 * taxonerd, gazetteer via Podman. NO legacy, NO image overrides.
 */
class DUUIV2AnnotatorMatrixTest {
    private static final String EXAMPLES = "../../duui-py/examples";
    private static final Random RNG = new Random();

    @TempDir
    Path tempDir;

    // ---- Orchestrator (v2 pipeline) ----

    @Test
    void orchestratorAllAnnotators() throws Exception {
        runOrchestratorAnnotator("spacy", SPACY_OUTPUT_ROOT_TYPES);
        runOrchestratorAnnotator("taxonerd", List.of("org.texttechnologylab.annotation.type.Taxon"));
        runOrchestratorAnnotator("gazetteer", List.of("org.texttechnologylab.annotation.GazetteerEntity"));
        runOrchestratorAnnotator("gnfinder", List.of("org.texttechnologylab.annotation.biofid.gnfinder.Taxon"));
    }

    private void runOrchestratorAnnotator(String key, List<String> countTypes) throws Exception {
        String image = imageFor(key);
        Map<String, String> params = paramsFor(key);
        boolean streaming = key.equals("spacy");
        String contentType = key.equals("gnfinder") ? "application/vnd.apache.uima.xmi+xml" : null;
        List<Path> docs = documentsFor(key);
        TypeSystemDescription ts = typeSystemFor(key);

        Path input = materializeXmiDir(key, ts, docs);
        DUUIInMemoryEventSink sink = new DUUIInMemoryEventSink();
        DUUIEventService events = new DUUIEventService(List.of(sink));
        DUUIOrchestrationResult result;

        try (DUUISystemScope system = DUUI.system("v2-" + key).events(events)) {
            try (DUUIPipelineScope pipeline = system.pipeline(key + "-pipeline")) {
                try (DUUIGeneratorScope<JCas> source = DUUIXmiCollectionReader.builder()
                        .typeSystem(ts).source(input).open(pipeline)) {
                    try (DUUIStageScope<JCas> stage = source.linear(key + "-stage")) {
                        DUUIV1ComponentBuilder comp = stage.v1(key)
                                .podman().image(image)
                                .sourceView("_InitialView").targetView("_InitialView")
                                .telemetrySink(sink).parameters(params)
                                .timeoutSeconds(Long.getLong("duui.py.matrix.timeout.seconds", 7200L))
                                .scale(Integer.getInteger("duui.py.matrix.scale", 1))
                                .concurrency(Integer.getInteger("duui.py.matrix.concurrency", 1));
                        // if (!streaming && contentType != null) comp.contentType(contentType);
                    }
                }
            }
            result = system.run(key + "-pipeline");
            assertFalse(result.hasFailures(), () -> key + " failures: " + describe(result, key, image));
            assertEquals(0, result.unroutableArtifacts().size(), key + " unroutable");
        }

        Map<String, ResultArtifact> artifacts = new LinkedHashMap<>();
        for (DUUIExecutionResult<?> exec : result.results()) {
            if (!(exec.artifact().payload() instanceof JCas cas)) continue;
            String aid = exec.artifact().gid().toString();
            List<DUUIEvent> obs = waitForMetrics(sink, aid, expectedMetrics(streaming));
            artifacts.put(textKey(cas), new ResultArtifact(aid, exec.durationMs(), cas,
                    cas.getDocumentText() == null ? 0 : cas.getDocumentText().length(),
                    typeSummary(cas, countTypes), obs));
        }
        assertEquals(docs.size(), artifacts.size(), key + " artifact count mismatch");

        for (var a : artifacts.values()) {
            assertTrue(a.typeCounts.values().stream().anyMatch(c -> c > 0),
                    () -> key + " produced no annotations for " + a.id);
        }
    }

    // ---- Helpers ----

    private static String imageFor(String key) {
        return switch (key) {
            case "spacy" -> System.getProperty("duui.py.spacy.async.image", "localhost/duui-py-spacy-msgpack-lua:latest");
            case "taxonerd" -> System.getProperty("duui.py.taxonerd.async.image", "localhost/duui-py-taxonerd-msgpack-lua:latest");
            case "gazetteer" -> System.getProperty("duui.py.gazetteer.async.image", "localhost/duui-py-gazetteer-msgpack-lua:latest");
            case "gnfinder" -> System.getProperty("duui.py.gnfinder.async.image", "localhost/duui-py-gnfinder-msgpack-lua:latest");
            default -> throw new IllegalArgumentException(key);
        };
    }

    private static Map<String, String> paramsFor(String key) {
        return switch (key) {
            case "spacy" -> Map.of(
                    "spacy_model_size", System.getProperty("duui.py.spacy.model_size", "trf"),
                    "spacy_batch_size", System.getProperty("duui.py.spacy.batch_size", "32"),
                    "use_existing_sentences", "false", "spacy_language", "de");
            case "taxonerd" -> Map.of(
                    "model", System.getProperty("duui.py.taxonerd.model", "en_ner_eco_md"),
                    "linking", System.getProperty("duui.py.taxonerd.linking", "gbif_backbone"),
                    "threshold", System.getProperty("duui.py.taxonerd.threshold", "0.7"),
                    "input_strategy", System.getProperty("duui.py.taxonerd.input_strategy", "legacy-procedure"),
                    "linker_strategy", System.getProperty("duui.py.taxonerd.linker_strategy", "ann-original"),
                    "allow_unlinked", System.getProperty("duui.py.taxonerd.allow_unlinked", "false"),
                    "prefer_gpu", "true", "timeout", System.getProperty("duui.py.taxonerd.timeout", "600"));
            case "gazetteer" -> Map.of("timeout", System.getProperty("duui.py.gazetteer.timeout", "120"));
            case "gnfinder" -> Map.of(
                    "lang", System.getProperty("duui.py.gnfinder.lang", "de"),
                    "verify", System.getProperty("duui.py.gnfinder.verify", "true"),
                    "utf8_input", System.getProperty("duui.py.gnfinder.utf8_input", "true"));
            default -> Map.of();
        };
    }

    private static List<Path> documentsFor(String key) {
        return resolveDocs(key, defaultDocPaths(key));
    }

    private static TypeSystemDescription typeSystemFor(String key) throws Exception {
        return switch (key) {
            case "taxonerd" -> localTS("taxonerd-msgpack-lua/TypeSystemTaxoNERD.xml");
            case "gazetteer" -> localTS("gazetteer-msgpack-lua/TypeSystemGazetteer.xml");
            case "gnfinder" -> localTS("gnfinder-msgpack-lua/TypeSystemGNFinder.xml");
            case "spacy" -> localTS("spacy-lua-msgpack/TypeSystemSpacyMsgpack.xml");
            default -> throw new IllegalArgumentException(key);
        };
    }

    private static TypeSystemDescription localTS(String... paths) throws Exception {
        List<TypeSystemDescription> ds = new ArrayList<>();
        ds.add(TypeSystemDescriptionFactory.createTypeSystemDescription());
        for (String p : paths) {
            var f = Path.of(EXAMPLES, p).toFile();
            if (f.exists()) ds.add(UIMAFramework.getXMLParser().parseTypeSystemDescription(new XMLInputSource(f)));
        }
        return CasCreationUtils.mergeTypeSystems(ds);
    }

    private Path materializeXmiDir(String id, TypeSystemDescription ts, List<Path> docs) throws Exception {
        Path dir = tempDir.resolve(id + "-input");
        Files.createDirectories(dir);
        for (int i = 0; i < docs.size(); i++) {
            JCas cas = loadXmi(ts, docs.get(i));
            Path out = dir.resolve(String.format(Locale.ROOT, "%04d-%s.xmi", i, stem(docs.get(i))));
            try (OutputStream os = Files.newOutputStream(out)) {
                CasIOUtils.save(cas.getCas(), os, SerialFormat.XMI_1_1);
            }
        }
        return dir;
    }

    private static JCas loadXmi(TypeSystemDescription ts, Path path) throws Exception {
        JCas cas = JCasFactory.createJCas(ts);
        try (InputStream f = Files.newInputStream(path);
             InputStream in = path.getFileName().toString().endsWith(".gz") ? new GZIPInputStream(f) : f) {
            XmiCasDeserializer.deserialize(in, cas.getCas(), true);
        }
        cas.setDocumentLanguage("de");
        return cas;
    }

    private static List<Path> resolveDocs(String key, String defaults) {
        String cfg = System.getProperty("duui.py." + key + ".documents", defaults);
        List<Path> cands = java.util.Arrays.stream(cfg.split(",")).map(String::trim)
                .filter(s -> !s.isEmpty()).flatMap(s -> {
                    try { return java.util.stream.Stream.of(Path.of(s)); }
                    catch (Exception e) { return java.util.stream.Stream.empty(); }
                }).toList();
        if (cands.size() > 5) cands = sample(cands, Integer.getInteger("duui.py.matrix.documents.max", 5));
        return expand(cands);
    }

    private static String defaultDocPaths(String key) {
        return switch (key) {
            case "spacy" -> "/home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/preprocessed_corpora_sample_1000/Ornithologische_Mitteilungen_Monatsschrift_für_Vogelbeobachtung__Feldornithologie_und_Avifaunistik/1999/10778329.xmi.gz,/home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/preprocessed_corpora_sample_1000/Der_Palmengarten/1979/12458605.xmi.gz";
            case "taxonerd" -> "/home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/preprocessed_corpora_sample_1000/General-Doubletten-Verzeichniss_des_Schlesischen_Botanischen_Tausch-Vereins_____Tauschjahr____/1883/4513701.xmi.gz";
            case "gazetteer" -> "/home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/preprocessed_corpora_sample_1000/General-Doubletten-Verzeichniss_des_Schlesischen_Botanischen_Tausch-Vereins_____Tauschjahr____/1883/4513701.xmi.gz";
            case "gnfinder" -> "/home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/preprocessed_corpora_sample_1000/General-Doubletten-Verzeichniss_des_Schlesischen_Botanischen_Tausch-Vereins_____Tauschjahr____/1883/4513701.xmi.gz";
            default -> "";
        };
    }

    private static List<Path> expand(List<Path> paths) {
        List<Path> out = new ArrayList<>();
        for (Path p : paths) {
            String fn = p.getFileName() != null ? p.getFileName().toString() : "";
            if (fn.contains("*") || fn.contains("?")) {
                Path parent = p.getParent();
                if (parent != null && Files.isDirectory(parent)) {
                    PathMatcher m = java.nio.file.FileSystems.getDefault().getPathMatcher("glob:" + fn);
                    try (DirectoryStream<Path> s = Files.newDirectoryStream(parent)) {
                        for (Path e : s) if (m.matches(e.getFileName())) out.add(e);
                    } catch (Exception ignored) { out.add(p); }
                } else out.add(p);
            } else out.add(p);
        }
        return out;
    }

    private static List<Path> sample(List<Path> pool, int n) {
        if (pool.size() <= n) return new ArrayList<>(pool);
        List<Path> c = new ArrayList<>(pool);
        Collections.shuffle(c, RNG);
        return c.subList(0, n);
    }

    private static Map<String, Integer> typeSummary(JCas cas, List<String> typeNames) throws Exception {
        CAS view = cas.getView("_InitialView").getCas();
        Map<String, Integer> counts = new LinkedHashMap<>();
        List<String> all = new ArrayList<>(typeNames);
        all.add("org.texttechnologylab.annotation.AnnotationComment");
        all.add("org.texttechnologylab.annotation.AnnotatorMetaData");
        all.add("org.texttechnologylab.annotation.DocumentModification");
        for (String tn : all) {
            Type t = view.getTypeSystem().getType(tn);
            if (t == null) continue;
            int c = 0;
            var it = view.getIndexRepository().getAllIndexedFS(t);
            while (it.hasNext()) { it.next(); c++; }
            if (c > 0) counts.put(tn.substring(tn.lastIndexOf('.') + 1), c);
        }
        return counts;
    }

    private static List<String> expectedMetrics(boolean streaming) {
        if (streaming) return List.of("duui.http.serialize_ms", "duui.http.request_bytes",
                "duui.http.response_decode_ms", "duui.http.request_duration_ms", "duui.http.response_bytes");
        return List.of("duui.http.serialize_ms", "duui.http.request_bytes",
                "duui.http.response_receive_ms", "duui.http.response_decode_ms",
                "duui.http.request_duration_ms", "duui.http.response_bytes");
    }

    private static List<DUUIEvent> waitForMetrics(DUUIInMemoryEventSink sink, String id, List<String> names) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        List<DUUIEvent> evts = filterByArtifact(sink.events(), id);
        while (!hasMetrics(evts, names) && System.nanoTime() < deadline) {
            Thread.sleep(20L);
            evts = filterByArtifact(sink.events(), id);
        }
        return evts;
    }

    private static List<DUUIEvent> filterByArtifact(List<DUUIEvent> evts, String aid) {
        return evts.stream().filter(e -> aid.equals(e.artifactId())).toList();
    }

    private static boolean hasMetrics(List<DUUIEvent> evts, List<String> names) {
        for (String n : names) {
            if (evts.stream().noneMatch(e -> n.equals(e.metricName()) && e.metricValue() != null)) return false;
        }
        return true;
    }

    private static String describe(DUUIOrchestrationResult result, String key, String image) {
        StringBuilder sb = new StringBuilder();
        result.results().stream().map(DUUIExecutionResult::failure).filter(Objects::nonNull)
                .forEach(f -> sb.append("  message=").append(f.message()).append(" cause=").append(f.cause()).append('\n'));
        return sb.toString();
    }

    private static String textKey(JCas cas) {
        String t = cas.getDocumentText();
        return Integer.toHexString(t == null ? 0 : t.hashCode()) + ":" + (t == null ? 0 : t.length());
    }

    private static String stem(Path p) {
        String n = p.getFileName().toString();
        if (n.endsWith(".xmi.gz")) return n.substring(0, n.length() - 7);
        if (n.endsWith(".xmi")) return n.substring(0, n.length() - 4);
        return n.replaceAll("[^A-Za-z0-9_.-]", "_");
    }

    private static final List<String> SPACY_OUTPUT_ROOT_TYPES = List.of(
            "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence",
            "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token",
            "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma",
            "de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS",
            "de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.morph.MorphologicalFeatures",
            "de.tudarmstadt.ukp.dkpro.core.api.syntax.type.dependency.Dependency",
            "de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity");

    private record ResultArtifact(String id, long durationMs, JCas cas, int characters,
                                  Map<String, Integer> typeCounts, List<DUUIEvent> events) {}
}
