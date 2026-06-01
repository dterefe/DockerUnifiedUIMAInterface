package org.texttechnologylab.duui.rework;

import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.FSIterator;
import org.apache.uima.cas.FeatureStructure;
import org.apache.uima.cas.SerialFormat;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.CasIOUtils;
import org.apache.uima.util.XMLInputSource;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPodmanDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIV1Driver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIMockStorageBackend;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelinePerformancePoint;
import org.texttechnologylab.duui.event.DUUIEvent;
import org.texttechnologylab.duui.event.DUUIEventService;
import org.texttechnologylab.duui.event.DUUIInMemoryEventSink;
import org.texttechnologylab.duui.exception.DUUIExecutionResult;
import org.texttechnologylab.duui.orchestration.DUUIOrchestrationResult;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIDispatchMode;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIDispatchPolicy;
import org.texttechnologylab.duui.pipeline.io.DUUIXmiCollectionReader;
import org.texttechnologylab.duui.runtime.DUUI;
import org.texttechnologylab.duui.runtime.DUUIGeneratorScope;
import org.texttechnologylab.duui.runtime.DUUIPipelineScope;
import org.texttechnologylab.duui.runtime.DUUIStageScope;
import org.texttechnologylab.duui.runtime.DUUISystemScope;
import org.texttechnologylab.duui.runtime.DUUIV1ComponentBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Opt-in DUUI-PY evaluation harness for comparing the v2 {@code DUUIOrchestrator}
 * path with the legacy {@code DUUIComposer} path.
 *
 * <p>Enable explicitly, for example:</p>
 * <pre>
 * mvn -pl duui-core -Dtest=org.texttechnologylab.duui.rework.DUUIPyAnnotatorEvaluationHarnessTest \
 *   -Dduui.py.eval.enabled=true \
 *   -Dduui.py.eval.annotator=gnfinder \
 *   -Dduui.py.eval.image=localhost/duui-py-gnfinder-async:pj-eval \
 *   -Dduui.py.eval.input.manifest=/tmp/duui-eval/input.txt \
 *   -Dduui.py.eval.output.dir=/tmp/duui-eval/out \
 *   test
 * </pre>
 */
class DUUIPyAnnotatorEvaluationHarnessTest {
    private static final String EXAMPLES = "../duui-py/examples";
    private static final String INITIAL_VIEW = "_InitialView";

    @TempDir
    Path tempDir;

    @Test
    void evaluateDuuiPyAnnotator() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("duui.py.eval.enabled"),
                "Set -Dduui.py.eval.enabled=true to run the DUUI-PY evaluation harness.");

        EvalConfig config = EvalConfig.fromProperties();
        Files.createDirectories(config.outputDir());

        TypeSystemDescription typeSystem = typeSystemFor(config.annotator());
        List<Path> documents = resolveDocuments(config);
        Assumptions.assumeFalse(documents.isEmpty(), "No input documents resolved for DUUI-PY evaluation.");

        List<ReportRow> rows = new ArrayList<>();
        if (config.runV2()) {
            rows.addAll(runV2(config, documents, typeSystem));
        }
        if (config.runLegacy()) {
            rows.addAll(runLegacyComposer(config, documents, typeSystem));
        }

        rows = coalesceRows(rows);
        writeReports(config, rows);
        List<ReportRow> failures = rows.stream().filter(row -> !row.success()).toList();
        assertFalse(rows.isEmpty(), "evaluation produced no rows");
        assertFalse(!failures.isEmpty(), () -> "DUUI-PY evaluation failures: " + failures);
    }

    private List<ReportRow> runV2(EvalConfig config, List<Path> documents, TypeSystemDescription typeSystem) throws Exception {
        Path input = materializeXmiDirectory("v2-" + config.annotator(), documents, typeSystem);
        DUUIInMemoryEventSink sink = new DUUIInMemoryEventSink();
        DUUIEventService events = new DUUIEventService(List.of(sink));
        DUUIOrchestrationResult result;
        long started = System.nanoTime();

        try (DUUISystemScope system = DUUI.system("duui-py-eval-v2-" + config.annotator()).events(events)) {
            try (DUUIPipelineScope pipeline = system.pipeline("duui-py-eval-v2")) {
                try (DUUIGeneratorScope<JCas> source = DUUIXmiCollectionReader.builder()
                        .typeSystem(typeSystem)
                        .source(input)
                        .open(pipeline)) {
                    try (DUUIStageScope<JCas> stage = source.linear("annotate-" + config.annotator())) {
                        stage.dispatchPolicy(dispatchPolicy(config));
                        DUUIV1ComponentBuilder component = stage.v1(config.annotator())
                                .podman()
                                .image(config.image())
                                .imageFetching(config.imageFetching())
                                .sourceView(INITIAL_VIEW)
                                .targetView(INITIAL_VIEW)
                                .telemetrySink(sink)
                                .parameters(config.parameters())
                                .timeoutSeconds(config.timeoutSeconds())
                                .gpu(config.gpu())
                                .scale(config.replicas())
                                .concurrency(config.concurrency())
                                .virtualThreads(config.scheduling() == SchedulingMode.VIRTUAL);
                        component.streamingTransport(config.streaming());
                        if (!config.streaming() && !config.contentType().isBlank()) {
                            component.contentType(config.contentType());
                        }
                    }
                }
            }
            result = system.run("duui-py-eval-v2");
        }

        long wallMs = Duration.ofNanos(System.nanoTime() - started).toMillis();
        List<ReportRow> rows = new ArrayList<>();
        Map<String, Path> inputByText = inputTextKeys(documents, typeSystem);
        for (DUUIExecutionResult<?> execution : result.results()) {
            if (!(execution.artifact().payload() instanceof JCas cas)) {
                continue;
            }
            String artifactId = execution.artifact().gid().toString();
            rows.add(row(config, "v2", inputByText.getOrDefault(textKey(cas), Path.of("unknown")),
                    true, null, execution.durationMs(), wallMs, cas, sink.events(), artifactId));
        }
        for (DUUIExecutionResult<?> execution : result.results()) {
            if (execution.failure() != null) {
                rows.add(errorRow(config, "v2", Path.of("unknown"), execution.durationMs(), wallMs,
                        execution.failure().message(), execution.failure().cause()));
            }
        }
        if (rows.isEmpty() && (result.hasFailures() || !result.unroutableArtifacts().isEmpty())) {
            rows.add(errorRow(config, "v2", Path.of("unknown"), 0L, wallMs,
                    "orchestrator failed without JCas result", result.toString()));
        }
        return rows;
    }

    private List<ReportRow> runLegacyComposer(EvalConfig config, List<Path> documents, TypeSystemDescription typeSystem) throws Exception {
        List<ReportRow> rows = new ArrayList<>();
        DUUIComposer composer = null;
        DUUIMockStorageBackend storage = new DUUIMockStorageBackend();
        long suiteStarted = System.nanoTime();
        try {
            DUUIV1Driver driver = (DUUIV1Driver) new DUUIPodmanDriver()
                    .withV1Transport(config.streaming(), config.contentType())
                    .withVirtualThreads(config.scheduling() == SchedulingMode.VIRTUAL);
            composer = new DUUIComposer()
                    .withLuaContext(new DUUILuaContext().withJsonLibrary())
                    .withSkipVerification(false)
                    .withWorkers(config.concurrency())
                    .withCasPoolsize(config.concurrency())
                    .withStorageBackend(storage);
            composer.addDriver(driver);

            DUUIPodmanDriver.Component component = new DUUIPodmanDriver.Component(config.image())
                    .withName(config.annotator())
                    .withSourceView(INITIAL_VIEW)
                    .withTargetView(INITIAL_VIEW)
                    .withGPU(config.gpu())
                    .withScale(config.replicas())
                    .withWorkers(config.concurrency())
                    .withTimeout(config.timeoutSeconds())
                    .withImageFetching(config.imageFetching());
            for (Map.Entry<String, String> entry : config.parameters().entrySet()) {
                component.withParameter(entry.getKey(), entry.getValue());
            }
            composer.add(component.build());

            Path input = materializeXmiDirectory("legacy-" + config.annotator(), documents, typeSystem);
            AsyncCollectionReader reader = new AsyncCollectionReader(
                    input.toString(), ".xmi", documents.size(), false);
            reader.withMaxMemorySize(Long.MAX_VALUE);

            long started = System.nanoTime();
            try {
                composer.run(reader, "duui-py-eval-legacy-" + config.annotator());
                long durationMs = Duration.ofNanos(System.nanoTime() - started).toMillis();
                rows.add(aggregateRow(config, "legacy", documents, true, null, durationMs,
                        Duration.ofNanos(System.nanoTime() - suiteStarted).toMillis(), typeSystem, legacyMetrics(storage)));
            } catch (Exception e) {
                long durationMs = Duration.ofNanos(System.nanoTime() - started).toMillis();
                rows.add(aggregateRow(config, "legacy", documents, false, e.getMessage() + " cause=" + e,
                        durationMs, Duration.ofNanos(System.nanoTime() - suiteStarted).toMillis(), typeSystem, legacyMetrics(storage)));
            }
        } finally {
            if (composer != null) {
                composer.shutdown();
            }
        }
        return rows;
    }

    private static ReportRow aggregateRow(
            EvalConfig config,
            String engine,
            List<Path> documents,
            boolean success,
            String error,
            long durationMs,
            long wallMs,
            TypeSystemDescription typeSystem,
            Map<String, Double> metrics
    ) throws Exception {
        int characters = 0;
        for (Path document : documents) {
            JCas cas = loadXmi(typeSystem, document);
            characters += cas.getDocumentText() == null ? 0 : cas.getDocumentText().length();
        }
        return new ReportRow(
                config.annotator(),
                engine,
                config.scheduling().propertyValue,
                config.mode().propertyValue,
                config.image(),
                config.replicas(),
                config.concurrency(),
                "__collection__/" + documents.size() + "-documents",
                characters,
                durationMs,
                wallMs,
                success,
                error == null ? "" : error,
                Map.of("documents", documents.size()),
                metrics
        );
    }

    private static Map<String, Double> legacyMetrics(DUUIMockStorageBackend storage) {
        Map<String, Double> metrics = new LinkedHashMap<>();
        for (var performances : storage.getPerformanceMonitoring().values()) {
            for (DUUIPipelineDocumentPerformance performance : performances) {
                for (DUUIPipelinePerformancePoint point : performance.getPerformancePoints()) {
                    mergeNanosAsMillis(metrics, "legacy.serialize_ms", point.getDurationSerialize());
                    mergeNanosAsMillis(metrics, "legacy.process_ms", point.getDurationAnnotator());
                    mergeNanosAsMillis(metrics, "legacy.deserialize_ms", point.getDurationDeserialize());
                    mergeNanosAsMillis(metrics, "legacy.component_total_ms", point.getDurationComponentTotal());
                    if (point.getSerializedSize() != null) {
                        metrics.merge("legacy.request_bytes", point.getSerializedSize().doubleValue(), Double::sum);
                    }
                }
            }
        }
        return metrics;
    }

    private static void mergeNanosAsMillis(Map<String, Double> metrics, String key, Long nanos) {
        if (nanos != null) {
            metrics.merge(key, nanos / 1_000_000.0, Double::sum);
        }
    }

    private Path materializeXmiDirectory(String id, List<Path> documents, TypeSystemDescription typeSystem) throws Exception {
        Path input = tempDir.resolve(id + "-input");
        Files.createDirectories(input);
        for (int index = 0; index < documents.size(); index++) {
            JCas cas = loadXmi(typeSystem, documents.get(index));
            Path output = input.resolve(String.format(Locale.ROOT, "%04d-%s.xmi", index, stem(documents.get(index))));
            try (OutputStream stream = Files.newOutputStream(output)) {
                CasIOUtils.save(cas.getCas(), stream, SerialFormat.XMI_1_1);
            }
        }
        return input;
    }

    private static JCas loadXmi(TypeSystemDescription typeSystem, Path path) throws Exception {
        JCas cas = JCasFactory.createJCas(typeSystem);
        try (InputStream file = Files.newInputStream(path);
             InputStream input = path.getFileName().toString().endsWith(".gz") ? new GZIPInputStream(file) : file) {
            XmiCasDeserializer.deserialize(input, cas.getCas(), true);
        }
        if (cas.getDocumentLanguage() == null || cas.getDocumentLanguage().isBlank()) {
            cas.setDocumentLanguage("de");
        }
        return cas;
    }

    private static Map<String, Path> inputTextKeys(List<Path> documents, TypeSystemDescription typeSystem) throws Exception {
        Map<String, Path> keys = new LinkedHashMap<>();
        for (Path document : documents) {
            keys.put(textKey(loadXmi(typeSystem, document)), document);
        }
        return keys;
    }

    private static DUUIDispatchPolicy dispatchPolicy(EvalConfig config) {
        DUUIDispatchMode mode = switch (config.scheduling()) {
            case PLATFORM -> DUUIDispatchMode.CPU;
            case VIRTUAL -> DUUIDispatchMode.IO;
            case MIXED -> DUUIDispatchMode.MIXED;
        };
        return DUUIDispatchPolicy.of(mode, config.concurrency());
    }

    private static ReportRow row(
            EvalConfig config,
            String engine,
            Path document,
            boolean success,
            String error,
            long durationMs,
            long wallMs,
            JCas cas,
            List<DUUIEvent> events,
            String artifactId
    ) throws Exception {
        Map<String, Integer> types = typeSummary(cas, countTypesFor(config.annotator()));
        return new ReportRow(
                config.annotator(),
                engine,
                config.scheduling().propertyValue,
                config.mode().propertyValue,
                config.image(),
                config.replicas(),
                config.concurrency(),
                document.toString(),
                cas.getDocumentText() == null ? 0 : cas.getDocumentText().length(),
                durationMs,
                wallMs,
                success,
                error == null ? "" : error,
                types,
                metricSummary(events, artifactId)
        );
    }

    private static ReportRow errorRow(
            EvalConfig config,
            String engine,
            Path document,
            long durationMs,
            long wallMs,
            String message,
            Object cause
    ) {
        String error = Objects.toString(message, "");
        if (cause != null) {
            error = error.isBlank() ? cause.toString() : error + " cause=" + cause;
        }
        return new ReportRow(
                config.annotator(),
                engine,
                config.scheduling().propertyValue,
                config.mode().propertyValue,
                config.image(),
                config.replicas(),
                config.concurrency(),
                document.toString(),
                0,
                durationMs,
                wallMs,
                false,
                error,
                Map.of(),
                Map.of()
        );
    }

    private static Map<String, Integer> typeSummary(JCas cas, List<String> typeNames) throws Exception {
        CAS view = cas.getView(INITIAL_VIEW).getCas();
        Map<String, Integer> counts = new LinkedHashMap<>();
        List<String> allTypes = new ArrayList<>(typeNames);
        allTypes.add("org.texttechnologylab.annotation.AnnotationComment");
        allTypes.add("org.texttechnologylab.annotation.AnnotatorMetaData");
        allTypes.add("org.texttechnologylab.annotation.DocumentModification");
        for (String typeName : allTypes) {
            Type type = view.getTypeSystem().getType(typeName);
            if (type == null) {
                continue;
            }
            int count = countIndexed(view, type);
            if (count > 0) {
                counts.put(typeName.substring(typeName.lastIndexOf('.') + 1), count);
            }
        }
        return counts;
    }

    private static int countIndexed(CAS view, Type type) {
        int count = 0;
        FSIterator<FeatureStructure> iterator = view.getIndexRepository().getAllIndexedFS(type);
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        return count;
    }

    private static Map<String, Double> metricSummary(List<DUUIEvent> events, String artifactId) {
        Map<String, Double> metrics = new LinkedHashMap<>();
        if (artifactId == null || artifactId.isBlank()) {
            return metrics;
        }
        for (DUUIEvent event : events) {
            if (!artifactId.equals(event.artifactId()) || event.metricName() == null || event.metricValue() == null) {
                continue;
            }
            metrics.merge(event.metricName(), event.metricValue().doubleValue(), Double::sum);
        }
        return metrics;
    }

    private static void writeReports(EvalConfig config, List<ReportRow> rows) throws IOException {
        writeCsv(config.outputDir().resolve("duui-py-eval.csv"), rows);
        writeJsonl(config.outputDir().resolve("duui-py-eval.jsonl"), rows);
        writeMarkdown(config.outputDir().resolve("duui-py-eval.md"), rows);
    }

    private static List<ReportRow> coalesceRows(List<ReportRow> rows) {
        Map<String, ReportRow> merged = new LinkedHashMap<>();
        for (ReportRow row : rows) {
            String key = row.annotator() + "\0" + row.engine() + "\0" + row.scheduling()
                    + "\0" + row.mode() + "\0" + row.document() + "\0" + row.success();
            ReportRow previous = merged.get(key);
            if (previous == null || row.durationMs() > previous.durationMs()) {
                merged.put(key, row);
            }
        }
        return new ArrayList<>(merged.values());
    }

    private static void writeCsv(Path file, List<ReportRow> rows) throws IOException {
        StringBuilder out = new StringBuilder();
        out.append("annotator,engine,scheduling,mode,image,replicas,concurrency,document,characters,duration_ms,wall_ms,success,error,type_counts,metrics\n");
        for (ReportRow row : rows) {
            out.append(csv(row.annotator())).append(',')
                    .append(csv(row.engine())).append(',')
                    .append(csv(row.scheduling())).append(',')
                    .append(csv(row.mode())).append(',')
                    .append(csv(row.image())).append(',')
                    .append(row.replicas()).append(',')
                    .append(row.concurrency()).append(',')
                    .append(csv(row.document())).append(',')
                    .append(row.characters()).append(',')
                    .append(row.durationMs()).append(',')
                    .append(row.wallMs()).append(',')
                    .append(row.success()).append(',')
                    .append(csv(row.error())).append(',')
                    .append(csv(row.typeCounts().toString())).append(',')
                    .append(csv(row.metrics().toString())).append('\n');
        }
        Files.writeString(file, out.toString(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private static void writeJsonl(Path file, List<ReportRow> rows) throws IOException {
        StringBuilder out = new StringBuilder();
        for (ReportRow row : rows) {
            out.append(row.toJson()).append('\n');
        }
        Files.writeString(file, out.toString(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private static void writeMarkdown(Path file, List<ReportRow> rows) throws IOException {
        long failures = rows.stream().filter(row -> !row.success()).count();
        StringBuilder out = new StringBuilder();
        out.append("# DUUI-PY Evaluation\n\n");
        out.append("- rows: ").append(rows.size()).append('\n');
        out.append("- failures: ").append(failures).append("\n\n");
        out.append("| annotator | engine | scheduling | mode | document | duration_ms | success | type_counts |\n");
        out.append("| --- | --- | --- | --- | --- | ---: | --- | --- |\n");
        for (ReportRow row : rows) {
            out.append("| ").append(md(row.annotator()))
                    .append(" | ").append(md(row.engine()))
                    .append(" | ").append(md(row.scheduling()))
                    .append(" | ").append(md(row.mode()))
                    .append(" | ").append(md(Path.of(row.document()).getFileName() == null ? row.document() : Path.of(row.document()).getFileName().toString()))
                    .append(" | ").append(row.durationMs())
                    .append(" | ").append(row.success())
                    .append(" | ").append(md(row.typeCounts().toString()))
                    .append(" |\n");
        }
        Files.writeString(file, out.toString(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private static String csv(String value) {
        String escaped = value == null ? "" : value.replace("\"", "\"\"");
        return "\"" + escaped + "\"";
    }

    private static String md(String value) {
        return (value == null ? "" : value).replace("|", "\\|").replace("\n", " ");
    }

    private static String json(String value) {
        StringBuilder out = new StringBuilder("\"");
        String text = value == null ? "" : value;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\b' -> out.append("\\b");
                case '\f' -> out.append("\\f");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.append('"').toString();
    }

    private static TypeSystemDescription typeSystemFor(String annotator) throws Exception {
        return switch (annotator) {
            case "spacy" -> localExampleTypeSystem(
                    "spacy-async/TypeSystemSpacy.xml");
            case "taxonerd" -> localExampleTypeSystem(
                    "taxonerd-async/TypeSystemTaxoNERD.xml");
            case "gazetteer" -> localExampleTypeSystem(
                    "gazetteer-async/TypeSystemGazetteer.xml");
            case "gnfinder" -> localExampleTypeSystem(
                    "gnfinder-async/TypeSystemGNFinder.xml");
            default -> throw new IllegalArgumentException("Unsupported annotator: " + annotator);
        };
    }

    private static TypeSystemDescription localExampleTypeSystem(String... relativePaths) throws Exception {
        List<TypeSystemDescription> descriptions = new ArrayList<>();
        descriptions.add(TypeSystemDescriptionFactory.createTypeSystemDescription());
        for (String relative : relativePaths) {
            Path file = Path.of(EXAMPLES, relative);
            if (!Files.exists(file)) {
                continue;
            }
            descriptions.add(UIMAFramework.getXMLParser().parseTypeSystemDescription(new XMLInputSource(file.toFile())));
        }
        return CasCreationUtils.mergeTypeSystems(descriptions);
    }

    private static List<String> countTypesFor(String annotator) {
        return switch (annotator) {
            case "spacy" -> List.of(
                    "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence",
                    "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token",
                    "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma",
                    "de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS",
                    "de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.morph.MorphologicalFeatures",
                    "de.tudarmstadt.ukp.dkpro.core.api.syntax.type.dependency.Dependency",
                    "de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity");
            case "taxonerd", "gazetteer" -> List.of("org.texttechnologylab.annotation.type.Taxon");
            case "gnfinder" -> List.of(
                    "org.texttechnologylab.annotation.type.Taxon",
                    "org.texttechnologylab.annotation.biofid.Taxon",
                    "org.texttechnologylab.annotation.biofid.gnfinder.Taxon");
            default -> List.of();
        };
    }

    private static List<Path> resolveDocuments(EvalConfig config) throws IOException {
        List<Path> documents;
        if (config.inputManifest() != null) {
            documents = Files.readAllLines(config.inputManifest(), StandardCharsets.UTF_8).stream()
                    .map(String::trim)
                    .filter(line -> !line.isBlank() && !line.startsWith("#"))
                    .flatMap(line -> Arrays.stream(line.split(",")))
                    .map(String::trim)
                    .map(DUUIPyAnnotatorEvaluationHarnessTest::manifestPathToken)
                    .filter(value -> !value.isBlank())
                    .map(Path::of)
                    .toList();
        } else {
            documents = paths(System.getProperty("duui.py.eval.input", defaultDocumentPaths(config.annotator())));
        }
        documents = expandGlobs(documents).stream().filter(Files::isRegularFile).toList();
        if (config.mode() == EvalMode.SMOKE && documents.size() > 1) {
            return List.of(documents.get(0));
        }
        int maxDocuments = Integer.getInteger("duui.py.eval.max.documents", config.mode() == EvalMode.SMOKE ? 1 : Integer.MAX_VALUE);
        if (documents.size() > maxDocuments) {
            return documents.subList(0, maxDocuments);
        }
        return documents;
    }

    private static List<Path> paths(String configured) {
        return Arrays.stream(configured.split(","))
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .map(Path::of)
                .toList();
    }

    private static List<Path> expandGlobs(List<Path> paths) {
        List<Path> expanded = new ArrayList<>();
        for (Path path : paths) {
            String fileName = path.getFileName() == null ? "" : path.getFileName().toString();
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
                    } catch (IOException ignored) {
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

    private static String manifestPathToken(String value) {
        if (value.indexOf('\t') >= 0) {
            String[] parts = value.split("\t");
            return parts[parts.length - 1].trim();
        }
        return value;
    }

    private static String defaultDocumentPaths(String annotator) {
        return switch (annotator) {
            case "spacy" -> "/home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/preprocessed_corpora_sample_1000/Ornithologische_Mitteilungen_Monatsschrift_für_Vogelbeobachtung__Feldornithologie_und_Avifaunistik/1999/10778329.xmi.gz";
            case "taxonerd", "gazetteer", "gnfinder" -> "/home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/preprocessed_corpora_sample_1000/General-Doubletten-Verzeichniss_des_Schlesischen_Botanischen_Tausch-Vereins_____Tauschjahr____/1883/4513701.xmi.gz";
            case "geonames" -> "/home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/preprocessed_corpora_sample_1000/Bericht_des_Vereins_zum_Schutze_der_Alpenpflanzen/1913/3713536.xmi.gz";
            default -> "";
        };
    }

    private static String defaultImage(String annotator) {
        return switch (annotator) {
            case "spacy" -> "localhost/duui-py-spacy-async:pj-eval";
            case "taxonerd" -> "localhost/duui-py-taxonerd-async:pj-eval";
            case "gazetteer" -> "localhost/duui-py-gazetteer-async:pj-eval";
            case "gnfinder" -> "localhost/duui-py-gnfinder-async:pj-eval";
            default -> "";
        };
    }

    private static Map<String, String> defaultParameters(String annotator) {
        return switch (annotator) {
            case "spacy" -> Map.of(
                    "spacy_model_size", System.getProperty("duui.py.spacy.model_size", "trf"),
                    "spacy_batch_size", System.getProperty("duui.py.spacy.batch_size", "32"),
                    "use_existing_sentences", "false",
                    "spacy_language", "de");
            case "taxonerd" -> Map.of(
                    "model", System.getProperty("duui.py.taxonerd.model", "en_ner_eco_md"),
                    "linking", System.getProperty("duui.py.taxonerd.linking", "gbif_backbone"),
                    "threshold", System.getProperty("duui.py.taxonerd.threshold", "0.7"),
                    "input_strategy", System.getProperty("duui.py.taxonerd.input_strategy", "legacy-procedure"),
                    "linker_strategy", System.getProperty("duui.py.taxonerd.linker_strategy", "ann-original"),
                    "allow_unlinked", System.getProperty("duui.py.taxonerd.allow_unlinked", "false"),
                    "prefer_gpu", System.getProperty("duui.py.taxonerd.prefer_gpu", "true"),
                    "timeout", System.getProperty("duui.py.taxonerd.timeout", "600"));
            case "gazetteer" -> Map.of("timeout", System.getProperty("duui.py.gazetteer.timeout", "120"));
            case "gnfinder" -> Map.of(
                    "lang", System.getProperty("duui.py.gnfinder.lang", "de"),
                    "verify", System.getProperty("duui.py.gnfinder.verify", "true"),
                    "utf8_input", System.getProperty("duui.py.gnfinder.utf8_input", "true"));
            default -> Map.of();
        };
    }

    private static Map<String, String> parametersFor(String annotator) {
        Map<String, String> parameters = new LinkedHashMap<>(defaultParameters(annotator));
        String flat = System.getProperty("duui.py.eval.parameters", "").trim();
        if (!flat.isBlank()) {
            for (String pair : flat.split(",")) {
                int split = pair.indexOf('=');
                if (split > 0) {
                    parameters.put(pair.substring(0, split).trim(), pair.substring(split + 1).trim());
                }
            }
        }
        return parameters;
    }

    private static String textKey(JCas cas) {
        String text = cas.getDocumentText();
        return Integer.toHexString(text == null ? 0 : text.hashCode()) + ":" + (text == null ? 0 : text.length());
    }

    private static String stem(Path path) {
        String name = path.getFileName().toString();
        if (name.endsWith(".xmi.gz")) {
            return name.substring(0, name.length() - 7);
        }
        if (name.endsWith(".xmi")) {
            return name.substring(0, name.length() - 4);
        }
        return name.replaceAll("[^A-Za-z0-9_.-]", "_");
    }

    private enum EvalMode {
        SMOKE("smoke"),
        FULL("full");

        private final String propertyValue;

        EvalMode(String propertyValue) {
            this.propertyValue = propertyValue;
        }

        static EvalMode parse(String value) {
            return "full".equalsIgnoreCase(value) ? FULL : SMOKE;
        }
    }

    private enum SchedulingMode {
        PLATFORM("platform"),
        VIRTUAL("virtual"),
        MIXED("mixed");

        private final String propertyValue;

        SchedulingMode(String propertyValue) {
            this.propertyValue = propertyValue;
        }

        static SchedulingMode parse(String value) {
            return switch ((value == null ? "mixed" : value).toLowerCase(Locale.ROOT)) {
                case "platform", "cpu" -> PLATFORM;
                case "virtual", "io" -> VIRTUAL;
                default -> MIXED;
            };
        }
    }

    private record EvalConfig(
            String annotator,
            String image,
            SchedulingMode scheduling,
            int replicas,
            int concurrency,
            Path inputManifest,
            Path outputDir,
            EvalMode mode,
            String engine,
            boolean streaming,
            String contentType,
            boolean gpu,
            boolean imageFetching,
            long timeoutSeconds,
            Map<String, String> parameters
    ) {
        static EvalConfig fromProperties() {
            String annotator = System.getProperty("duui.py.eval.annotator", "gnfinder").trim().toLowerCase(Locale.ROOT);
            String image = System.getProperty("duui.py.eval.image", defaultImage(annotator)).trim();
            String output = System.getProperty("duui.py.eval.output.dir", "").trim();
            if (output.isBlank()) {
                throw new IllegalArgumentException("Set -Dduui.py.eval.output.dir to a directory outside committed reports.");
            }
            String manifest = System.getProperty("duui.py.eval.input.manifest", "").trim();
            return new EvalConfig(
                    annotator,
                    image,
                    SchedulingMode.parse(System.getProperty("duui.py.eval.scheduling", "mixed")),
                    Math.max(1, Integer.getInteger("duui.py.eval.replicas", 1)),
                    Math.max(1, Integer.getInteger("duui.py.eval.concurrency", 1)),
                    manifest.isBlank() ? null : Path.of(manifest),
                    Path.of(output),
                    EvalMode.parse(System.getProperty("duui.py.eval.mode", "smoke")),
                    System.getProperty("duui.py.eval.engine", "both").trim().toLowerCase(Locale.ROOT),
                    Boolean.parseBoolean(System.getProperty("duui.py.eval.streaming", "true")),
                    System.getProperty("duui.py.eval.content.type", "application/octet-stream"),
                    Boolean.parseBoolean(System.getProperty("duui.py.eval.gpu", defaultGpu(annotator))),
                    Boolean.getBoolean("duui.py.eval.image.fetching"),
                    Long.getLong("duui.py.eval.timeout.seconds", 7200L),
                    parametersFor(annotator)
            );
        }

        boolean runV2() {
            return engine.equals("both") || engine.equals("v2") || engine.equals("orchestrator");
        }

        boolean runLegacy() {
            return engine.equals("both") || engine.equals("legacy") || engine.equals("composer");
        }
    }

    private static String defaultGpu(String annotator) {
        return switch (annotator) {
            case "spacy", "taxonerd" -> "true";
            default -> "false";
        };
    }

    private record ReportRow(
            String annotator,
            String engine,
            String scheduling,
            String mode,
            String image,
            int replicas,
            int concurrency,
            String document,
            int characters,
            long durationMs,
            long wallMs,
            boolean success,
            String error,
            Map<String, Integer> typeCounts,
            Map<String, Double> metrics
    ) {
        String toJson() {
            return "{"
                    + "\"annotator\":" + json(annotator)
                    + ",\"engine\":" + json(engine)
                    + ",\"scheduling\":" + json(scheduling)
                    + ",\"mode\":" + json(mode)
                    + ",\"image\":" + json(image)
                    + ",\"replicas\":" + replicas
                    + ",\"concurrency\":" + concurrency
                    + ",\"document\":" + json(document)
                    + ",\"characters\":" + characters
                    + ",\"durationMs\":" + durationMs
                    + ",\"wallMs\":" + wallMs
                    + ",\"success\":" + success
                    + ",\"error\":" + json(error)
                    + ",\"typeCounts\":" + intMapJson(typeCounts)
                    + ",\"metrics\":" + doubleMapJson(metrics)
                    + "}";
        }

        private static String intMapJson(Map<String, Integer> map) {
            StringBuilder out = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                if (!first) {
                    out.append(',');
                }
                first = false;
                out.append(json(entry.getKey())).append(':').append(entry.getValue());
            }
            return out.append('}').toString();
        }

        private static String doubleMapJson(Map<String, Double> map) {
            StringBuilder out = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<String, Double> entry : map.entrySet()) {
                if (!first) {
                    out.append(',');
                }
                first = false;
                out.append(json(entry.getKey())).append(':').append(String.format(Locale.ROOT, "%.3f", entry.getValue()));
            }
            return out.append('}').toString();
        }
    }
}
