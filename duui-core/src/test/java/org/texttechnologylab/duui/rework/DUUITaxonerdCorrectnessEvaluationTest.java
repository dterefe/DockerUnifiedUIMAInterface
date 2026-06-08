package org.texttechnologylab.duui.rework;

import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.Feature;
import org.apache.uima.cas.FeatureStructure;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.text.AnnotationFS;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.XMLInputSource;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.artifact.DUUIArtifactEmitter;
import org.texttechnologylab.duui.event.DUUIEvent;
import org.texttechnologylab.duui.event.DUUIEventService;
import org.texttechnologylab.duui.event.DUUIEventSink;
import org.texttechnologylab.duui.event.DUUIEventType;
import org.texttechnologylab.duui.event.DUUIInMemoryEventSink;
import org.texttechnologylab.duui.exception.DUUIExecutionResult;
import org.texttechnologylab.duui.orchestration.DUUIOrchestrationResult;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIDispatchMode;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIDispatchPolicy;
import org.texttechnologylab.duui.pipeline.DUUIGenerator;
import org.texttechnologylab.duui.runtime.DUUI;
import org.texttechnologylab.duui.runtime.DUUIGeneratorScope;
import org.texttechnologylab.duui.runtime.DUUIPipelineScope;
import org.texttechnologylab.duui.runtime.DUUIStageScope;
import org.texttechnologylab.duui.runtime.DUUISystemScope;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertFalse;

class DUUITaxonerdCorrectnessEvaluationTest {
    private static final String TAXON_TYPE = "org.texttechnologylab.annotation.type.Taxon";
    private static final String DOCUMENT_MODIFICATION_TYPE = "org.texttechnologylab.annotation.DocumentModification";
    private static final String SENTENCE_TYPE = "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence";
    private static final String TOKEN_TYPE = "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token";
    private static final String POS_TYPE = "de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS";
    private static final String PARAGRAPH_TYPE = "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph";
    private static final String DIV_TYPE = "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Div";
    private static final String HUCOMPUTE_DIV_TYPE = "org.hucompute.textimager.uima.type.segmentation.Div";
    private static final String SECTION_TYPE = "org.texttechnologylab.annotation.paper.Section";
    private static final String TITLE_TYPE = "org.texttechnologylab.annotation.paper.Title";
    private static final String OCR_PARAGRAPH_TYPE = "org.texttechnologylab.annotation.ocr.OCRParagraph";
    private static final String ABBYY_PARAGRAPH_TYPE = "org.texttechnologylab.annotation.ocr.abbyy.Paragraph";
    private static final String NAMED_ENTITY_TYPE = "de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity";
    private static final String TTLAB_NAMED_ENTITY_TYPE = "org.texttechnologylab.annotation.NamedEntity";
    private static final String TTLAB_TEXTTECHNOLOGY_NAMED_ENTITY_TYPE = "org.texttechnologylab.annotation.type.TexttechnologyNamedEntity";
    private static final Pattern SCIENTIFIC_NAME = Pattern.compile("\\b[A-ZÄÖÜ][a-zäöüß]+\\s+[a-zäöüß-]+\\b");
    private static final HttpClient CLIENT = HttpClient.newHttpClient();

    @Test
    void smallTaxonerdCorrectnessEvaluation() throws Exception {
        List<Variant> variants = variants();
        List<DocumentCase> documents = documents();
        List<ResultRow> rows = new ArrayList<>();
        emit("INFO", "STATUS", "evaluation started", Map.of(
                "variants", variants.stream().map(Variant::id).toList(),
                "documents", documents.stream().map(DocumentCase::id).toList()
        ));
        for (Variant variant : variants) {
            emit("INFO", "STATUS", "variant started", Map.of(
                    "variant", variant.id(),
                    "endpoint", variant.endpoint().toString(),
                    "parameters", variant.parameters()
            ));
            assertHealthy(variant.endpoint());
            emit("DEBUG", "STATUS", "annotator endpoint healthy", Map.of("variant", variant.id(), "endpoint", variant.endpoint().toString()));
            if (variant.requiresSpacy()) {
                assertHealthy(spacyEndpoint());
                emit("DEBUG", "STATUS", "spaCy prerequisite endpoint healthy", Map.of("variant", variant.id(), "endpoint", spacyEndpoint().toString()));
            }
            TypeSystemDescription typeSystem = variant.requiresSpacy()
                    ? mergedRemoteTypeSystem(spacyEndpoint(), variant.endpoint())
                    : mergedRemoteTypeSystem(variant.endpoint());
            emit("DEBUG", "STATUS", "remote type system loaded", Map.of("variant", variant.id(), "endpoint", variant.endpoint().toString()));
            for (DocumentCase document : documents) {
                DUUIInMemoryEventSink events = new DUUIInMemoryEventSink();
                JCas cas = JCasFactory.createJCas(typeSystem);
                emit("INFO", "STATUS", "document preparing", Map.of(
                        "variant", variant.id(),
                        "document", document.id(),
                        "input", document.xmiPath() == null ? "text" : "xmi",
                        "expected", document.expected().size()
                ));
                if (document.xmiPath() == null) {
                    cas.setDocumentLanguage(System.getProperty("duui.py.taxonerd.language", "de"));
                    cas.setDocumentText(document.text());
                    emit("DEBUG", "LOG", "text document loaded into CAS", Map.of(
                            "variant", variant.id(),
                            "document", document.id(),
                            "characters", document.text().length()
                    ));
                } else {
                    loadXmi(document.xmiPath(), cas);
                    emit("DEBUG", "LOG", "XMI document loaded into CAS", Map.of(
                            "variant", variant.id(),
                            "document", document.id(),
                            "path", document.xmiPath().toString(),
                            "characters", cas.getDocumentText() == null ? 0 : cas.getDocumentText().length()
                    ));
                }
                InputStats inputStats = inputStats(cas);
                emit("DEBUG", "METRIC", "input CAS annotation counts", Map.of(
                        "variant", variant.id(),
                        "document", document.id(),
                        "characters", inputStats.characters(),
                        "sentences", inputStats.sentences(),
                        "tokens", inputStats.tokens(),
                        "pos", inputStats.pos(),
                        "namedEntities", inputStats.namedEntities(),
                        "existingTaxons", inputStats.taxons()
                ));
                seedRequiredAnnotations(cas, variant, document);
                long started = System.nanoTime();
                emit("INFO", "STATUS", "DUUI document run started", Map.of("variant", variant.id(), "document", document.id()));
                DUUIOrchestrationResult result = runVariant(variant, document, cas, events);
                long elapsedMs = Duration.ofNanos(System.nanoTime() - started).toMillis();
                for (DUUIEvent event : events.events()) {
                    emitEvent(variant, document, event);
                }
                JCas processed = processedJCas(result, cas);
                TaxonStats stats = taxonStats(processed);
                List<String> found = stats.covered();
                List<String> missing = document.expected().stream()
                        .filter(expected -> !found.contains(expected))
                        .toList();
                boolean failed = result.hasFailures() || (variant.usesBackbone() && !found.isEmpty() && stats.linked() < found.size());
                rows.add(new ResultRow(
                        variant.id(),
                        document.id(),
                        document.expected().size(),
                        found.size(),
                        stats.linked(),
                        missing.size(),
                        elapsedMs,
                        failed,
                        events.events().stream().filter(event -> event.type().name().equals("METRIC")).count(),
                        metricValue(events, processed, "taxonerd_linker_exact_ms"),
                        metricValue(events, processed, "taxonerd_linker_ann_ms"),
                        metricValue(events, processed, "taxonerd_linker_exact_matches"),
                        metricValue(events, processed, "taxonerd_linker_ann_mentions"),
                        metricValue(events, processed, "taxonerd_linker_cache_hits"),
                        metricValue(events, processed, "taxonerd_linker_cache_misses"),
                        metricValue(events, processed, "taxonerd_linker_fuseki_aliases"),
                        metricValue(events, processed, "taxonerd_linker_fuseki_matches"),
                        metricValue(events, processed, "taxonerd_linker_fuseki_errors"),
                        metricValue(events, processed, "taxonerd_linker_fuseki_ms"),
                        inputStats.characters(),
                        inputStats.sentences(),
                        inputStats.tokens(),
                        inputStats.pos(),
                        inputStats.namedEntities(),
                        inputStats.taxons(),
                        String.join("; ", missing),
                        String.join("; ", found),
                        String.join("; ", stats.linkedText())
                ));
                emit("METRIC", "METRIC", "document latency", Map.of(
                        "variant", variant.id(),
                        "document", document.id(),
                        "metricName", "duui.document.latency_ms",
                        "metricValue", elapsedMs,
                        "metricUnit", "ms"
                ));
                Map<String, Object> completed = new LinkedHashMap<>();
                completed.put("variant", variant.id());
                completed.put("document", document.id());
                completed.put("elapsedMs", elapsedMs);
                completed.put("found", found.size());
                completed.put("linked", stats.linked());
                completed.put("missing", missing.size());
                completed.put("failed", failed);
                completed.put("metricEvents", events.events().stream().filter(event -> event.type().name().equals("METRIC")).count());
                completed.put("foundText", String.join("; ", found));
                completed.put("linkedText", String.join("; ", stats.linkedText()));
                completed.put("missingText", String.join("; ", missing));
                emit(failed ? "ERROR" : "INFO", failed ? "ERROR" : "STATUS", "DUUI document run completed", completed);
            }
            emit("INFO", "STATUS", "variant completed", Map.of("variant", variant.id()));
        }
        writeReports(rows);
        emit("INFO", "STATUS", "evaluation reports written", Map.of(
                "csv", "../duui-py/examples/taxonerd_correctness_eval_results.csv",
                "report", "../duui-py/examples/taxonerd_correctness_eval_report.md",
                "rows", rows.size()
        ));
        if (Boolean.parseBoolean(System.getProperty("duui.py.taxonerd.assert", "true"))) {
            assertFalse(rows.stream().anyMatch(ResultRow::failed), () -> "TaxoNERD correctness failures:\n" + markdown(rows));
        }
    }

    private static DUUIOrchestrationResult runVariant(Variant variant, DocumentCase document, JCas cas, DUUIEventSink sink) throws Exception {
        DUUIEventService eventService = new DUUIEventService(List.of(sink));
        try (DUUISystemScope system = DUUI.system("taxonerd-eval-" + variant.id()).events(eventService)) {
            try (DUUIPipelineScope pipeline = system.pipeline(variant.id() + "-pipeline")) {
                try (DUUIGeneratorScope<JCas> documents = new SingleJCasSource(cas).open(pipeline)) {
                    if (variant.requiresSpacy() && document.xmiPath() == null) {
                        try (DUUIStageScope<JCas> spacy = documents.linear("spacy-prerequisite-" + variant.id())) {
                            var spacyComponent = spacy.v1("spacy-prerequisite-" + variant.id())
                                    .remote()
                                    .endpoint(spacyEndpoint().toString())
                                    .sourceView("_InitialView")
                                    .targetView("_InitialView")
                                    .telemetrySink(sink)
                                    .parameters(Map.of(
                                            "model_name", System.getProperty("duui.py.spacy.model", "de_core_news_sm"),
                                            "spacy_language", System.getProperty("duui.py.taxonerd.language", "de")
                                    ));
                        }
                    }
                    try (DUUIStageScope<JCas> stage = documents.linear("remote-" + variant.id())) {
                        var component = stage.v1(variant.id())
                                .remote()
                                .endpoint(variant.endpoint().toString())
                                .sourceView("_InitialView")
                                .targetView("_InitialView")
                                .telemetrySink(sink)
                                .parameters(variant.parameters());
                        if (variant.asyncGreedy()) {
                            stage.dispatchPolicy(DUUIDispatchPolicy.of(DUUIDispatchMode.IO, 8));
                        }
                    }
                }
            }
            return system.run(variant.id() + "-pipeline");
        }
    }

    private static List<Variant> variants() {
        URI legacy = uri("duui.py.taxonerd.legacy.endpoint", "http://127.0.0.1:19718");
        URI msgpack = uri("duui.py.taxonerd.msgpack.endpoint", "http://127.0.0.1:19719");
        Map<String, String> mdGbif = Map.of(
                "model", System.getProperty("duui.py.taxonerd.model", "en_ner_eco_md"),
                "linking", System.getProperty("duui.py.taxonerd.linking", "gbif_backbone"),
                "threshold", System.getProperty("duui.py.taxonerd.threshold", "0.7")
        );
        String sparqlEndpoint = System.getProperty(
                "duui.py.taxonerd.sparql.endpoint",
                "http://host.containers.internal:8098/biofid-search/sparql"
        );
        List<Variant> variants = new ArrayList<>(List.of(
                new Variant("legacy-taxonerd-whole-document", legacy, mdGbif, false, false),
                new Variant("async-taxonerd-legacy-procedure-gbif", msgpack, params(mdGbif, "input_strategy", "legacy-procedure"), true, false),
                new Variant("async-taxonerd-legacy-procedure-gbif-fuseki", msgpack, params(mdGbif, "input_strategy", "legacy-procedure", "linking", "gbif_fuseki", "sparql_endpoint", sparqlEndpoint, "sparql_batch_size", "64", "sparql_concurrency", "8"), true, false),
                new Variant("async-taxonerd-whole-document-gbif", msgpack, params(mdGbif, "input_strategy", "whole-document", "neighbours", "10", "ann_ef_search", "80"), true, false),
                new Variant("async-taxonerd-whole-document-gbif-fuseki", msgpack, params(mdGbif, "input_strategy", "whole-document", "linking", "gbif_fuseki", "neighbours", "10", "ann_ef_search", "80", "linker_strategy", "exact-first-batched", "link_cache", "true", "sparql_endpoint", sparqlEndpoint, "sparql_batch_size", "64", "sparql_concurrency", "8"), true, false),
                new Variant("async-taxonerd-whole-document-gbif-linker-original", msgpack, params(mdGbif, "input_strategy", "whole-document", "neighbours", "10", "ann_ef_search", "80", "linker_strategy", "exact-first-original", "link_cache", "false"), true, false),
                new Variant("async-taxonerd-whole-document-gbif-linker-batched", msgpack, params(mdGbif, "input_strategy", "whole-document", "neighbours", "10", "ann_ef_search", "80", "linker_strategy", "exact-first-batched", "link_cache", "false"), true, false),
                new Variant("async-taxonerd-whole-document-gbif-linker-cached", msgpack, params(mdGbif, "input_strategy", "whole-document", "neighbours", "10", "ann_ef_search", "80", "linker_strategy", "exact-first-batched", "link_cache", "true"), true, false),
                new Variant("async-taxonerd-whole-document-gbif-linker-k5-ef40", msgpack, params(mdGbif, "input_strategy", "whole-document", "neighbours", "5", "ann_ef_search", "40", "linker_strategy", "exact-first-batched", "link_cache", "false"), true, false),
                new Variant("async-taxonerd-whole-document-gbif-linker-k3-ef20", msgpack, params(mdGbif, "input_strategy", "whole-document", "neighbours", "3", "ann_ef_search", "20", "linker_strategy", "exact-first-batched", "link_cache", "false"), true, false),
                new Variant("async-taxonerd-legacy-compatible-gbif-linker-original", msgpack, params(mdGbif, "input_strategy", "legacy-compatible", "neighbours", "10", "ann_ef_search", "80", "linker_strategy", "ann-original", "link_cache", "false"), true, false),
                new Variant("async-taxonerd-legacy-compatible-gbif-linker-batched", msgpack, params(mdGbif, "input_strategy", "legacy-compatible", "neighbours", "10", "ann_ef_search", "80", "linker_strategy", "ann-batched", "link_cache", "false"), true, false),
                new Variant("async-taxonerd-legacy-compatible-gbif-linker-cached", msgpack, params(mdGbif, "input_strategy", "legacy-compatible", "neighbours", "10", "ann_ef_search", "80", "linker_strategy", "ann-batched", "link_cache", "true"), true, false),
                new Variant("async-taxonerd-legacy-compatible-gbif-linker-k5-ef40", msgpack, params(mdGbif, "input_strategy", "legacy-compatible", "neighbours", "5", "ann_ef_search", "40", "linker_strategy", "ann-batched", "link_cache", "false"), true, false),
                new Variant("async-taxonerd-legacy-compatible-gbif-linker-original-ef200", msgpack, params(mdGbif, "input_strategy", "legacy-compatible", "neighbours", "10", "ann_ef_search", "200", "linker_strategy", "ann-original", "link_cache", "false"), true, false),
                new Variant("async-taxonerd-legacy-compatible-gbif-linker-batched-ef200", msgpack, params(mdGbif, "input_strategy", "legacy-compatible", "neighbours", "10", "ann_ef_search", "200", "linker_strategy", "ann-batched", "link_cache", "false"), true, false),
                new Variant("async-taxonerd-legacy-compatible-gbif-linker-cached-ef200", msgpack, params(mdGbif, "input_strategy", "legacy-compatible", "neighbours", "10", "ann_ef_search", "200", "linker_strategy", "ann-batched", "link_cache", "true"), true, false),
                new Variant("async-taxonerd-legacy-compatible-gbif-fuseki-ann-ef200", msgpack, params(mdGbif, "input_strategy", "legacy-compatible", "linking", "gbif_fuseki", "neighbours", "10", "ann_ef_search", "200", "linker_strategy", "ann-batched", "link_cache", "false", "sparql_endpoint", sparqlEndpoint, "sparql_batch_size", "64", "sparql_concurrency", "8"), true, false),
                new Variant("async-taxonerd-legacy-compatible-gbif-fuseki-ann-cached-ef200", msgpack, params(mdGbif, "input_strategy", "legacy-compatible", "linking", "gbif_fuseki", "neighbours", "10", "ann_ef_search", "200", "linker_strategy", "ann-batched", "link_cache", "true", "sparql_endpoint", sparqlEndpoint, "sparql_batch_size", "64", "sparql_concurrency", "8"), true, false),
                new Variant("async-taxonerd-legacy-compatible-gbif-fuseki-ann-cached-ef200-buffered", msgpack, params(mdGbif, "input_strategy", "legacy-compatible", "linking", "gbif_fuseki", "neighbours", "10", "ann_ef_search", "200", "linker_strategy", "ann-batched", "link_cache", "true", "sparql_endpoint", sparqlEndpoint, "sparql_batch_size", "64", "sparql_concurrency", "8"), true, false),
                new Variant("async-taxonerd-legacy-compatible-gbif-linker-exact-original", msgpack, params(mdGbif, "input_strategy", "legacy-compatible", "neighbours", "10", "ann_ef_search", "80", "linker_strategy", "exact-first-original", "link_cache", "false"), true, false),
                new Variant("async-taxonerd-legacy-compatible-gbif-linker-exact-batched", msgpack, params(mdGbif, "input_strategy", "legacy-compatible", "neighbours", "10", "ann_ef_search", "80", "linker_strategy", "exact-first-batched", "link_cache", "false"), true, false),
                new Variant("async-taxonerd-legacy-compatible-gbif-linker-exact-cached", msgpack, params(mdGbif, "input_strategy", "legacy-compatible", "neighbours", "10", "ann_ef_search", "80", "linker_strategy", "exact-first-batched", "link_cache", "true"), true, false),
                new Variant("async-taxonerd-whole-document-taxref-linker-original", msgpack, Map.of("input_strategy", "whole-document", "model", System.getProperty("duui.py.taxonerd.model", "en_ner_eco_md"), "linking", "taxref", "threshold", System.getProperty("duui.py.taxonerd.threshold", "0.7"), "neighbours", "10", "ann_ef_search", "80", "linker_strategy", "exact-first-original", "link_cache", "false"), true, false),
                new Variant("async-taxonerd-whole-document-taxref-linker-batched", msgpack, Map.of("input_strategy", "whole-document", "model", System.getProperty("duui.py.taxonerd.model", "en_ner_eco_md"), "linking", "taxref", "threshold", System.getProperty("duui.py.taxonerd.threshold", "0.7"), "neighbours", "10", "ann_ef_search", "80", "linker_strategy", "exact-first-batched", "link_cache", "false"), true, false),
                new Variant("async-taxonerd-whole-document-ncbi-linker-original", msgpack, Map.of("input_strategy", "whole-document", "model", System.getProperty("duui.py.taxonerd.model", "en_ner_eco_md"), "linking", "ncbi_taxonomy", "threshold", System.getProperty("duui.py.taxonerd.threshold", "0.7"), "neighbours", "10", "ann_ef_search", "80", "linker_strategy", "exact-first-original", "link_cache", "false"), true, false),
                new Variant("async-taxonerd-whole-document-ncbi-linker-batched", msgpack, Map.of("input_strategy", "whole-document", "model", System.getProperty("duui.py.taxonerd.model", "en_ner_eco_md"), "linking", "ncbi_taxonomy", "threshold", System.getProperty("duui.py.taxonerd.threshold", "0.7"), "neighbours", "10", "ann_ef_search", "80", "linker_strategy", "exact-first-batched", "link_cache", "false"), true, false),
                new Variant("async-taxonerd-span-sentence-gbif", msgpack, params(mdGbif, "input_strategy", "span-window", "span_types", "sentence", "batch_size", "16", "max_window_chars", "1200", "overlap_chars", "0", "neighbours", "10", "ann_ef_search", "80"), true, false),
                new Variant("async-taxonerd-span-sentence-nproc2-gbif", msgpack, params(mdGbif, "input_strategy", "span-window", "span_types", "sentence", "batch_size", "16", "n_process", "2", "max_window_chars", "1200", "overlap_chars", "0", "neighbours", "10", "ann_ef_search", "80"), true, false),
                new Variant("async-taxonerd-span-sentence-nproc4-gbif", msgpack, params(mdGbif, "input_strategy", "span-window", "span_types", "sentence", "batch_size", "16", "n_process", "4", "max_window_chars", "1200", "overlap_chars", "0", "neighbours", "10", "ann_ef_search", "80"), true, false),
                new Variant("async-taxonerd-span-sentence-merged-gbif", msgpack, params(mdGbif, "input_strategy", "span-window", "span_types", "sentence", "batch_size", "16", "max_window_chars", "1800", "overlap_chars", "80", "merge_spans", "true", "neighbours", "10", "ann_ef_search", "80"), true, false),
                new Variant("async-taxonerd-span-paragraph-gbif", msgpack, params(mdGbif, "input_strategy", "span-window", "span_types", "paragraph,ocr-paragraph,abbyy-paragraph", "batch_size", "8", "max_window_chars", "2500", "overlap_chars", "120", "neighbours", "10", "ann_ef_search", "80"), true, false),
                new Variant("async-taxonerd-span-paragraph-nproc2-gbif", msgpack, params(mdGbif, "input_strategy", "span-window", "span_types", "paragraph,ocr-paragraph,abbyy-paragraph", "batch_size", "8", "n_process", "2", "max_window_chars", "2500", "overlap_chars", "120", "neighbours", "10", "ann_ef_search", "80"), true, false),
                new Variant("async-taxonerd-span-paragraph-nproc4-gbif", msgpack, params(mdGbif, "input_strategy", "span-window", "span_types", "paragraph,ocr-paragraph,abbyy-paragraph", "batch_size", "8", "n_process", "4", "max_window_chars", "2500", "overlap_chars", "120", "neighbours", "10", "ann_ef_search", "80"), true, false),
                new Variant("async-taxonerd-span-div-gbif", msgpack, params(mdGbif, "input_strategy", "span-window", "span_types", "div,hucompute-div", "batch_size", "8", "max_window_chars", "4000", "overlap_chars", "160", "neighbours", "10", "ann_ef_search", "80"), true, false),
                new Variant("async-taxonerd-span-section-gbif", msgpack, params(mdGbif, "input_strategy", "span-window", "span_types", "section", "batch_size", "6", "max_window_chars", "5000", "overlap_chars", "200", "neighbours", "10", "ann_ef_search", "80"), true, false),
                new Variant("async-taxonerd-span-title-gbif", msgpack, params(mdGbif, "input_strategy", "span-window", "span_types", "title", "batch_size", "16", "max_window_chars", "600", "overlap_chars", "0", "neighbours", "10", "ann_ef_search", "80"), true, false)
        ));
        if (Boolean.getBoolean("duui.py.taxonerd.includeBiobert")) {
            variants.add(new Variant("async-taxonerd-whole-document-biobert-gbif", msgpack, Map.of("input_strategy", "whole-document", "model", "en_ner_eco_biobert", "linking", "gbif_backbone", "threshold", "0.7", "neighbours", "10", "ann_ef_search", "80"), true, false));
        }
        String selected = System.getProperty("duui.py.taxonerd.variants", "legacy-taxonerd-whole-document,async-taxonerd-legacy-procedure-gbif,async-taxonerd-whole-document-gbif");
        if (selected.equalsIgnoreCase("all")) return variants;
        List<String> allowed = List.of(selected.split(",")).stream().map(String::trim).filter(value -> !value.isEmpty()).toList();
        return variants.stream().filter(variant -> allowed.contains(variant.id())).toList();
    }

    private static List<DocumentCase> documents() {
        String text = System.getProperty("duui.py.taxonerd.text", "").trim();
        if (!text.isBlank()) {
            String expected = System.getProperty("duui.py.taxonerd.expected", "").trim();
            List<String> expectedMentions = expected.isBlank()
                    ? List.of()
                    : List.of(expected.split(";")).stream().map(String::trim).filter(value -> !value.isBlank()).toList();
            return List.of(new DocumentCase("adhoc-text", text, expectedMentions, null));
        }
        String sampleFiles = System.getProperty("duui.py.taxonerd.sample.files", "").trim();
        if (!sampleFiles.isBlank()) {
            return List.of(sampleFiles.split(",")).stream()
                    .map(String::trim)
                    .filter(value -> !value.isBlank())
                    .map(path -> new DocumentCase(Path.of(path).getFileName().toString(), "", List.of(), Path.of(path)))
                    .toList();
        }
        if (Boolean.getBoolean("duui.py.taxonerd.requireXmi")) {
            throw new IllegalArgumentException("XMI-only TaxoNERD evaluation requires -Dduui.py.taxonerd.sample.files");
        }
        List<DocumentCase> documents = List.of(
                new DocumentCase("doc-01", "Homo sapiens and Panthera leo were recorded near Quercus robur.", List.of("Homo sapiens", "Panthera leo", "Quercus robur"), null),
                new DocumentCase("doc-02", "A colony of Apis mellifera visited Rosa canina and Taraxacum officinale.", List.of("Apis mellifera", "Rosa canina", "Taraxacum officinale"), null),
                new DocumentCase("doc-03", "Canis lupus, Vulpes vulpes, and Ursus arctos occur in the survey notes.", List.of("Canis lupus", "Vulpes vulpes", "Ursus arctos"), null),
                new DocumentCase("doc-04", "The wetland contained Phragmites australis, Salix alba, and Rana temporaria.", List.of("Phragmites australis", "Salix alba", "Rana temporaria"), null),
                new DocumentCase("doc-05", "Researchers compared Escherichia coli with Bacillus subtilis in the sample.", List.of("Escherichia coli", "Bacillus subtilis"), null),
                new DocumentCase("doc-06", "Fagus sylvatica and Acer pseudoplatanus shaded Dryopteris filix-mas.", List.of("Fagus sylvatica", "Acer pseudoplatanus", "Dryopteris filix-mas"), null),
                new DocumentCase("doc-07", "Observations mention Felis catus, Mus musculus, and Rattus norvegicus.", List.of("Felis catus", "Mus musculus", "Rattus norvegicus"), null),
                new DocumentCase("doc-08", "The old record lists Bellis perennis beside Trifolium pratense.", List.of("Bellis perennis", "Trifolium pratense"), null)
        );
        int limit = Math.min(documents.size(), Integer.getInteger("duui.py.taxonerd.docs", 5));
        return documents.subList(0, limit);
    }

    private static TaxonStats taxonStats(JCas cas) throws Exception {
        CAS view = cas.getView("_InitialView").getCas();
        Type type = view.getTypeSystem().getType(TAXON_TYPE);
        if (type == null) return new TaxonStats(List.of(), 0, List.of());
        Feature identifier = type.getFeatureByBaseName("identifier");
        List<String> covered = new ArrayList<>();
        List<String> linkedText = new ArrayList<>();
        for (AnnotationFS annotation : view.getAnnotationIndex(type)) {
            covered.add(annotation.getCoveredText());
            String id = identifier == null ? null : annotation.getFeatureValueAsString(identifier);
            if (id == null || id.isBlank()) id = legacyLinkComment(view, annotation);
            if (id != null && !id.isBlank()) linkedText.add(annotation.getCoveredText() + "=" + id);
        }
        return new TaxonStats(covered, linkedText.size(), linkedText);
    }

    private static InputStats inputStats(JCas cas) throws Exception {
        CAS view = cas.getView("_InitialView").getCas();
        int characters = cas.getDocumentText() == null ? 0 : cas.getDocumentText().length();
        return new InputStats(
                characters,
                annotationCount(view, SENTENCE_TYPE),
                annotationCount(view, TOKEN_TYPE),
                annotationCount(view, POS_TYPE),
                annotationCount(view, NAMED_ENTITY_TYPE)
                        + annotationCount(view, TTLAB_NAMED_ENTITY_TYPE)
                        + annotationCount(view, TTLAB_TEXTTECHNOLOGY_NAMED_ENTITY_TYPE),
                annotationCount(view, TAXON_TYPE)
        );
    }

    private static int annotationCount(CAS view, String typeName) {
        Type type = view.getTypeSystem().getType(typeName);
        if (type == null) return 0;
        return view.getAnnotationIndex(type).size();
    }

    private static String legacyLinkComment(CAS view, AnnotationFS annotation) {
        Type commentType = view.getTypeSystem().getType("org.texttechnologylab.annotation.AnnotationComment");
        if (commentType == null) return null;
        Feature reference = commentType.getFeatureByBaseName("reference");
        Feature key = commentType.getFeatureByBaseName("key");
        Feature value = commentType.getFeatureByBaseName("value");
        if (reference == null || key == null || value == null) return null;
        var comments = view.getIndexRepository().getAllIndexedFS(commentType);
        while (comments.hasNext()) {
            FeatureStructure fs = comments.next();
            if (fs.getFeatureValue(reference) != annotation) continue;
            String keyValue = fs.getStringValue(key);
            String linkedValue = fs.getStringValue(value);
            if ("link".equals(keyValue) && linkedValue != null && !linkedValue.isBlank()) return linkedValue;
        }
        return null;
    }

    private static void seedRequiredAnnotations(JCas cas, Variant variant, DocumentCase document) {
        String text = cas.getDocumentText();
        if (text == null || text.isBlank()) return;
        if (document.xmiPath() != null) return;
        String strategy = variant.parameters().getOrDefault("input_strategy", "whole-document");
        String spanTypes = variant.parameters().getOrDefault("span_types", "");
        if ("span-window".equals(strategy) && containsAny(spanTypes, "sentence")) {
            seedSentenceAnnotations(cas, text);
        }
        if ("span-window".equals(strategy) && containsAny(spanTypes, "paragraph", "ocr-paragraph", "abbyy-paragraph")) {
            seedParagraphAnnotations(cas, text);
        }
        if ("span-window".equals(strategy) && containsAny(spanTypes, "div", "hucompute-div")) {
            seedDivAnnotations(cas, text);
        }
        if ("span-window".equals(strategy) && containsAny(spanTypes, "section")) {
            seedSectionAnnotations(cas, text);
        }
        if ("span-window".equals(strategy) && containsAny(spanTypes, "title")) {
            seedTitleAnnotations(cas, text);
        }
    }

    private static boolean containsAny(String value, String... needles) {
        String normalized = value == null ? "" : value.toLowerCase();
        for (String needle : needles) {
            if (normalized.contains(needle.toLowerCase())) return true;
        }
        return false;
    }

    private static void seedSentenceAnnotations(JCas cas, String text) {
        Type type = cas.getCas().getTypeSystem().getType(SENTENCE_TYPE);
        if (type == null) return;
        int start = 0;
        for (int index = 0; index < text.length(); index++) {
            char c = text.charAt(index);
            if (c == '.' || c == '!' || c == '?') {
                int end = index + 1;
                addAnnotation(cas, type, start, end);
                start = skipWhitespace(text, end);
            }
        }
        if (start < text.length()) addAnnotation(cas, type, start, text.length());
    }

    private static void seedParagraphAnnotations(JCas cas, String text) {
        Type type = firstType(cas, PARAGRAPH_TYPE, OCR_PARAGRAPH_TYPE, ABBYY_PARAGRAPH_TYPE);
        if (type == null) return;
        int start = 0;
        for (String paragraph : text.split("\\n\\s*\\n")) {
            int begin = text.indexOf(paragraph, start);
            if (begin < 0) continue;
            int end = begin + paragraph.length();
            addAnnotation(cas, type, begin, end);
            start = end;
        }
    }

    private static void seedDivAnnotations(JCas cas, String text) {
        Type type = firstType(cas, DIV_TYPE, HUCOMPUTE_DIV_TYPE);
        if (type == null) return;
        addAnnotation(cas, type, 0, text.length());
    }

    private static void seedSectionAnnotations(JCas cas, String text) {
        Type type = cas.getCas().getTypeSystem().getType(SECTION_TYPE);
        if (type == null) return;
        int midpoint = text.length() / 2;
        int split = text.indexOf("\n\n", Math.max(0, midpoint - 250));
        if (split <= 0 || split >= text.length() - 1) {
            addAnnotation(cas, type, 0, text.length());
            return;
        }
        addAnnotation(cas, type, 0, split);
        addAnnotation(cas, type, skipWhitespace(text, split), text.length());
    }

    private static void seedTitleAnnotations(JCas cas, String text) {
        Type type = cas.getCas().getTypeSystem().getType(TITLE_TYPE);
        if (type == null) return;
        int end = text.indexOf('.');
        if (end < 0) end = Math.min(text.length(), 180);
        else end = Math.min(text.length(), end + 1);
        addAnnotation(cas, type, 0, end);
    }

    private static void seedNamedEntities(JCas cas, String text, List<String> expected) {
        Type type = firstType(cas, NAMED_ENTITY_TYPE, TTLAB_NAMED_ENTITY_TYPE, TTLAB_TEXTTECHNOLOGY_NAMED_ENTITY_TYPE, TAXON_TYPE);
        if (type == null) return;
        for (String mention : expected) {
            addAllOccurrences(cas, type, text, mention);
        }
        addAllOccurrences(cas, type, text, "Picea abies");
        addAllOccurrences(cas, type, text, "Fichte");
        Matcher matcher = SCIENTIFIC_NAME.matcher(text);
        while (matcher.find()) {
            addAnnotation(cas, type, matcher.start(), matcher.end());
        }
    }

    private static Type firstType(JCas cas, String... typeNames) {
        for (String typeName : typeNames) {
            Type type = cas.getCas().getTypeSystem().getType(typeName);
            if (type != null) return type;
        }
        return null;
    }

    private static void addAllOccurrences(JCas cas, Type type, String text, String mention) {
        if (mention == null || mention.isBlank()) return;
        int from = 0;
        while (from < text.length()) {
            int begin = text.indexOf(mention, from);
            if (begin < 0) return;
            addAnnotation(cas, type, begin, begin + mention.length());
            from = begin + Math.max(1, mention.length());
        }
    }

    private static void addAnnotation(JCas cas, Type type, int begin, int end) {
        if (begin < 0 || end <= begin || end > cas.getDocumentText().length()) return;
        cas.getCas().addFsToIndexes(cas.getCas().createAnnotation(type, begin, end));
    }

    private static int skipWhitespace(String text, int offset) {
        int cursor = offset;
        while (cursor < text.length() && Character.isWhitespace(text.charAt(cursor))) cursor++;
        return cursor;
    }

    private static void loadXmi(Path path, JCas cas) throws Exception {
        try (InputStream file = Files.newInputStream(path);
             InputStream input = path.getFileName().toString().endsWith(".gz") ? new GZIPInputStream(file) : file) {
            XmiCasDeserializer.deserialize(input, cas.getCas(), true);
        }
    }

    private static JCas processedJCas(DUUIOrchestrationResult result, JCas fallback) {
        return result.results().stream()
                .map(DUUIExecutionResult::artifact)
                .map(artifact -> artifact.payload())
                .filter(JCas.class::isInstance)
                .map(JCas.class::cast)
                .reduce((first, second) -> second)
                .orElse(fallback);
    }

    private static void writeReports(List<ResultRow> rows) throws Exception {
        Path root = Path.of("..", "duui-py", "examples").normalize();
        Files.writeString(root.resolve("taxonerd_correctness_eval_report.md"), markdown(rows), StandardCharsets.UTF_8);
        Files.writeString(root.resolve("taxonerd_correctness_eval_results.csv"), csv(rows), StandardCharsets.UTF_8);
    }

    private static String markdown(List<ResultRow> rows) {
        StringBuilder out = new StringBuilder("# TaxoNERD correctness evaluation\n\n");
        out.append("Baseline: `legacy-taxonerd-whole-document` is the actual legacy DUUI TaxoNERD endpoint using the old custom JSON/Lua communication path. It is not using the generated MsgPack/Lua runtime path or async greedy response handling.\n\n");
        out.append("| variant | docs | chars | input_sentences | input_tokens | input_ne | expected | found | linked | missing | failed | median_ms | linker_ann_ms | linker_exact_ms | cache_hits | cache_misses | fuseki_ms | fuseki_aliases | fuseki_matches | fuseki_errors | metric_events |\n");
        out.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        rows.stream().collect(java.util.stream.Collectors.groupingBy(ResultRow::variant, LinkedHashMap::new, java.util.stream.Collectors.toList()))
                .forEach((variant, group) -> out.append("| ").append(variant)
                        .append(" | ").append(group.size())
                        .append(" | ").append(group.stream().mapToInt(ResultRow::inputCharacters).sum())
                        .append(" | ").append(group.stream().mapToInt(ResultRow::inputSentences).sum())
                        .append(" | ").append(group.stream().mapToInt(ResultRow::inputTokens).sum())
                        .append(" | ").append(group.stream().mapToInt(ResultRow::inputNamedEntities).sum())
                        .append(" | ").append(group.stream().mapToInt(ResultRow::expected).sum())
                        .append(" | ").append(group.stream().mapToInt(ResultRow::found).sum())
                        .append(" | ").append(group.stream().mapToInt(ResultRow::linked).sum())
                        .append(" | ").append(group.stream().mapToInt(ResultRow::missing).sum())
                        .append(" | ").append(group.stream().filter(ResultRow::failed).count())
                        .append(" | ").append(median(group.stream().mapToLong(ResultRow::elapsedMs).sorted().toArray()))
                        .append(" | ").append(Math.round(group.stream().mapToDouble(ResultRow::linkerAnnMs).sum()))
                        .append(" | ").append(Math.round(group.stream().mapToDouble(ResultRow::linkerExactMs).sum()))
                        .append(" | ").append(Math.round(group.stream().mapToDouble(ResultRow::linkerCacheHits).sum()))
                        .append(" | ").append(Math.round(group.stream().mapToDouble(ResultRow::linkerCacheMisses).sum()))
                        .append(" | ").append(Math.round(group.stream().mapToDouble(ResultRow::linkerFusekiMs).sum()))
                        .append(" | ").append(Math.round(group.stream().mapToDouble(ResultRow::linkerFusekiAliases).sum()))
                        .append(" | ").append(Math.round(group.stream().mapToDouble(ResultRow::linkerFusekiMatches).sum()))
                        .append(" | ").append(Math.round(group.stream().mapToDouble(ResultRow::linkerFusekiErrors).sum()))
                        .append(" | ").append(group.stream().mapToLong(ResultRow::metricEvents).sum())
                        .append(" |\n"));
        out.append("\n## Missing mentions\n\n");
        rows.stream().filter(row -> row.missing() > 0 || row.failed())
                .forEach(row -> out.append("- ").append(row.variant()).append(" / ").append(row.document())
                        .append(": missing=").append(row.missingText().isBlank() ? "none" : row.missingText())
                        .append(" found=").append(row.foundText())
                        .append(" linked=").append(row.linkedText())
                        .append('\n'));
        return out.toString();
    }

    private static String csv(List<ResultRow> rows) {
        StringBuilder out = new StringBuilder("variant,document,expected,found,linked,missing,elapsed_ms,failed,metric_events,linker_exact_ms,linker_ann_ms,linker_exact_matches,linker_ann_mentions,linker_cache_hits,linker_cache_misses,linker_fuseki_aliases,linker_fuseki_matches,linker_fuseki_errors,linker_fuseki_ms,input_characters,input_sentences,input_tokens,input_pos,input_named_entities,input_existing_taxons,missing_text,found_text,linked_text\n");
        for (ResultRow row : rows) {
            out.append(quote(row.variant())).append(',').append(quote(row.document())).append(',')
                    .append(row.expected()).append(',').append(row.found()).append(',').append(row.linked()).append(',').append(row.missing()).append(',')
                    .append(row.elapsedMs()).append(',').append(row.failed()).append(',').append(row.metricEvents()).append(',')
                    .append(row.linkerExactMs()).append(',').append(row.linkerAnnMs()).append(',').append(row.linkerExactMatches()).append(',')
                    .append(row.linkerAnnMentions()).append(',').append(row.linkerCacheHits()).append(',').append(row.linkerCacheMisses()).append(',')
                    .append(row.linkerFusekiAliases()).append(',').append(row.linkerFusekiMatches()).append(',').append(row.linkerFusekiErrors()).append(',')
                    .append(row.linkerFusekiMs()).append(',')
                    .append(row.inputCharacters()).append(',').append(row.inputSentences()).append(',').append(row.inputTokens()).append(',')
                    .append(row.inputPos()).append(',').append(row.inputNamedEntities()).append(',').append(row.inputExistingTaxons()).append(',')
                    .append(quote(row.missingText())).append(',').append(quote(row.foundText())).append(',').append(quote(row.linkedText())).append('\n');
        }
        return out.toString();
    }

    private static long median(long[] values) {
        if (values.length == 0) return 0;
        return values[values.length / 2];
    }

    private static String quote(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    private static double metricValue(DUUIInMemoryEventSink events, String name) {
        return events.events().stream()
                .filter(event -> event.type() == DUUIEventType.METRIC)
                .filter(event -> name.equals(event.metricName()))
                .map(DUUIEvent::metricValue)
                .filter(Objects::nonNull)
                .mapToDouble(Double::doubleValue)
                .sum();
    }

    private static double metricValue(DUUIInMemoryEventSink events, JCas view, String name) {
        Double tagValue = metricTagValue(events, "taxonerd_taxon_matches", name);
        if (tagValue != null) {
            return tagValue;
        }
        double eventValue = metricValue(events, name);
        if (eventValue != 0.0) {
            return eventValue;
        }
        return metadataMetricValue(view, name);
    }

    private static Double metricTagValue(DUUIInMemoryEventSink events, String metricName, String tagName) {
        return events.events().stream()
                .filter(event -> event.type() == DUUIEventType.METRIC)
                .filter(event -> metricName.equals(event.metricName()))
                .map(event -> event.metricTags().get(tagName))
                .filter(Objects::nonNull)
                .map(Double::parseDouble)
                .findFirst()
                .orElse(null);
    }

    private static double metadataMetricValue(JCas view, String name) {
        Type type = view.getTypeSystem().getType(DOCUMENT_MODIFICATION_TYPE);
        if (type == null) {
            return 0.0;
        }
        Feature commentFeature = type.getFeatureByBaseName("comment");
        if (commentFeature == null) {
            return 0.0;
        }
        Pattern pattern = Pattern.compile("(?:^|\\s)" + Pattern.quote(name) + "=([-+]?[0-9]*\\.?[0-9]+)");
        double out = 0.0;
        for (FeatureStructure fs : view.getCas().select(type)) {
            String comment = fs.getStringValue(commentFeature);
            if (comment == null) {
                continue;
            }
            Matcher matcher = pattern.matcher(comment);
            while (matcher.find()) {
                out += Double.parseDouble(matcher.group(1));
            }
        }
        return out;
    }

    private static void emitEvent(Variant variant, DocumentCase document, DUUIEvent event) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("variant", variant.id());
        data.put("document", document.id());
        data.put("duuiEventId", event.id());
        data.put("duuiEventType", event.type().name());
        data.put("duuiEventLevel", event.level() == null ? null : event.level().name());
        data.put("duuiEventStatus", event.status() == null ? null : event.status().name());
        data.put("name", event.name());
        data.put("message", event.message());
        data.put("metricName", event.metricName());
        data.put("metricValue", event.metricValue());
        data.put("metricUnit", event.metricUnit());
        data.put("metricIntervalMs", event.metricIntervalMs());
        data.put("metricTags", event.metricTags());
        data.put("errorType", event.errorType());
        data.put("recoveryHint", event.recoveryHint());
        data.put("attributes", event.attributes());
        data.put("traceId", event.traceId());
        data.put("spanId", event.spanId());
        data.put("parentSpanId", event.parentSpanId());
        data.put("orchestratorId", event.orchestratorId());
        data.put("taskId", event.taskId());
        data.put("stageId", event.stageId());
        data.put("componentId", event.componentId());
        data.put("annotatorId", event.annotatorId());
        data.put("workerId", event.workerId());
        String level = switch (event.type()) {
            case ERROR -> "ERROR";
            case METRIC -> "METRIC";
            case STATUS -> event.status() == null ? "INFO" : event.status().name();
            case LOG -> event.level() == null ? "INFO" : event.level().name();
        };
        String message = event.type() == DUUIEventType.METRIC
                ? "metric " + event.metricName() + "=" + event.metricValue() + " " + nullToEmpty(event.metricUnit())
                : Objects.requireNonNullElse(event.message(), Objects.requireNonNullElse(event.name(), event.type().name()));
        emit(level, event.type().name(), message, data);
    }

    private static void emit(String level, String type, String message, Map<String, ?> data) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("level", level == null ? "INFO" : level.toLowerCase());
        event.put("type", type == null ? "LOG" : type.toLowerCase());
        event.put("message", message);
        event.put("data", data == null ? Map.of() : data);
        System.out.println("@@DUUI_EVENT@@" + json(event));
    }

    private static String json(Object value) {
        if (value == null) return "null";
        if (value instanceof String text) return "\"" + escapeJson(text) + "\"";
        if (value instanceof Number || value instanceof Boolean) return value.toString();
        if (value instanceof Map<?, ?> map) {
            StringBuilder out = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!first) out.append(',');
                first = false;
                out.append(json(String.valueOf(entry.getKey()))).append(':').append(json(entry.getValue()));
            }
            return out.append('}').toString();
        }
        if (value instanceof Iterable<?> items) {
            StringBuilder out = new StringBuilder("[");
            boolean first = true;
            for (Object item : items) {
                if (!first) out.append(',');
                first = false;
                out.append(json(item));
            }
            return out.append(']').toString();
        }
        return json(String.valueOf(value));
    }

    private static String escapeJson(String value) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\b' -> out.append("\\b");
                case '\f' -> out.append("\\f");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) out.append(String.format("\\u%04x", (int) c));
                    else out.append(c);
                }
            }
        }
        return out.toString();
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    private static Map<String, String> params(Map<String, String> base, String... values) {
        Map<String, String> out = new LinkedHashMap<>(base);
        for (int i = 0; i + 1 < values.length; i += 2) out.put(values[i], values[i + 1]);
        return Map.copyOf(out);
    }

    private static URI uri(String property, String fallback) {
        return URI.create(System.getProperty(property, fallback));
    }

    private static URI spacyEndpoint() {
        return uri("duui.py.spacy.endpoint", "http://127.0.0.1:19722");
    }

    private static void assertHealthy(URI endpoint) throws Exception {
        HttpResponse<Void> response = CLIENT.send(
                HttpRequest.newBuilder(endpoint.resolve("/v1/documentation"))
                        .timeout(Duration.ofSeconds(10))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.discarding());
        if (response.statusCode() != 200) throw new IllegalStateException(endpoint + " returned HTTP " + response.statusCode());
    }

    private static TypeSystemDescription mergedRemoteTypeSystem(URI... endpoints) throws Exception {
        List<TypeSystemDescription> descriptions = new ArrayList<>();
        descriptions.add(TypeSystemDescriptionFactory.createTypeSystemDescription());
        for (URI endpoint : endpoints) {
            HttpResponse<String> response = CLIENT.send(
                    HttpRequest.newBuilder(endpoint.resolve("/v1/typesystem"))
                            .timeout(Duration.ofSeconds(10))
                            .GET()
                            .build(),
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            descriptions.add(UIMAFramework.getXMLParser().parseTypeSystemDescription(
                    new XMLInputSource(new ByteArrayInputStream(response.body().getBytes(StandardCharsets.UTF_8)), null)));
        }
        return CasCreationUtils.mergeTypeSystems(descriptions);
    }

    private record SingleJCasSource(JCas cas) implements DUUIGenerator<JCas> {
        @Override
        public void generate(DUUIArtifactEmitter<JCas> emitter) {
            emitter.emit(DUUIArtifact.of(cas));
        }
    }

    private record Variant(String id, URI endpoint, Map<String, String> parameters, boolean asyncGreedy, boolean requiresSpacy) {
        boolean usesBackbone() {
            String linking = parameters.get("linking");
            return "gbif_backbone".equals(linking) || "gbif_fuseki".equals(linking);
        }
    }
    private record DocumentCase(String id, String text, List<String> expected, Path xmiPath) {}
    private record TaxonStats(List<String> covered, int linked, List<String> linkedText) {}
    private record InputStats(int characters, int sentences, int tokens, int pos, int namedEntities, int taxons) {}
    private record ResultRow(String variant, String document, int expected, int found, int linked, int missing, long elapsedMs, boolean failed, long metricEvents, double linkerExactMs, double linkerAnnMs, double linkerExactMatches, double linkerAnnMentions, double linkerCacheHits, double linkerCacheMisses, double linkerFusekiAliases, double linkerFusekiMatches, double linkerFusekiErrors, double linkerFusekiMs, int inputCharacters, int inputSentences, int inputTokens, int inputPos, int inputNamedEntities, int inputExistingTaxons, String missingText, String foundText, String linkedText) {}
}
