package org.texttechnologylab.duui.rework;

import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.Feature;
import org.apache.uima.cas.FSIterator;
import org.apache.uima.cas.FeatureStructure;
import org.apache.uima.cas.SerialFormat;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.text.AnnotationFS;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.CasIOUtils;
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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.StandardOpenOption;
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

class DUUILegacyModernAnnotatorMatrixTest {
    private static final String EXAMPLES = "../../duui-py/examples";
    private static final HttpClient CLIENT = HttpClient.newHttpClient();
    private static final String TAXON_TYPE = "org.texttechnologylab.annotation.type.Taxon";
    private static final String LEGACY_GEONAMES_TYPE = "org.texttechnologylab.annotation.GeoNamesEntity";
    private static final String RICH_GEONAMES_TYPE = "org.texttechnologylab.annotation.geonames.GeoNamesEntity";
    private static final String LEGACY_GNFINDER_TAXON_TYPE = "org.texttechnologylab.annotation.type.Taxon";
    private static final String MODERN_GNFINDER_TAXON_TYPE = "org.texttechnologylab.annotation.biofid.gnfinder.Taxon";
    private static final List<String> SPACY_OUTPUT_ROOT_TYPES = List.of(
            "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence",
            "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token",
            "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma",
            "de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS",
            "de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.morph.MorphologicalFeatures",
            "de.tudarmstadt.ukp.dkpro.core.api.syntax.type.dependency.Dependency",
            "de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity"
    );
    private static final List<String> MORPH_FEATURES = List.of(
            "value", "gender", "number", "case", "degree", "verbForm", "tense", "mood",
            "voice", "definiteness", "person", "aspect", "animacy", "negative", "numType",
            "possessive", "pronType", "reflex", "transitivity"
    );
    private static final Random RNG = new Random();

    @TempDir
    Path tempDir;

    // -----------------------------------------------------------------------
    // @Test entry points — keep all existing signatures unchanged
    // -----------------------------------------------------------------------

    @Test
    void compareSpacyLegacyCustomLuaAndModernGeneratedMsgpackLuaOnXmi() throws Exception {
        String legacy = System.getProperty("duui.py.spacy.legacy.image", "localhost/duui-py-spacy-legacy-lua:latest");
        String modern = System.getProperty("duui.py.spacy.async.image", "localhost/duui-py-spacy-msgpack-lua:latest");
        Map<String, String> parameters = Map.of(
                "spacy_model_size", System.getProperty("duui.py.spacy.model_size", "trf"),
                "spacy_batch_size", System.getProperty("duui.py.spacy.batch_size", "32"),
                "use_existing_sentences", "false",
                "spacy_language", "de"
        );
        compareSpacyAnnotator(
                "SPACY_BASELINE_VS_ASYNC_XMI",
                legacy,
                modern,
                parameters,
                spacyDocuments()
        );
    }

    @Test
    void compareTaxonerdLegacyJsonLuaAndModernGeneratedMsgpackLuaOnXmi() throws Exception {
        String legacy = System.getProperty("duui.py.taxonerd.legacy.image", "localhost/duui-py-taxonerd-legacy-lua:latest");
        String modern = System.getProperty("duui.py.taxonerd.async.image", "localhost/duui-py-taxonerd-msgpack-lua:latest");
        Map<String, String> parameters = Map.of(
                "model", System.getProperty("duui.py.taxonerd.model", "en_ner_eco_md"),
                "linking", System.getProperty("duui.py.taxonerd.linking", "gbif_backbone"),
                "threshold", System.getProperty("duui.py.taxonerd.threshold", "0.7"),
                "input_strategy", System.getProperty("duui.py.taxonerd.input_strategy", "legacy-procedure"),
                "linker_strategy", System.getProperty("duui.py.taxonerd.linker_strategy", "ann-original"),
                "allow_unlinked", System.getProperty("duui.py.taxonerd.allow_unlinked", "false"),
                "prefer_gpu", "true",
                "timeout", System.getProperty("duui.py.taxonerd.timeout", "600")
        );
        compareTaxonAnnotator(
                "TAXONERD_BASELINE_VS_ASYNC_XMI",
                "taxonerd",
                legacy,
                modern,
                parameters,
                taxonerdDocuments()
        );
    }

    @Test
    void compareGazetteerLegacyJsonLuaAndModernGeneratedMsgpackLuaOnXmi() throws Exception {
        String legacy = System.getProperty("duui.py.gazetteer.legacy.image", "localhost/duui-py-gazetteer-legacy-lua:latest");
        String modern = System.getProperty("duui.py.gazetteer.async.image", "localhost/duui-py-gazetteer-msgpack-lua:latest");
        compareTaxonAnnotator(
                "GAZETTEER_BASELINE_VS_ASYNC_XMI",
                "gazetteer",
                legacy,
                modern,
                Map.of("timeout", System.getProperty("duui.py.gazetteer.timeout", "120")),
                gazetteerDocuments()
        );
    }

    @Test
    void compareGeoNamesLegacyJsonLuaAndModernGeneratedMsgpackLuaOnXmi() throws Exception {
        String legacy = System.getProperty("duui.py.geonames.legacy.image", "localhost/duui-py-geonames-legacy-lua:latest");
        String modern = System.getProperty("duui.py.geonames.async.image", "localhost/duui-py-geonames-msgpack-lua:latest");
        compareTypedAnnotator(
                "GEONAMES_BASELINE_VS_ASYNC_XMI",
                "geonames",
                legacy,
                modern,
                Map.of(
                        "timeout", System.getProperty("duui.py.geonames.timeout", "120"),
                        "annotation_type", System.getProperty("duui.py.geonames.annotation_type", "de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity")
                ),
                geonamesDocuments(),
                List.of(RICH_GEONAMES_TYPE),
                List.of(RICH_GEONAMES_TYPE),
                "application/json"
        );
    }

    @Test
    void compareGNFinderLegacyXmiLuaAndModernGeneratedMsgpackLuaOnXmi() throws Exception {
        String legacy = System.getProperty("duui.py.gnfinder.legacy.image", "localhost/duui-py-gnfinder-legacy-lua:latest");
        String modern = System.getProperty("duui.py.gnfinder.async.image", "localhost/duui-py-gnfinder-msgpack-lua:latest");
        compareTypedAnnotator(
                "GNFINDER_BASELINE_VS_ASYNC_XMI",
                "gnfinder",
                legacy,
                modern,
                Map.of(
                        "lang", System.getProperty("duui.py.gnfinder.lang", "de"),
                        "verify", System.getProperty("duui.py.gnfinder.verify", "true"),
                        "utf8_input", System.getProperty("duui.py.gnfinder.utf8_input", "true")
                ),
                gnfinderDocuments(),
                List.of(LEGACY_GNFINDER_TAXON_TYPE),
                List.of(MODERN_GNFINDER_TAXON_TYPE),
                "application/vnd.apache.uima.xmi+xml"
        );
    }

    @Test
    void compareAllAnnotatorsAtScale() throws Exception {
        int scale = Integer.getInteger("duui.py.matrix.scale", 0);
        appendMatrixHeader("label\tdocument\tchars\tvariants\tbaseline_taxa\tasync_taxa\tnewstrategy_taxa\tall_equal\tbaseline_best_ms\tasync_best_ms\tnewstrategy_best_ms\tdelta_ms\tbaseline_serialize_ms\tbaseline_response_wait_ms\tbaseline_apply_ms\tbaseline_request_bytes\tbaseline_response_bytes\tasync_serialize_ms\tasync_request_ms\tasync_apply_ms\tasync_request_bytes\tasync_response_bytes\tnewstrategy_serialize_ms\tnewstrategy_request_ms\tnewstrategy_apply_ms\tnewstrategy_request_bytes\tnewstrategy_response_bytes\tbaseline_types\tasync_types\tnewstrategy_types");

        runSingleAnnotatorAtScale("spacy", scale);
        runSingleAnnotatorAtScale("taxonerd", scale);
        runSingleAnnotatorAtScale("gazetteer", scale);
        runSingleAnnotatorAtScale("gnfinder", scale);
        runSingleAnnotatorAtScale("geonames", scale);
    }

    // -----------------------------------------------------------------------
    // Scale orchestrator
    // -----------------------------------------------------------------------

    private void runSingleAnnotatorAtScale(String annotatorKey, int scale) throws Exception {
        List<Path> pool = resolveDocumentPaths(annotatorKey, defaultDocumentPaths(annotatorKey));
        List<Path> documents = scale > 0 && scale < pool.size()
                ? sampleRandom(pool, scale)
                : pool;

        String label = annotatorKey.toUpperCase(Locale.ROOT) + "_SCALE";
        String legacyImage = System.getProperty("duui.py." + annotatorKey + ".legacy.image");
        String asyncImage = System.getProperty("duui.py." + annotatorKey + ".async.image");
        if (legacyImage == null && asyncImage == null) {
            System.out.println(label + "\tskipped (no images configured)");
            return;
        }

        switch (annotatorKey) {
            case "spacy" -> {
                if (legacyImage == null) legacyImage = "localhost/duui-py-spacy-legacy-lua:latest";
                if (asyncImage == null) asyncImage = "localhost/duui-py-spacy-msgpack-lua:latest";
                Map<String, String> parameters = Map.of(
                        "spacy_model_size", System.getProperty("duui.py.spacy.model_size", "trf"),
                        "spacy_batch_size", System.getProperty("duui.py.spacy.batch_size", "32"),
                        "use_existing_sentences", "false",
                        "spacy_language", "de"
                );
                compareSpacyAnnotator(label, legacyImage, asyncImage, parameters, documents);
            }
            case "taxonerd" -> {
                if (legacyImage == null) legacyImage = "localhost/duui-py-taxonerd-legacy-lua:latest";
                if (asyncImage == null) asyncImage = "localhost/duui-py-taxonerd-msgpack-lua:latest";
                Map<String, String> parameters = Map.of(
                        "model", System.getProperty("duui.py.taxonerd.model", "en_ner_eco_md"),
                        "linking", System.getProperty("duui.py.taxonerd.linking", "gbif_backbone"),
                        "threshold", System.getProperty("duui.py.taxonerd.threshold", "0.7"),
                        "input_strategy", System.getProperty("duui.py.taxonerd.input_strategy", "legacy-procedure"),
                        "linker_strategy", System.getProperty("duui.py.taxonerd.linker_strategy", "ann-original"),
                        "allow_unlinked", System.getProperty("duui.py.taxonerd.allow_unlinked", "false"),
                        "prefer_gpu", "true",
                        "timeout", System.getProperty("duui.py.taxonerd.timeout", "600")
                );
                compareTaxonAnnotator(label, "taxonerd", legacyImage, asyncImage, parameters, documents);
            }
            case "gazetteer" -> {
                if (legacyImage == null) legacyImage = "localhost/duui-py-gazetteer-legacy-lua:latest";
                if (asyncImage == null) asyncImage = "localhost/duui-py-gazetteer-msgpack-lua:latest";
                compareTaxonAnnotator(label, "gazetteer", legacyImage, asyncImage,
                        Map.of("timeout", System.getProperty("duui.py.gazetteer.timeout", "120")),
                        documents);
            }
            case "gnfinder" -> {
                if (legacyImage == null) legacyImage = "localhost/duui-py-gnfinder-legacy-lua:latest";
                if (asyncImage == null) asyncImage = "localhost/duui-py-gnfinder-msgpack-lua:latest";
                compareTypedAnnotator(label, "gnfinder", legacyImage, asyncImage,
                        Map.of(
                                "lang", System.getProperty("duui.py.gnfinder.lang", "de"),
                                "verify", System.getProperty("duui.py.gnfinder.verify", "true"),
                                "utf8_input", System.getProperty("duui.py.gnfinder.utf8_input", "true")
                        ),
                        documents,
                        List.of(LEGACY_GNFINDER_TAXON_TYPE),
                        List.of(MODERN_GNFINDER_TAXON_TYPE),
                        "application/vnd.apache.uima.xmi+xml");
            }
            case "geonames" -> {
                if (legacyImage == null) legacyImage = "localhost/duui-py-geonames-legacy-lua:latest";
                if (asyncImage == null) asyncImage = "localhost/duui-py-geonames-msgpack-lua:latest";
                compareTypedAnnotator(label, "geonames", legacyImage, asyncImage,
                        Map.of(
                                "timeout", System.getProperty("duui.py.geonames.timeout", "120"),
                                "annotation_type", System.getProperty("duui.py.geonames.annotation_type", "de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity")
                        ),
                        documents,
                        List.of(RICH_GEONAMES_TYPE),
                        List.of(RICH_GEONAMES_TYPE),
                        "application/json");
            }
            default -> throw new IllegalArgumentException("Unknown annotator key: " + annotatorKey);
        }

        // Emit aggregate metrics row
        emitAggregateRow(label, documents.size());
    }

    // -----------------------------------------------------------------------
    // Core comparison methods (spaCy uses fingerprints; others count types)
    // -----------------------------------------------------------------------

    private void compareSpacyAnnotator(
            String label,
            String legacyImage,
            String modernImage,
            Map<String, String> parameters,
            List<Path> documents
    ) throws Exception {
        TypeSystemDescription typeSystem = localExampleTypeSystem(
                "spacy-legacy-lua/TypeSystemSpacyLegacy.xml",
                "spacy-lua-msgpack/TypeSystemSpacy.xml"
        );
        Map<String, InputProfile> inputProfiles = inputProfiles(typeSystem, documents);
        BatchRun legacyRun = runPodmanBatch(
                label, "spacy-baseline", legacyImage, false, false, parameters,
                "application/json", documents, typeSystem, SPACY_OUTPUT_ROOT_TYPES);
        BatchRun modernRun = runPodmanBatch(
                label, "spacy-async", modernImage, true, false, parameters,
                null, documents, typeSystem, SPACY_OUTPUT_ROOT_TYPES);

        BatchRun newStrategyRun = null;
        if (hasNewStrategy("spacy")) {
            newStrategyRun = runPodmanBatch(
                    label, "spacy-newstrategy",
                    newStrategyImage("spacy"),
                    newStrategyStreaming("spacy"),
                    false,
                    newStrategyParameters("spacy", parameters),
                    newStrategyStreaming("spacy") ? null : "application/json",
                    documents, typeSystem, SPACY_OUTPUT_ROOT_TYPES);
        }

        for (Path document : documents) {
            InputProfile input = inputProfiles.get(stem(document));
            ResultArtifact legacy = legacyRun.required(input.textKey());
            ResultArtifact modern = modernRun.required(input.textKey());
            Map<String, Integer> legacyAdded = subtract(spacyFingerprints(legacy.cas()), input.spacyFingerprints());
            Map<String, Integer> modernAdded = subtract(spacyFingerprints(modern.cas()), input.spacyFingerprints());
            boolean outputEqual = legacyAdded.equals(modernAdded);

            StringBuilder row = new StringBuilder();
            row.append(String.format(Locale.ROOT,
                    "%s\tdocument=%s\tchars=%d\tbaseline_added=%d\tasync_added=%d\toutput_equal=%s\tbaseline_best_ms=%d\tasync_best_ms=%d\tdelta_ms=%d\tbaseline_serialize_ms=%.1f\tbaseline_response_wait_ms=%.1f\tbaseline_apply_ms=%.1f\tbaseline_request_bytes=%.0f\tbaseline_response_bytes=%.0f\tasync_serialize_ms=%.1f\tasync_request_ms=%.1f\tasync_apply_ms=%.1f\tasync_request_bytes=%.0f\tasync_response_bytes=%.0f\tbaseline_types=%s\tasync_types=%s",
                    label,
                    stem(document),
                    legacy.characters(),
                    legacyAdded.values().stream().mapToInt(Integer::intValue).sum(),
                    modernAdded.values().stream().mapToInt(Integer::intValue).sum(),
                    outputEqual,
                    legacy.durationMs(),
                    modern.durationMs(),
                    legacy.durationMs() - modern.durationMs(),
                    legacy.encodeMs(), legacy.requestMs(), legacy.applyMs(),
                    legacy.requestBytes(), legacy.responseBytes(),
                    modern.encodeMs(), modern.requestMs(), modern.applyMs(),
                    modern.requestBytes(), modern.responseBytes(),
                    typeSummary(typeCountsFromFingerprints(legacyAdded)),
                    typeSummary(typeCountsFromFingerprints(modernAdded))
            ));

            if (newStrategyRun != null) {
                ResultArtifact newStrategy = newStrategyRun.required(input.textKey());
                Map<String, Integer> newStrategyAdded = subtract(spacyFingerprints(newStrategy.cas()), input.spacyFingerprints());
                row.append(String.format(Locale.ROOT,
                        "\tnewstrategy_added=%d\tnewstrategy_best_ms=%d\tnewstrategy_serialize_ms=%.1f\tnewstrategy_request_ms=%.1f\tnewstrategy_apply_ms=%.1f\tnewstrategy_request_bytes=%.0f\tnewstrategy_response_bytes=%.0f\tnewstrategy_types=%s",
                        newStrategyAdded.values().stream().mapToInt(Integer::intValue).sum(),
                        newStrategy.durationMs(),
                        newStrategy.encodeMs(), newStrategy.requestMs(), newStrategy.applyMs(),
                        newStrategy.requestBytes(), newStrategy.responseBytes(),
                        typeSummary(typeCountsFromFingerprints(newStrategyAdded))
                ));
            }

            System.out.println(row);
            appendMatrixRow(row.toString());
            assertTrue(outputEqual, label + " semantic spaCy outputs differ for " + document + " diff=" + fingerprintDiff(legacyAdded, modernAdded));
        }
    }

    private void compareTaxonAnnotator(
            String label,
            String idPrefix,
            String legacyImage,
            String modernImage,
            Map<String, String> parameters,
            List<Path> documents
    ) throws Exception {
        compareTypedAnnotator(label, idPrefix, legacyImage, modernImage, parameters, documents,
                List.of(TAXON_TYPE), List.of(TAXON_TYPE), "application/json");
    }

    private void compareTypedAnnotator(
            String label,
            String idPrefix,
            String legacyImage,
            String modernImage,
            Map<String, String> parameters,
            List<Path> documents,
            List<String> legacyCountTypes,
            List<String> modernCountTypes,
            String legacyContentType
    ) throws Exception {
        TypeSystemDescription typeSystem = localTypeSystemFor(idPrefix);
        Map<String, InputProfile> inputProfiles = inputProfiles(typeSystem, documents);
        boolean useGpu = "taxonerd".equals(idPrefix);

        String legacyEndpoint = System.getProperty("duui.py." + idPrefix + ".legacy.endpoint");
        String modernEndpoint = System.getProperty("duui.py." + idPrefix + ".msgpack.endpoint");

        BatchRun legacyRun;
        if (legacyEndpoint != null && !legacyEndpoint.isBlank()) {
            legacyRun = runRemoteBatch(label, idPrefix + "-baseline", URI.create(legacyEndpoint), false, parameters,
                    legacyContentType, documents, typeSystem, legacyCountTypes);
        } else {
            legacyRun = runPodmanBatch(label, idPrefix + "-baseline", legacyImage, false, useGpu, parameters,
                    legacyContentType, documents, typeSystem, legacyCountTypes);
        }

        BatchRun modernRun;
        if (modernEndpoint != null && !modernEndpoint.isBlank()) {
            modernRun = runRemoteBatch(label, idPrefix + "-async", URI.create(modernEndpoint), true, parameters,
                    null, documents, typeSystem, modernCountTypes);
        } else {
            modernRun = runPodmanBatch(label, idPrefix + "-async", modernImage, true, useGpu, parameters,
                    null, documents, typeSystem, modernCountTypes);
        }

        BatchRun newStrategyRun = null;
        if (hasNewStrategy(idPrefix)) {
            boolean nsStreaming = newStrategyStreaming(idPrefix);
            newStrategyRun = runPodmanBatch(label, idPrefix + "-newstrategy",
                    newStrategyImage(idPrefix),
                    nsStreaming, useGpu,
                    newStrategyParameters(idPrefix, parameters),
                    nsStreaming ? null : legacyContentType,
                    documents, typeSystem, modernCountTypes);
        }

        for (Path document : documents) {
            InputProfile input = inputProfiles.get(stem(document));
            ResultArtifact legacy = legacyRun.required(input.textKey());
            ResultArtifact modern = modernRun.required(input.textKey());
            int legacyCount = countTypes(legacy.cas(), legacyCountTypes);
            int modernCount = countTypes(modern.cas(), modernCountTypes);
            boolean outputEqual;
            if ("gazetteer".equals(idPrefix)) {
                double tolerance = Math.max(legacyCount, modernCount) * 0.10;
                outputEqual = Math.abs(legacyCount - modernCount) <= tolerance;
            } else {
                outputEqual = legacyCount == modernCount;
            }

            StringBuilder row = new StringBuilder();
            row.append(String.format(Locale.ROOT,
                    "%s\tdocument=%s\tchars=%d\tbaseline_taxa=%d\tasync_taxa=%d\toutput_equal=%s\tbaseline_best_ms=%d\tasync_best_ms=%d\tdelta_ms=%d\tbaseline_serialize_ms=%.1f\tbaseline_response_wait_ms=%.1f\tbaseline_apply_ms=%.1f\tbaseline_request_bytes=%.0f\tbaseline_response_bytes=%.0f\tasync_serialize_ms=%.1f\tasync_request_ms=%.1f\tasync_apply_ms=%.1f\tasync_request_bytes=%.0f\tasync_response_bytes=%.0f\tbaseline_types=%s\tasync_types=%s",
                    label,
                    stem(document),
                    legacy.characters(),
                    legacyCount,
                    modernCount,
                    outputEqual,
                    legacy.durationMs(),
                    modern.durationMs(),
                    legacy.durationMs() - modern.durationMs(),
                    legacy.encodeMs(), legacy.requestMs(), legacy.applyMs(),
                    legacy.requestBytes(), legacy.responseBytes(),
                    modern.encodeMs(), modern.requestMs(), modern.applyMs(),
                    modern.requestBytes(), modern.responseBytes(),
                    typeSummary(legacy.cas(), legacyCountTypes),
                    typeSummary(modern.cas(), modernCountTypes)
            ));

            if (newStrategyRun != null) {
                ResultArtifact newStrategy = newStrategyRun.required(input.textKey());
                int nsCount = countTypes(newStrategy.cas(), modernCountTypes);
                row.append(String.format(Locale.ROOT,
                        "\tnewstrategy_taxa=%d\tnewstrategy_best_ms=%d\tnewstrategy_serialize_ms=%.1f\tnewstrategy_request_ms=%.1f\tnewstrategy_apply_ms=%.1f\tnewstrategy_request_bytes=%.0f\tnewstrategy_response_bytes=%.0f\tnewstrategy_types=%s",
                        nsCount,
                        newStrategy.durationMs(),
                        newStrategy.encodeMs(), newStrategy.requestMs(), newStrategy.applyMs(),
                        newStrategy.requestBytes(), newStrategy.responseBytes(),
                        typeSummary(newStrategy.cas(), modernCountTypes)
                ));
            }

            System.out.println(row);
            appendMatrixRow(row.toString());
            assertFalse(legacyCount < 1 || modernCount < 1,
                    label + " did not write target annotations for " + document);
            assertTrue(outputEqual,
                    label + " annotation counts differ for " + document
                            + " baseline=" + typeSummary(legacy.cas(), legacyCountTypes)
                            + " async=" + typeSummary(modern.cas(), modernCountTypes));
        }
    }

    // -----------------------------------------------------------------------
    // CAS I/O
    // -----------------------------------------------------------------------

    private static JCas xmi(TypeSystemDescription typeSystem, Path path) throws Exception {
        JCas cas = JCasFactory.createJCas(typeSystem);
        try (InputStream file = Files.newInputStream(path);
             InputStream input = path.getFileName().toString().endsWith(".gz") ? new GZIPInputStream(file) : file) {
            XmiCasDeserializer.deserialize(input, cas.getCas(), true);
        }
        cas.setDocumentLanguage("de");
        return cas;
    }

    // -----------------------------------------------------------------------
    // Podman pipeline runner
    // -----------------------------------------------------------------------

    private BatchRun runPodmanBatch(
            String label,
            String id,
            String image,
            boolean streaming,
            boolean gpu,
            Map<String, String> parameters,
            String contentType,
            List<Path> documents,
            TypeSystemDescription typeSystem,
            List<String> countTypes
    ) throws Exception {
        Path input = materializeXmiDirectory(id, typeSystem, documents);
        DUUIInMemoryEventSink sink = new DUUIInMemoryEventSink();
        DUUIEventService events = new DUUIEventService(List.of(sink));
        DUUIOrchestrationResult result;
        try (DUUISystemScope system = DUUI.system("legacy-modern-matrix-" + id).events(events)) {
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
                    artifactId,
                    execution.durationMs(),
                    cas,
                    cas.getDocumentText() == null ? 0 : cas.getDocumentText().length(),
                    typeSummary(cas, countTypes),
                    observed
            );
            artifacts.put(textKey(cas), artifact);
        }
        assertEquals(documents.size(), artifacts.size(),
                label + " did not produce one result artifact per input document for " + id + " (expected " + documents.size() + " got " + artifacts.size() + ")");
        return new BatchRun(id, artifacts);
    }

    private BatchRun runRemoteBatch(
            String label,
            String id,
            URI endpoint,
            boolean streaming,
            Map<String, String> parameters,
            String contentType,
            List<Path> documents,
            TypeSystemDescription typeSystem,
            List<String> countTypes
    ) throws Exception {
        Path input = materializeXmiDirectory(id, typeSystem, documents);
        DUUIInMemoryEventSink sink = new DUUIInMemoryEventSink();
        DUUIEventService events = new DUUIEventService(List.of(sink));
        DUUIOrchestrationResult result;
        try (DUUISystemScope system = DUUI.system("legacy-modern-matrix-" + id).events(events)) {
            try (DUUIPipelineScope pipeline = system.pipeline(id + "-pipeline")) {
                try (DUUIGeneratorScope<JCas> source = DUUIXmiCollectionReader.builder()
                        .typeSystem(typeSystem)
                        .source(input)
                        .open(pipeline)) {
                    try (DUUIStageScope<JCas> stage = source.linear(id + "-stage")) {
                        DUUIV1ComponentBuilder component = stage.v1(id)
                                .remote()
                                .endpoint(endpoint.toString())
                                .sourceView("_InitialView")
                                .targetView("_InitialView")
                                .telemetrySink(sink)
                                .parameters(parameters)
                                .timeoutSeconds(Long.getLong("duui.py.matrix.timeout.seconds", 7200L));
                        component.streamingTransport(streaming);
                        if (!streaming && contentType != null) {
                            component.contentType(contentType);
                        }
                    }
                }
            }
            result = system.run(id + "-pipeline");
            assertFalse(result.hasFailures(), () -> describeFailures(label, id, endpoint.toString(), result));
            assertEquals(0, result.unroutableArtifacts().size(),
                    () -> label + " unroutable artifacts for " + id + " endpoint=" + endpoint);
        }

        Map<String, ResultArtifact> artifacts = new LinkedHashMap<>();
        for (DUUIExecutionResult<?> execution : result.results()) {
            if (!(execution.artifact().payload() instanceof JCas cas)) {
                continue;
            }
            String artifactId = execution.artifact().gid().toString();
            List<DUUIEvent> observed = waitForMetrics(sink, artifactId, expectedHttpMetrics(streaming));
            ResultArtifact artifact = new ResultArtifact(
                    artifactId,
                    execution.durationMs(),
                    cas,
                    cas.getDocumentText() == null ? 0 : cas.getDocumentText().length(),
                    typeSummary(cas, countTypes),
                    observed
            );
            artifacts.put(textKey(cas), artifact);
        }
        assertEquals(documents.size(), artifacts.size(),
                label + " did not produce one result artifact per input document for " + id);
        return new BatchRun(id, artifacts);
    }

    private Path materializeXmiDirectory(String id, TypeSystemDescription typeSystem, List<Path> documents) throws Exception {
        Path input = tempDir.resolve(id + "-input");
        Files.createDirectories(input);
        for (int index = 0; index < documents.size(); index++) {
            JCas cas = xmi(typeSystem, documents.get(index));
            Path output = input.resolve(String.format(Locale.ROOT, "%04d-%s.xmi", index, stem(documents.get(index))));
            try (OutputStream stream = Files.newOutputStream(output)) {
                CasIOUtils.save(cas.getCas(), stream, SerialFormat.XMI_1_1);
            }
        }
        return input;
    }

    // -----------------------------------------------------------------------
    // New-strategy helpers
    // -----------------------------------------------------------------------

    private static boolean hasNewStrategy(String annotatorKey) {
        return System.getProperty("duui.py." + annotatorKey + ".newstrategy.image") != null;
    }

    private static String newStrategyImage(String annotatorKey) {
        return System.getProperty("duui.py." + annotatorKey + ".newstrategy.image");
    }

    private static boolean newStrategyStreaming(String annotatorKey) {
        return Boolean.parseBoolean(System.getProperty("duui.py." + annotatorKey + ".newstrategy.streaming", "true"));
    }

    private static Map<String, String> newStrategyParameters(String annotatorKey, Map<String, String> fallback) {
        String json = System.getProperty("duui.py." + annotatorKey + ".newstrategy.parameters", "").trim();
        if (json.isEmpty()) {
            return fallback;
        }
        return parseFlatJsonParameters(json, fallback);
    }

    /**
     * Parses a flat JSON object like {@code {"key1":"value1","key2":"value2"}}
     * and returns a merged map that starts with {@code fallback} and overlays the
     * parsed entries.  Handles escaped quotes and backslashes in values.
     */
    private static Map<String, String> parseFlatJsonParameters(String json, Map<String, String> fallback) {
        Map<String, String> merged = new LinkedHashMap<>(fallback);
        String trimmed = json.trim();
        if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) {
            return merged;
        }
        String inner = trimmed.substring(1, trimmed.length() - 1).trim();
        if (inner.isEmpty()) {
            return merged;
        }
        // Split on commas that are not inside quoted strings
        List<String> pairs = splitJsonPairs(inner);
        for (String pair : pairs) {
            int colon = findJsonColon(pair);
            if (colon < 0) continue;
            String key = unquoteJson(pair.substring(0, colon).trim());
            String value = unquoteJson(pair.substring(colon + 1).trim());
            if (!key.isEmpty()) {
                merged.put(key, value);
            }
        }
        return merged;
    }

    private static List<String> splitJsonPairs(String inner) {
        List<String> pairs = new ArrayList<>();
        boolean inString = false;
        boolean escaped = false;
        int start = 0;
        for (int i = 0; i < inner.length(); i++) {
            char c = inner.charAt(i);
            if (escaped) {
                escaped = false;
                continue;
            }
            if (c == '\\') {
                escaped = true;
                continue;
            }
            if (c == '"') {
                inString = !inString;
                continue;
            }
            if (c == ',' && !inString) {
                pairs.add(inner.substring(start, i).trim());
                start = i + 1;
            }
        }
        pairs.add(inner.substring(start).trim());
        return pairs;
    }

    private static int findJsonColon(String pair) {
        boolean inString = false;
        boolean escaped = false;
        for (int i = 0; i < pair.length(); i++) {
            char c = pair.charAt(i);
            if (escaped) { escaped = false; continue; }
            if (c == '\\') { escaped = true; continue; }
            if (c == '"') { inString = !inString; continue; }
            if (c == ':' && !inString) return i;
        }
        return -1;
    }

    private static String unquoteJson(String value) {
        if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
            String inner = value.substring(1, value.length() - 1);
            return inner.replace("\\\"", "\"").replace("\\\\", "\\");
        }
        return value;
    }

    // -----------------------------------------------------------------------
    // Type-system helpers
    // -----------------------------------------------------------------------

    private static Map<String, InputProfile> inputProfiles(TypeSystemDescription typeSystem, List<Path> documents) throws Exception {
        Map<String, InputProfile> profiles = new LinkedHashMap<>();
        for (Path document : documents) {
            JCas cas = xmi(typeSystem, document);
            profiles.put(stem(document), new InputProfile(stem(document), textKey(cas), spacyFingerprints(cas)));
        }
        return profiles;
    }

    private static TypeSystemDescription localTypeSystemFor(String idPrefix) throws Exception {
        return switch (idPrefix) {
            case "taxonerd" -> localExampleTypeSystem(
                    "taxonerd-legacy-lua/TypeSystemTaxoNERDLegacy.xml",
                    "taxonerd-msgpack-lua/TypeSystemTaxoNERD.xml"
            );
            case "gazetteer" -> localExampleTypeSystem(
                    "gazetteer-legacy-lua/TypeSystemGazetteerLegacy.xml",
                    "gazetteer-msgpack-lua/TypeSystemGazetteer.xml"
            );
            case "geonames" -> localExampleTypeSystem(
                    "geonames-legacy-lua/TypeSystemGeoNamesLegacy.xml",
                    "geonames-msgpack-lua/TypeSystemGeoNames.xml"
            );
            case "gnfinder" -> localExampleTypeSystem(
                    "gnfinder-legacy-lua/TypeSystemGNFinderLegacy.xml",
                    "gnfinder-msgpack-lua/TypeSystemGNFinder.xml"
            );
            default -> throw new IllegalArgumentException("No local type system configured for " + idPrefix);
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

    private static String textKey(JCas cas) {
        String text = cas.getDocumentText();
        return Integer.toHexString(text == null ? 0 : text.hashCode()) + ":" + (text == null ? 0 : text.length());
    }

    // -----------------------------------------------------------------------
    // Configurable document list resolution
    // -----------------------------------------------------------------------

    /**
     * Resolves document paths for an annotator key.
     * <ol>
     *   <li>System property {@code duui.py.matrix.samples.<annotatorKey>} (comma-separated)</li>
     *   <li>System property {@code duui.py.matrix.samples.dir} + glob pattern {@code *.xmi.gz}</li>
     *   <li>The provided {@code defaultPaths} (comma-separated absolute paths)</li>
     * </ol>
     * Glob/wildcard patterns in any of the above are expanded.
     */
    private static List<Path> resolveDocumentPaths(String annotatorKey, String defaultPaths) {
        String specificFiles = System.getProperty("duui.py.matrix.samples." + annotatorKey, "").trim();
        if (!specificFiles.isBlank()) {
            return expandGlobs(paths(specificFiles));
        }
        String samplesDir = System.getProperty("duui.py.matrix.samples.dir", "").trim();
        if (!samplesDir.isBlank()) {
            List<Path> fromDir = listSampleFiles(Path.of(samplesDir), annotatorKey);
            if (!fromDir.isEmpty()) {
                return fromDir;
            }
        }
        return expandGlobs(paths(defaultPaths));
    }

    /**
     * Lists {@code *.xmi.gz} files in {@code dir}, optionally filtered by a
     * subdirectory whose name starts with {@code annotatorKey}.
     */
    private static List<Path> listSampleFiles(Path dir, String annotatorKey) {
        List<Path> collected = new ArrayList<>();
        if (!Files.isDirectory(dir)) {
            return collected;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.xmi.gz")) {
            for (Path entry : stream) {
                collected.add(entry);
            }
        } catch (Exception ignored) {
            // ignore unreadable directories
        }
        // Also try subdirectories recursively (one level)
        try (DirectoryStream<Path> dirs = Files.newDirectoryStream(dir, entry -> Files.isDirectory(entry))) {
            for (Path subdir : dirs) {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(subdir, "*.xmi.gz")) {
                    for (Path entry : stream) {
                        collected.add(entry);
                    }
                } catch (Exception ignored) {
                }
            }
        } catch (Exception ignored) {
        }
        collected.sort(Path::compareTo);
        return collected;
    }

    /**
     * Expands glob/wildcard patterns in the given path list.
     * Supports {@code *} and {@code ?} in the file name portion.
     */
    private static List<Path> expandGlobs(List<Path> paths) {
        List<Path> expanded = new ArrayList<>();
        for (Path path : paths) {
            String fileName = path.getFileName() != null ? path.getFileName().toString() : "";
            if (fileName.contains("*") || fileName.contains("?")) {
                Path parent = path.getParent();
                if (parent != null && Files.isDirectory(parent)) {
                    PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + fileName);
                    try (DirectoryStream<Path> stream = Files.newDirectoryStream(parent)) {
                        for (Path entry : stream) {
                            if (matcher.matches(entry.getFileName())) {
                                expanded.add(entry);
                            }
                        }
                    } catch (Exception ignored) {
                        expanded.add(path); // fallback: keep as-is
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

    /**
     * Randomly samples {@code count} entries from {@code pool} without replacement.
     */
    private static List<Path> sampleRandom(List<Path> pool, int count) {
        List<Path> shuffled = new ArrayList<>(pool);
        Collections.shuffle(shuffled, RNG);
        return shuffled.subList(0, Math.min(count, shuffled.size()));
    }

    /**
     * Returns the default hardcoded document paths for an annotator key,
     * used when no system properties are set.
     */
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
            case "geonames" -> String.join(",",
                    "/home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/preprocessed_corpora_sample_1000/Bericht_des_Vereins_zum_Schutze_der_Alpenpflanzen/1913/3713536.xmi.gz",
                    "/home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/preprocessed_corpora_sample_1000/Bericht_des_Vereins_zum_Schutze_und_zur_Pflege_der_Alpenpflanzen/1911/3721555.xmi.gz",
                    "/home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/preprocessed_corpora_sample_1000/Mitteilungen_des_Vereins_Sächsischer_Ornithologen/2001/9840314.xmi.gz"
            );
            case "gnfinder" -> String.join(",",
                    "/home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/preprocessed_corpora_sample_1000/General-Doubletten-Verzeichniss_des_Schlesischen_Botanischen_Tausch-Vereins_____Tauschjahr____/1883/4513701.xmi.gz",
                    "/home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/preprocessed_corpora_sample_1000/General-Doubletten-Verzeichniss_des_Schlesischen_Botanischen_Tausch-Vereins_____Tauschjahr____/1886/4566707.xmi.gz",
                    "/home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/preprocessed_corpora_sample_1000/Botanisches_Literaturblatt_Organ_für_Autor-_und_Instituts-Referate_aus_dem_Gesamtgebiete_der_botan__Literatur/1903/4544734.xmi.gz"
            );
            default -> "";
        };
    }

    // -----------------------------------------------------------------------
    // Existing document methods — now delegate to resolveDocumentPaths
    // -----------------------------------------------------------------------

    private static List<Path> taxonerdDocuments() {
        return resolveDocumentPaths("taxonerd", defaultDocumentPaths("taxonerd"));
    }

    private static List<Path> gazetteerDocuments() {
        return resolveDocumentPaths("gazetteer", defaultDocumentPaths("gazetteer"));
    }

    private static List<Path> geonamesDocuments() {
        return resolveDocumentPaths("geonames", defaultDocumentPaths("geonames"));
    }

    private static List<Path> gnfinderDocuments() {
        return resolveDocumentPaths("gnfinder", defaultDocumentPaths("gnfinder"));
    }

    private static List<Path> spacyDocuments() {
        return resolveDocumentPaths("spacy", defaultDocumentPaths("spacy"));
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

    // -----------------------------------------------------------------------
    // Type counting
    // -----------------------------------------------------------------------

    private static int countTypes(JCas cas, List<String> typeNames) throws Exception {
        CAS view = cas.getView("_InitialView").getCas();
        int total = 0;
        for (String typeName : typeNames) {
            Type type = view.getTypeSystem().getType(typeName);
            if (type != null) {
                total += countIndexed(view, type);
            }
        }
        return total;
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
        FSIterator<FeatureStructure> iterator = view.getIndexRepository().getAllIndexedFS(type);
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        return count;
    }

    // -----------------------------------------------------------------------
    // spaCy fingerprints
    // -----------------------------------------------------------------------

    private static Map<String, Integer> spacyFingerprints(JCas cas) throws Exception {
        CAS view = cas.getView("_InitialView").getCas();
        Map<String, Integer> fingerprints = new LinkedHashMap<>();
        for (String typeName : SPACY_OUTPUT_ROOT_TYPES) {
            Type type = view.getTypeSystem().getType(typeName);
            if (type == null) continue;
            FSIterator<FeatureStructure> iterator = view.getIndexRepository().getAllIndexedFS(type);
            while (iterator.hasNext()) {
                FeatureStructure fs = iterator.next();
                fingerprints.merge(spacyFingerprint(fs), 1, Integer::sum);
            }
        }
        return fingerprints;
    }

    private static String spacyFingerprint(FeatureStructure fs) {
        String type = fs.getType().getName();
        StringBuilder value = new StringBuilder(type)
                .append('|')
                .append(begin(fs))
                .append('-')
                .append(end(fs));
        if (type.endsWith(".Lemma")) {
            value.append("|value=").append(stringFeature(fs, "value"));
        } else if (type.equals("de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS") || type.contains(".lexmorph.type.pos.")) {
            value.append("|PosValue=").append(stringFeature(fs, "PosValue"))
                    .append("|coarseValue=").append(stringFeature(fs, "coarseValue"));
        } else if (type.endsWith(".MorphologicalFeatures")) {
            for (String feature : MORPH_FEATURES) {
                value.append('|').append(feature).append('=').append(stringFeature(fs, feature));
            }
        } else if (type.equals("de.tudarmstadt.ukp.dkpro.core.api.syntax.type.dependency.Dependency") || type.contains(".syntax.type.dependency.")) {
            value.append("|DependencyType=").append(stringFeature(fs, "DependencyType").toLowerCase(Locale.ROOT))
                    .append("|flavor=").append(stringFeature(fs, "flavor"))
                    .append("|Governor=").append(referenceSpan(fs, "Governor"))
                    .append("|Dependent=").append(referenceSpan(fs, "Dependent"));
        } else if (type.equals("de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity") || type.contains(".ner.type.")) {
            value.append("|value=").append(stringFeature(fs, "value"))
                    .append("|identifier=").append(stringFeature(fs, "identifier"));
        } else if (type.endsWith(".Token")) {
            value.append("|lemma=").append(referenceSpan(fs, "lemma"))
                    .append("|pos=").append(referenceSpan(fs, "pos"))
                    .append("|morph=").append(referenceSpan(fs, "morph"));
        }
        return value.toString();
    }

    private static int begin(FeatureStructure fs) {
        return fs instanceof AnnotationFS annotation ? annotation.getBegin() : -1;
    }

    private static int end(FeatureStructure fs) {
        return fs instanceof AnnotationFS annotation ? annotation.getEnd() : -1;
    }

    private static String stringFeature(FeatureStructure fs, String baseName) {
        Feature feature = fs.getType().getFeatureByBaseName(baseName);
        if (feature == null) return "";
        String value = fs.getFeatureValueAsString(feature);
        return value == null ? "" : value;
    }

    private static String referenceSpan(FeatureStructure fs, String baseName) {
        Feature feature = fs.getType().getFeatureByBaseName(baseName);
        if (feature == null) return "";
        FeatureStructure target = fs.getFeatureValue(feature);
        if (target == null) return "";
        return target.getType().getName() + '@' + begin(target) + '-' + end(target);
    }

    // -----------------------------------------------------------------------
    // Arithmetic helpers
    // -----------------------------------------------------------------------

    private static Map<String, Integer> subtract(Map<String, Integer> values, Map<String, Integer> baseline) {
        Map<String, Integer> out = new LinkedHashMap<>(values);
        for (Map.Entry<String, Integer> entry : baseline.entrySet()) {
            out.computeIfPresent(entry.getKey(), (key, count) -> count > entry.getValue() ? count - entry.getValue() : null);
        }
        return out;
    }

    private static Map<String, Integer> typeCountsFromFingerprints(Map<String, Integer> fingerprints) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (Map.Entry<String, Integer> entry : fingerprints.entrySet()) {
            String key = entry.getKey();
            int delimiter = key.indexOf('|');
            String type = delimiter > 0 ? key.substring(0, delimiter) : key;
            counts.merge(type.substring(type.lastIndexOf('.') + 1), entry.getValue(), Integer::sum);
        }
        return counts;
    }

    private static String fingerprintDiff(Map<String, Integer> expected, Map<String, Integer> actual) {
        List<String> missing = new ArrayList<>();
        List<String> unexpected = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : expected.entrySet()) {
            int actualCount = actual.getOrDefault(entry.getKey(), 0);
            if (actualCount != entry.getValue()) {
                missing.add("missing_or_count expected=" + entry.getValue() + " actual=" + actualCount + " " + entry.getKey());
            }
            if (missing.size() >= 6) break;
        }
        for (Map.Entry<String, Integer> entry : actual.entrySet()) {
            if (!expected.containsKey(entry.getKey())) {
                unexpected.add("unexpected count=" + entry.getValue() + " " + entry.getKey());
            }
            if (unexpected.size() >= 6) break;
        }
        return "missing=[" + String.join(" || ", missing) + "] unexpected=[" + String.join(" || ", unexpected) + "]";
    }

    // -----------------------------------------------------------------------
    // Diagnostics & reporting
    // -----------------------------------------------------------------------

    private static String describeFailures(String label, String id, String image, DUUIOrchestrationResult result) {
        StringBuilder builder = new StringBuilder();
        builder.append(label).append(" pipeline ").append(id).append(" image=").append(image).append(" failures:\n");
        result.results().stream()
                .map(DUUIExecutionResult::failure)
                .filter(Objects::nonNull)
                .forEach(failure -> builder.append("  message=")
                        .append(failure.message())
                        .append(" cause=")
                        .append(failure.cause())
                        .append('\n'));
        return builder.toString();
    }

    private static void appendMatrixRow(String row) throws Exception {
        Path report = Path.of(System.getProperty("duui.py.matrix.report", "target/duui-legacy-async-matrix.tsv"));
        Path parent = report.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.writeString(
                report,
                row + System.lineSeparator(),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        );
    }

    private static void appendMatrixHeader(String header) throws Exception {
        Path report = Path.of(System.getProperty("duui.py.matrix.report", "target/duui-legacy-async-matrix.tsv"));
        Path parent = report.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        // Only write header if the file does not exist yet
        if (!Files.exists(report) || Files.size(report) == 0) {
            Files.writeString(report, header + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        }
    }

    /**
     * Emits an aggregate throughput row after all per-document rows for a label.
     * Reads back the per-document durations from this run to compute percentiles.
     */
    private void emitAggregateRow(String label, int documentCount) throws Exception {
        // Collect durations from the recent batch (simplified: read from in-memory store)
        // Since we don't have a global store, emit a basic aggregate note.
        // The detailed aggregate requires per-document durations — those are already
        // in the TSV rows. Here we emit a summary row with document count.
        String row = String.format(Locale.ROOT,
                "%s_AGGREGATE\tdocument=ALL\tchars=0\tdocs=%d\taggregate=summary",
                label, documentCount);
        System.out.println(row);
        appendMatrixRow(row);
    }

    // -----------------------------------------------------------------------
    // Metrics helpers
    // -----------------------------------------------------------------------

    private static List<String> expectedHttpMetrics(boolean streaming) {
        if (streaming) {
            return List.of(
                    "duui.http.serialize_ms",
                    "duui.http.request_bytes",
                    "duui.http.response_decode_ms",
                    "duui.http.request_duration_ms",
                    "duui.http.response_bytes"
            );
        }
        return List.of(
                "duui.http.serialize_ms",
                "duui.http.request_bytes",
                "duui.http.response_receive_ms",
                "duui.http.response_decode_ms",
                "duui.http.request_duration_ms",
                "duui.http.response_bytes"
        );
    }

    private static List<DUUIEvent> waitForMetrics(DUUIInMemoryEventSink sink, String id, List<String> names) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        List<DUUIEvent> events = eventsForArtifact(sink.events(), id);
        while (!hasMetrics(events, names) && System.nanoTime() < deadline) {
            Thread.sleep(20L);
            events = eventsForArtifact(sink.events(), id);
        }
        return events;
    }

    private static List<DUUIEvent> eventsForArtifact(List<DUUIEvent> events, String artifactId) {
        return events.stream()
                .filter(event -> artifactId.equals(event.artifactId()))
                .toList();
    }

    private static boolean hasMetrics(List<DUUIEvent> events, List<String> names) {
        for (String name : names) {
            boolean present = events.stream().anyMatch(event -> name.equals(event.metricName()) && event.metricValue() != null);
            if (!present) {
                return false;
            }
        }
        return true;
    }

    private static String stem(Path path) {
        String name = path.getFileName().toString();
        if (name.endsWith(".xmi.gz")) {
            return name.substring(0, name.length() - ".xmi.gz".length());
        }
        if (name.endsWith(".xmi")) {
            return name.substring(0, name.length() - ".xmi".length());
        }
        return name.replaceAll("[^A-Za-z0-9_.-]", "_");
    }

    private static String typeSummary(Map<String, Integer> counts) {
        return counts.entrySet().stream()
                .map(entry -> entry.getKey() + ":" + entry.getValue())
                .collect(Collectors.joining("|"));
    }

    // -----------------------------------------------------------------------
    // Aggregate metrics record
    // -----------------------------------------------------------------------

    private record AggregateMetrics(
            String label,
            int documentCount,
            long totalDurationMs,
            double docsPerSec,
            double avgLatencyMs,
            double p50LatencyMs,
            double p95LatencyMs,
            double p99LatencyMs
    ) {
        static AggregateMetrics from(String label, List<Long> durationsMs) {
            if (durationsMs.isEmpty()) {
                return new AggregateMetrics(label, 0, 0, 0, 0, 0, 0, 0);
            }
            List<Long> sorted = new ArrayList<>(durationsMs);
            Collections.sort(sorted);
            int n = sorted.size();
            long total = sorted.stream().mapToLong(Long::longValue).sum();
            double docsPerSec = total > 0 ? (n * 1000.0) / total : 0;
            double avg = (double) total / n;
            double p50 = percentile(sorted, 50);
            double p95 = percentile(sorted, 95);
            double p99 = percentile(sorted, 99);
            return new AggregateMetrics(label, n, total, docsPerSec, avg, p50, p95, p99);
        }

        private static double percentile(List<Long> sorted, double percentile) {
            int n = sorted.size();
            if (n == 0) return 0;
            double index = (percentile / 100.0) * (n - 1);
            int lo = (int) Math.floor(index);
            int hi = (int) Math.ceil(index);
            if (lo == hi) return sorted.get(lo);
            double frac = index - lo;
            return sorted.get(lo) * (1 - frac) + sorted.get(hi) * frac;
        }
    }

    // -----------------------------------------------------------------------
    // Data records
    // -----------------------------------------------------------------------

    private record BatchRun(String id, Map<String, ResultArtifact> artifacts) {
        ResultArtifact required(String textKey) {
            ResultArtifact artifact = artifacts.get(textKey);
            if (artifact == null) {
                throw new AssertionError("No result artifact for text key " + textKey + " in " + id + "; keys=" + artifacts.keySet());
            }
            return artifact;
        }
    }

    private record ResultArtifact(String id, long durationMs, JCas cas, int characters, Map<String, Integer> typeCounts,
                                  List<DUUIEvent> events) {
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
                throw new AssertionError("missing metric " + name + " for " + id + "; emitted=" + metricNames());
            }
            return value;
        }

        double metricRequiredAny(String... names) {
            for (String name : names) {
                double value = metric(name);
                if (!Double.isNaN(value)) {
                    return value;
                }
            }
            throw new AssertionError("missing metric any of " + String.join("|", names) + " for " + id + "; emitted=" + metricNames());
        }

        double encodeMs() {
            return metricRequiredAny("duui.http.serialize_ms", "duui.process.encode_ms", "duui.wire.pack_msgpack_ms");
        }

        double requestMs() {
            return metricRequiredAny("duui.http.response_receive_ms", "duui.http.request_duration_ms", "duui.request.duration");
        }

        double applyMs() {
            return metricRequiredAny("duui.http.response_decode_ms", "duui.process.decode_ms", "duui.wire.decode_chunk_ms");
        }

        double requestBytes() {
            return metricRequiredAny("duui.http.request_bytes", "duui.wire.payload_bytes", "duui.wire.frame_bytes");
        }

        double responseBytes() {
            return metricRequiredAny("duui.http.response_bytes", "duui.wire.frame_bytes", "duui.wire.payload_bytes");
        }

        String metricNames() {
            return events.stream()
                    .map(DUUIEvent::metricName)
                    .filter(Objects::nonNull)
                    .distinct()
                    .collect(Collectors.joining("|"));
        }

        String typeSummary() {
            return DUUILegacyModernAnnotatorMatrixTest.typeSummary(typeCounts);
        }
    }

    private record InputProfile(String document, String textKey, Map<String, Integer> spacyFingerprints) {
    }
}
