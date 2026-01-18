package org.texttechnologylab.DockerUnifiedUIMAInterface.model;

import java.util.List;

/**
 * Descriptor returned by {@code GET /v1/details/input_output} for DUUI-BIN components.
 *
 * This model aligns with the Python annotator sketch in {@code docs/sketch-msgpck-annotator}.
 *
 * <p>Notes:
 * <ul>
 *   <li>{@code sourceView}/{@code targetView} are view routing hints for DUUI; if empty, DUUI uses its defaults.</li>
 *   <li>{@code input.optional_inputs} describes which indexed feature structures DUUI should include in requests.</li>
 *   <li>{@code exclude} allows pruning expensive features from serialization.</li>
 * </ul>
 */
public record AnnotatorDescriptor(
    String name,
    String version,
    InputDesc input,
    OutputDesc output,
    String sourceView,
    String targetView
) {
    /**
     * SofA constraints for a component.
     *
     * @param mimeType Supported mime types (may include alternatives using {@code |}, e.g. {@code "audio/*|text/plain"}).
     * @param language Document language (e.g. {@code "en"} or {@code "x-unspecified"}).
     */
    public record SofaDesc(String mimeType, String language) {}

    /**
     * Domain constraints for input documents.
     *
     * @param sofa Supported SofA description for incoming requests.
     * @param optional_types Optional allow-list for selection semantics. Annotator may use 
     *                       the selected types spans for analysis instead of the complete sofa. (reserved; may be empty).
     */
    public record Domain(SofaDesc sofa, List<String> optional_types) {}

    /**
     * Controls which features are skipped during serialization.
     *
     * <p>Semantics:
     * <ul>
     *   <li>{@code features}: exclude by feature short name (e.g. {@code "vector"}).</li>
     *   <li>{@code ranges}: exclude features whose declared range type is subsumed by one of these type names.</li>
     *   <li>{@code types}: exclude features whose feature value is (or contains, for FS arrays/lists) a FS
     *       whose type is subsumed by one of these type names.</li>
     * </ul>
     */
    public record ExcludeSpec(List<String> features, List<String> ranges, List<String> types) {}

    /**
     * Declares a seed type to serialize from the CAS, plus an optional exclusion spec.
     */
    public record InputTypeSpec(String type, ExcludeSpec exclude) {}

    /**
     * Input description for the annotator.
     *
     * @param domain Input SofA constraints and (reserved) optional selection types.
     * @param optional_inputs List of types to serialize from the CAS (usually UIMA annotations and their referenced FS graph).
     */
    public record InputDesc(Domain domain, List<InputTypeSpec> optional_inputs) {}

    /**
     * Output description for the annotator.
     *
     * @param sofa Output SofA constraints.
     * @param types Type names that may be produced by the annotator (may be empty).
     */
    public record OutputDesc(SofaDesc sofa, List<String> types) {}
}
