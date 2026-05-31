package org.texttechnologylab.duui.util;

/**
 * Annotation for explicit lifecycle profiling hooks.
 *
 * <p>Place on fields, methods, or classes that participate in DUUI's lifecycle
 * and should be profiled. This is a weaker contract than {@link DUUITimed} — it
 * signals that the annotated element is a profiling candidate rather than
 * guaranteeing automatic instrumentation.</p>
 */
@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
@java.lang.annotation.Target({
        java.lang.annotation.ElementType.TYPE,
        java.lang.annotation.ElementType.METHOD,
        java.lang.annotation.ElementType.FIELD
})
public @interface DUUIProfile {

    /**
     * The lifecycle phase category (e.g., "serialize", "process", "deserialize").
     */
    String value() default "";
}
