package org.texttechnologylab.duui.util;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declarative annotation for method-level profiling.
 *
 * <p>Place on a method to indicate that its execution should be automatically wrapped
 * in a {@link DUUIProfiler.Span}. The annotation value specifies the phase name
 * (defaults to the method name).</p>
 *
 * <pre>{@code
 * {@literal @}DUUITimed("serialize")
 * public byte[] serialize(JCas cas) {
 *     // ...
 * }
 * }</pre>
 *
 * <p>This annotation is a marker consumed by framework-level interceptors or wrappers.
 * It has no effect unless the calling code explicitly checks for it and opens a
 * {@link DUUIProfiler.Span}.</p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface DUUITimed {

    /**
     * The phase name for this profiling span. Defaults to the annotated method's name.
     */
    String value() default "";
}
