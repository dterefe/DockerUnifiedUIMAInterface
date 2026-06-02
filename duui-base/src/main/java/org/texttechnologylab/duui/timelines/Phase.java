package org.texttechnologylab.duui.timelines;

import org.texttechnologylab.duui.orchestration.scheduling.DUUIDispatchMode;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Phase {
    DUUIStatus value();

    DUUIDispatchMode dispatch() default DUUIDispatchMode.MIXED;
}
