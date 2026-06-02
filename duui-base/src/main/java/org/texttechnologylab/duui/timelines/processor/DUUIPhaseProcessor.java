package org.texttechnologylab.duui.timelines.processor;

import org.texttechnologylab.duui.timelines.Phase;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import java.util.Set;

@SupportedAnnotationTypes("org.texttechnologylab.duui.timelines.Phase")
@SupportedSourceVersion(SourceVersion.RELEASE_21)
public final class DUUIPhaseProcessor extends AbstractProcessor {
    private Messager messager;

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        this.messager = processingEnv.getMessager();
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        for (Element element : roundEnv.getElementsAnnotatedWith(Phase.class)) {
            if (element.getKind() != ElementKind.METHOD) {
                error(element, "@Phase can only be applied to methods.");
                continue;
            }
            ExecutableElement method = (ExecutableElement) element;
            if (!method.getModifiers().contains(Modifier.PUBLIC)) {
                error(method, "@Phase methods must be public so the DUUI phase aspect can call them.");
            }
        }
        return false;
    }

    private void error(Element element, String message) {
        messager.printMessage(Diagnostic.Kind.ERROR, message, element);
    }
}
