package org.texttechnologylab.duui.timelines.processor;

import org.texttechnologylab.duui.timelines.Phase;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
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
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.TypeParameterElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.ElementFilter;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@SupportedAnnotationTypes("org.texttechnologylab.duui.timelines.Phase")
@SupportedSourceVersion(SourceVersion.RELEASE_21)
public final class DUUIPhaseProcessor extends AbstractProcessor {
    private Filer filer;
    private Messager messager;

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        this.filer = processingEnv.getFiler();
        this.messager = processingEnv.getMessager();
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        Map<TypeElement, List<ExecutableElement>> byOwner = new LinkedHashMap<>();
        for (Element element : roundEnv.getElementsAnnotatedWith(Phase.class)) {
            if (element.getKind() != ElementKind.METHOD) {
                error(element, "@Phase can only be applied to methods.");
                continue;
            }
            ExecutableElement method = (ExecutableElement) element;
            if (!method.getModifiers().contains(Modifier.PUBLIC)) {
                error(method, "@Phase methods must be public so generated dispatch wrappers can call them.");
                continue;
            }
            TypeElement owner = (TypeElement) method.getEnclosingElement();
            byOwner.computeIfAbsent(owner, ignored -> new ArrayList<>()).add(method);
        }

        for (Map.Entry<TypeElement, List<ExecutableElement>> entry : byOwner.entrySet()) {
            try {
                writeDispatch(entry.getKey(), entry.getValue());
            } catch (IOException e) {
                error(entry.getKey(), "Failed to generate phase dispatcher: " + e.getMessage());
            }
        }
        return false;
    }

    private void writeDispatch(TypeElement owner, List<ExecutableElement> methods) throws IOException {
        PackageElement pkg = processingEnv.getElementUtils().getPackageOf(owner);
        String packageName = pkg.isUnnamed() ? "" : pkg.getQualifiedName().toString();
        String ownerSimpleName = owner.getSimpleName().toString();
        String generatedSimpleName = ownerSimpleName + "PhaseDispatch";
        String generatedName = packageName.isBlank() ? generatedSimpleName : packageName + "." + generatedSimpleName;
        List<ExecutableElement> ordered = methods.stream()
                .sorted(Comparator.comparing((ExecutableElement method) -> method.getSimpleName().toString())
                        .thenComparing(method -> method.getParameters().size()))
                .toList();

        JavaFileObject file = filer.createSourceFile(generatedName, owner);
        try (Writer out = file.openWriter()) {
            if (!packageName.isBlank()) {
                out.write("package " + packageName + ";\n\n");
            }
            out.write("import org.texttechnologylab.duui.timelines.DUUIPhaseAspect;\n");
            out.write("import org.texttechnologylab.duui.timelines.DUUIDispatcher;\n");
            out.write("import org.texttechnologylab.duui.ems.DUUIActor;\n");
            out.write("import java.lang.reflect.Method;\n");
            out.write("import java.util.List;\n\n");
            out.write("final class " + generatedSimpleName + " {\n");
            out.write("    private static final DUUIDispatcher DEFAULT_DISPATCHER = new DUUIDispatcher();\n\n");
            out.write("    private " + generatedSimpleName + "() {\n");
            out.write("    }\n\n");

            int index = 0;
            for (ExecutableElement method : ordered) {
                writeMethod(out, owner, method, index++);
            }

            writeActorResolver(out);
            out.write("    private static Method phaseMethod(String name, Class<?>... parameterTypes) {\n");
            out.write("        try {\n");
            out.write("            Method method = " + ownerSimpleName + ".class.getDeclaredMethod(name, parameterTypes);\n");
            out.write("            method.setAccessible(true);\n");
            out.write("            return method;\n");
            out.write("        } catch (NoSuchMethodException e) {\n");
            out.write("            throw new ExceptionInInitializerError(e);\n");
            out.write("        }\n");
            out.write("    }\n");
            out.write("}\n");
        }
    }

    private void writeMethod(Writer out, TypeElement owner, ExecutableElement method, int index) throws IOException {
        String methodName = method.getSimpleName().toString();
        String constant = "METHOD_" + index + "_" + methodName.toUpperCase();
        List<? extends VariableElement> parameters = method.getParameters();
        String parameterClasses = parameters.stream()
                .map(parameter -> classLiteral(parameter.asType()))
                .collect(Collectors.joining(", "));
        out.write("    private static final Method " + constant + " = phaseMethod(\"" + methodName + "\"");
        if (!parameterClasses.isBlank()) {
            out.write(", " + parameterClasses);
        }
        out.write(");\n\n");

        TypeMirror returnType = method.getReturnType();
        boolean returnsVoid = returnType.getKind() == TypeKind.VOID;
        out.write("    static " + typeParameters(owner) + returnType + " " + methodName + "(" + ownerType(owner) + " owner");
        for (VariableElement parameter : parameters) {
            out.write(", " + parameter.asType() + " " + parameter.getSimpleName());
        }
        out.write(") throws Exception {\n");
        out.write("        ");
        if (!returnsVoid) {
            out.write("return ");
        }
        out.write("aspect(owner).around(owner, " + constant + ", actors(owner, \"" + methodName + "\"");
        for (VariableElement parameter : parameters) {
            out.write(", " + parameter.getSimpleName());
        }
        out.write("), () -> {\n");
        out.write("            ");
        if (!returnsVoid) {
            out.write("return ");
        }
        out.write("owner." + methodName + "(");
        out.write(parameters.stream()
                .map(parameter -> parameter.getSimpleName().toString())
                .collect(Collectors.joining(", ")));
        out.write(");\n");
        if (returnsVoid) {
            out.write("            return null;\n");
        }
        out.write("        });\n");
        out.write("    }\n\n");
        writeAsyncMethod(out, owner, method, constant);
    }

    private void writeAsyncMethod(Writer out, TypeElement owner, ExecutableElement method, String constant) throws IOException {
        String methodName = method.getSimpleName().toString();
        List<? extends VariableElement> parameters = method.getParameters();
        TypeMirror returnType = method.getReturnType();
        boolean returnsVoid = returnType.getKind() == TypeKind.VOID;
        boolean returnsCompletion = isCompletion(returnType);
        String asyncReturn = returnsVoid ? "java.util.concurrent.CompletableFuture<Void>"
                : returnsCompletion ? returnType.toString()
                : "java.util.concurrent.CompletableFuture<" + boxed(returnType) + ">";
        out.write("    static " + typeParameters(owner) + asyncReturn + " " + methodName + "Async(" + ownerType(owner) + " owner");
        for (VariableElement parameter : parameters) {
            out.write(", " + parameter.asType() + " " + parameter.getSimpleName());
        }
        out.write(") {\n");
        if (returnsCompletion) {
            out.write("        return (" + returnType + ") aspect(owner).aroundCompletion(owner, " + constant + ", actors(owner, \"" + methodName + "\"");
            for (VariableElement parameter : parameters) {
                out.write(", " + parameter.getSimpleName());
            }
            out.write("), () -> owner." + methodName + "(");
            out.write(parameters.stream().map(parameter -> parameter.getSimpleName().toString()).collect(Collectors.joining(", ")));
            out.write("));\n");
        } else {
            out.write("        return aspect(owner).aroundAsync(owner, " + constant + ", actors(owner, \"" + methodName + "\"");
            for (VariableElement parameter : parameters) {
                out.write(", " + parameter.getSimpleName());
            }
            out.write("), () -> {\n");
            out.write("            ");
            if (!returnsVoid) {
                out.write("return ");
            }
            out.write("owner." + methodName + "(");
            out.write(parameters.stream().map(parameter -> parameter.getSimpleName().toString()).collect(Collectors.joining(", ")));
            out.write(");\n");
            if (returnsVoid) {
                out.write("            return null;\n");
            }
            out.write("        });\n");
        }
        out.write("    }\n\n");
    }

    private boolean isCompletion(TypeMirror type) {
        String erased = processingEnv.getTypeUtils().erasure(type).toString();
        return "java.util.concurrent.CompletableFuture".equals(erased)
                || "java.util.concurrent.CompletionStage".equals(erased);
    }

    private String boxed(TypeMirror type) {
        return switch (type.getKind()) {
            case BOOLEAN -> "Boolean";
            case BYTE -> "Byte";
            case SHORT -> "Short";
            case INT -> "Integer";
            case LONG -> "Long";
            case CHAR -> "Character";
            case FLOAT -> "Float";
            case DOUBLE -> "Double";
            default -> type.toString();
        };
    }

    private String typeParameters(TypeElement owner) {
        List<? extends TypeParameterElement> parameters = owner.getTypeParameters();
        if (parameters.isEmpty()) {
            return "";
        }
        return parameters.stream()
                .map(TypeParameterElement::toString)
                .collect(Collectors.joining(", ", "<", "> "));
    }

    private String ownerType(TypeElement owner) {
        List<? extends TypeParameterElement> parameters = owner.getTypeParameters();
        if (parameters.isEmpty()) {
            return owner.getQualifiedName().toString();
        }
        String arguments = parameters.stream()
                .map(parameter -> parameter.getSimpleName().toString())
                .collect(Collectors.joining(", ", "<", ">"));
        return owner.getQualifiedName() + arguments;
    }

    private void writeActorResolver(Writer out) throws IOException {
        out.write("    private static DUUIPhaseAspect aspect(Object owner) {\n");
        out.write("        try {\n");
        out.write("            Method method = owner.getClass().getDeclaredMethod(\"dispatcher\");\n");
        out.write("            method.setAccessible(true);\n");
        out.write("            Object dispatcher = method.invoke(owner);\n");
        out.write("            if (dispatcher instanceof DUUIDispatcher duuiDispatcher) {\n");
        out.write("                return new DUUIPhaseAspect(duuiDispatcher);\n");
        out.write("            }\n");
        out.write("        } catch (NoSuchMethodException ignored) {\n");
        out.write("        } catch (ReflectiveOperationException e) {\n");
        out.write("            throw new IllegalStateException(e);\n");
        out.write("        }\n");
        out.write("        return new DUUIPhaseAspect(DEFAULT_DISPATCHER);\n");
        out.write("    }\n\n");
        out.write("    private static List<DUUIActor> actors(Object owner, String phaseMethod, Object... args) {\n");
        out.write("        try {\n");
        out.write("            Method method = owner.getClass().getDeclaredMethod(\"phaseActors\", String.class, Object[].class);\n");
        out.write("            method.setAccessible(true);\n");
        out.write("            @SuppressWarnings(\"unchecked\")\n");
        out.write("            List<DUUIActor> actors = (List<DUUIActor>) method.invoke(owner, phaseMethod, args);\n");
        out.write("            return actors == null ? List.of() : actors;\n");
        out.write("        } catch (NoSuchMethodException ignored) {\n");
        out.write("            return List.of();\n");
        out.write("        } catch (ReflectiveOperationException e) {\n");
        out.write("            throw new IllegalStateException(e);\n");
        out.write("        }\n");
        out.write("    }\n\n");
    }

    private String classLiteral(TypeMirror type) {
        TypeKind kind = type.getKind();
        return switch (kind) {
            case BOOLEAN -> "boolean.class";
            case BYTE -> "byte.class";
            case SHORT -> "short.class";
            case INT -> "int.class";
            case LONG -> "long.class";
            case CHAR -> "char.class";
            case FLOAT -> "float.class";
            case DOUBLE -> "double.class";
            case ARRAY -> type + ".class";
            default -> processingEnv.getTypeUtils().erasure(type).toString() + ".class";
        };
    }

    private void error(Element element, String message) {
        messager.printMessage(Diagnostic.Kind.ERROR, message, element);
    }
}
