package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.testcontainers.shaded.org.apache.commons.lang3.exception.ExceptionUtils;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUIDocument;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIDriverInterface;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIInstantiatedPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIUrlAccessible;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIDocumentReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIContext.*;

public final class DUUIContexts {
    private static final @Nonnull Payload DEFAULT_PAYLOAD = new Payload(DUUIStatus.UNKNOWN, "", PayloadKind.NONE, Thread.currentThread().getName());
    private static final DUUIContext DEFAULT_CONTEXT = new DefaultContext(DEFAULT_PAYLOAD);

    private DUUIContexts() {
    }

    public static DUUIContext defaultContext() {
        return DEFAULT_CONTEXT;
    }

    public static void setComposerContext(DUUIContext context) {
        if (context instanceof ComposerContext composerContext) {
            DUUIContextTL.COMPOSER.set(composerContext);
        } else {
            DUUIContextTL.COMPOSER.remove();
        }
    }

    public static void setDocumentContext(DUUIContext context) {
        if (context instanceof DocumentProcessContext documentContext) {
            DUUIContextTL.DOCUMENT.set(documentContext);
        } else {
            DUUIContextTL.DOCUMENT.remove();
        }
    }

    public static DUUIContext status(DUUIStatus status) {
        DUUIStatus effectiveStatus = status != null ? status : DUUIStatus.UNKNOWN;
        DUUIContext base = DUUILogContext.getContext();
        if (base == null) {
            return new DefaultContext(new Payload(effectiveStatus, "", PayloadKind.NONE, Thread.currentThread().getName()));
        }
        return base.updateStatus(effectiveStatus);
    }

    public static ContextBuilder current() {
        return new ContextBuilder(DUUILogContext.getContext());
    }

    public static DUUIContext lua(String content) {
        return DUUILogContext
            .getContext()
            .updatePayload(
                new Payload(
                    DUUILogContext.getContext().status(),
                    StringUtils.defaultString(content),
                    PayloadKind.LUA,
                    Thread.currentThread().getName()
            ));
    }

    public static DUUIContext typesystem(String content) {
        return DUUILogContext
            .getContext()
            .updatePayload(
                new Payload(
                    DUUILogContext.getContext().status(),
                    StringUtils.defaultString(content),
                    PayloadKind.TYPESYSTEM,
                    Thread.currentThread().getName()
            ));
    }

    public static DUUIContext response(String content) {
        return DUUILogContext
            .getContext()
            .updatePayload(
                new Payload(
                    DUUILogContext.getContext().status(),
                    StringUtils.defaultString(content),
                    PayloadKind.RESPONSE,
                    Thread.currentThread().getName()
            ));
    }

    public static DUUIContext exception(Throwable e) {
        return DUUILogContext
            .getContext()
            .updatePayload(
                new Payload(
                    DUUILogContext.getContext().status(),
                    ExceptionUtils.getStackTrace(e),
                    PayloadKind.STACKTRACE,
                    Thread.currentThread().getName()
            ));
    }

    public static DUUIContext metric(Duration duration) {
        return DUUILogContext
            .getContext()
            .updatePayload(
                new Payload(
                    DUUILogContext.getContext().status(),
                    String.valueOf(duration.toMillis()),
                    PayloadKind.METRIC_MILLIS,
                    Thread.currentThread().getName()
            ));
    }

    public static ContextBuilder doc(DUUIDocument doc) {
        return new ContextBuilder(ContextScope.DOCUMENT).doc(doc);
    }

    public static ContextBuilder reader(DUUIDocumentReader reader) {
        return new ContextBuilder(ContextScope.READER).reader(reader);
    }

    public static ContextBuilder reader(DUUIDocumentReader reader, DUUIDocument doc) {
        return new ContextBuilder(ContextScope.READER).reader(reader).doc(doc);
    }

    public static ContextBuilder component(IDUUIInstantiatedPipelineComponent comp) {
        return new ContextBuilder(ContextScope.COMPONENT).component(comp);
    }

    public static ContextBuilder component(IDUUIInstantiatedPipelineComponent comp, IDUUIUrlAccessible instance) {
        return new ContextBuilder(ContextScope.COMPONENT).component(comp).instance(instance);
    }

    public static ContextBuilder driver(IDUUIDriverInterface driver) {
        return new ContextBuilder(ContextScope.DRIVER).driver(driver);
    }

    public static ContextBuilder composer(DUUIComposer composer, String runkey) {
        return new ContextBuilder(ContextScope.COMPOSER).composer(composer, runkey);
    }

    public static ContextBuilder worker(String name, AtomicInteger active) {
        return new ContextBuilder(ContextScope.WORKER).worker(name, active);
    }

    @Nonnull
    public static DUUIContext updateStatus(DUUIContext context, @Nonnull DUUIStatus status) {
        return updateStatus(context, new DUUIContext.Payload(status, "", PayloadKind.NONE, Thread.currentThread().getName()));
    }

    @Nonnull
    public static DUUIContext updateStatus(DUUIContext context, Payload newPayload) {
        if (context == null) {
            return new DefaultContext(new Payload(newPayload.status(), "", PayloadKind.NONE, Thread.currentThread().getName()));
        }

        Payload payload = new Payload(newPayload.status(), newPayload.content(),newPayload.kind(), Thread.currentThread().getName());

        if (context instanceof DocumentComponentProcessContext ctx) {
            return new DocumentComponentProcessContext(ctx.document(), ctx.component(), payload);
        }
        if (context instanceof DocumentProcessContext ctx) {
            return new DocumentProcessContext(ctx.document(), ctx.composer(), payload);
        }
        if (context instanceof InstantiatedComponentContext ctx) {
            return new InstantiatedComponentContext(ctx.component(), ctx.instanceId(), ctx.endpoint(), payload);
        }
        if (context instanceof ComponentContext ctx) {
            return new ComponentContext(ctx.componentId(), ctx.componentName(), ctx.driverName(), ctx.instanceIds(), payload);
        }
        if (context instanceof ReaderContext ctx) {
            return new ReaderContext(ctx.reader(), payload);
        }
        if (context instanceof WorkerContext ctx) {
            return new WorkerContext(ctx.composer(), ctx.name(), ctx.activeWorkers(), payload);
        }
        if (context instanceof ComposerContext ctx) {
            return new ComposerContext(ctx.runKey(), ctx.pipelineStatus(), ctx.progressAtomic(), ctx.total(), payload);
        }
        if (context instanceof DocumentContext ctx) {
            return new DocumentContext(ctx.document(), payload);
        }
        if (context instanceof DriverContext ctx) {
            return new DriverContext(ctx.driverName(), payload);
        }
        if (context instanceof DefaultContext) {
            return new DefaultContext(payload);
        }

        return new DefaultContext(payload);
    }

    public static Document toDocument(DUUIContext context) {
        Document out = new Document("type", context.getClass().getSimpleName());

        switch (context) {
            case DefaultContext _ctx -> {
                return out;
            }
            case ComposerContext ctx -> {
                out.append("runKey", ctx.runKey());
                out.append("progress", ctx.progressAtomic().get());
                out.append("total", ctx.total());
                return out;
            }
            case WorkerContext ctx -> {
                out.append("name", ctx.name());
                out.append("activeWorkers", ctx.activeWorkers().get());
                out.append("composer", toDocument(ctx.composer()));
                return out;
            }
            case DocumentContext ctx -> {
                DUUIDocument doc = ctx.document();
                out.append("document", toDocument(doc));
                return out;
            }
            case DocumentProcessContext ctx -> {
                out.append("document", toDocument(ctx.document()));
                out.append("composer", toDocument(ctx.composer()));
                return out;
            }
            case ComponentContext ctx -> {
                out.append("componentId", ctx.componentId());
                out.append("componentName", ctx.componentName());
                out.append("driverName", ctx.driverName());
                out.append("instanceIds", ctx.instanceIds());
                return out;
            }
            case InstantiatedComponentContext ctx -> {
                out.append("instanceId", ctx.instanceId());
                out.append("endpoint", ctx.endpoint());
                out.append("component", toDocument(ctx.component()));
                return out;
            }
            case DocumentComponentProcessContext ctx -> {
                out.append("document", toDocument(ctx.document()));
                out.append("component", toDocument(ctx.component()));
                return out;
            }
            case ReaderContext ctx -> {
                out.append("reader", ctx.reader().getClass().getSimpleName());
                return out;
            }
            case ReaderDocumentContext ctx -> {
                out.append("reader", toDocument(ctx.reader()));
                out.append("document", toDocument(ctx.document()));
                return out;
            }
            case DriverContext ctx -> {
                out.append("driverName", ctx.driverName());
                return out;
            }
            default -> {
                return out;
            }
        }
    }

    public static Document toDocument(Payload payload) {
        return new Document("kind", payload.kind().name())
            .append("content", payload.content());
    }

    static Document toDocument(DUUIDocument doc) {
        return doc.getState().toDocument();
    }

    public enum ContextScope {
        DOCUMENT,
        COMPOSER,
        DRIVER,
        COMPONENT,
        READER,
        WORKER,
        DEFAULT,
        UPDATEPAYLOAD
    }

    public static final class ContextBuilder {
        private final ContextScope scope;
        private DUUIDocument doc;
        private DUUIDocumentReader reader;
        private IDUUIInstantiatedPipelineComponent component;
        private IDUUIDriverInterface driver;
        private DUUIComposer composer;
        private String instanceId;
        private String endpoint;
        private String workerName;
        private AtomicInteger activeWorkers;
        @Nonnull
        private String payloadContent;
        @Nonnull
        private PayloadKind payloadKind = PayloadKind.NONE;

        private DUUIContext updateContext;
        private String runKey; 

        private ContextBuilder(DUUIContext context) {
            this.scope = ContextScope.UPDATEPAYLOAD;
            this.updateContext = context;        
        } 

        private ContextBuilder(ContextScope scope) {
            this.scope = scope;
        }

        public ContextBuilder doc(DUUIDocument doc) {
            this.doc = doc;
            return this;
        }

        public ContextBuilder reader(DUUIDocumentReader reader) {
            this.reader = reader;
            return this;
        }

        public ContextBuilder component(IDUUIInstantiatedPipelineComponent comp) {
            this.component = comp;
            return this;
        }

        public ContextBuilder instance(IDUUIUrlAccessible instance) {
            this.instanceId = instance.getUniqueInstanceKey();
            this.endpoint = instance.generateURL();
            return this;
        }

        public ContextBuilder endpoint(String url) {
            this.endpoint = url;
            return this;
        }

        public ContextBuilder driver(IDUUIDriverInterface driver) {
            this.driver = driver;
            return this;
        }

        public ContextBuilder composer(DUUIComposer composer, String runKey) {
            this.runKey = runKey;
            this.composer = composer;
            return this;
        }

        public ContextBuilder worker(String name, AtomicInteger active) {
            this.workerName = name;
            this.activeWorkers = active;
            return this;
        }

        public ContextBuilder typesystem(@Nonnull String content) {
            return payload(content, PayloadKind.TYPESYSTEM);
        }

        public ContextBuilder lua(@Nonnull String content) {
            return payload(content, PayloadKind.LUA);
        }

        public ContextBuilder metric(@Nonnull Duration duration) {
            return payload(String.valueOf(duration.toMillis()), PayloadKind.METRIC_MILLIS);
        }

        public ContextBuilder exception(Throwable content) {
            return exception(content == null ? "" : ExceptionUtils.getStackTrace(content));
        }

        public ContextBuilder exception(@Nonnull String content) {
            return payload(content, PayloadKind.STACKTRACE);
        }

        public ContextBuilder response(@Nonnull String content) {
            return payload(content, PayloadKind.RESPONSE);
        }

        public ContextBuilder logs(@Nonnull String content) {
            return payload(content, PayloadKind.LOGS);
        }

        public ContextBuilder payload(@Nonnull String content, PayloadKind kind) {
            this.payloadContent = content;
            this.payloadKind = kind != null ? kind : PayloadKind.GENERIC;
            return this;
        }

        public DUUIContext status(@Nonnull DUUIStatus status) {
            String content = StringUtils.defaultString(payloadContent);
            Payload payload = new Payload(status, content, payloadKind, Thread.currentThread().getName());

            ComposerContext tlComposer = DUUIContextTL.COMPOSER.get();
            DocumentProcessContext tlDocument = DUUIContextTL.DOCUMENT.get();

            switch (scope) {
                case UPDATEPAYLOAD -> {
                    return this.updateContext.updatePayload(payload);
                }
                case DOCUMENT -> {
                    this.doc.setStatus(payload.status());

                    DocumentContext docCtx = new DocumentContext(doc, payload);
                    if (tlComposer != null) {
                        return new DocumentProcessContext(docCtx, tlComposer, payload);
                    }
                    return docCtx;
                }
                case COMPONENT -> {
                    java.util.List<String> instanceIds = component.getInstanceIdentifiers();
                    if (instanceIds == null) {
                        instanceIds = Collections.emptyList();
                    }
                    ComponentContext compCtx = new ComponentContext(
                        component.getUniqueComponentKey(),
                        component.getName(),
                        component.getPipelineComponent().getDriverSimpleName(),
                        instanceIds,
                        payload
                    );
                    if (instanceId != null) {
                        
                        InstantiatedComponentContext inst = new InstantiatedComponentContext(
                            compCtx,
                            StringUtils.defaultString(instanceId),
                            StringUtils.defaultString(endpoint),
                            payload
                        );
                        if (tlDocument != null) {
                            return new DocumentComponentProcessContext(tlDocument, inst, payload);
                        }
                        return inst;
                    }
                    return compCtx;
                }
                case READER -> {
                    ReaderContext readerCtx = new ReaderContext(reader, payload);

                    if (payload.status() == DUUIStatus.SETUP && tlComposer != null) {
                        DUUIContext updated = tlComposer.updateStatus(payload.status());
                        if (updated instanceof ComposerContext updatedComposer) {
                            DUUIContextTL.COMPOSER.set(updatedComposer);
                        }
                    }

                    if (this.doc != null) {
                        DocumentContext docCtx = new DocumentContext(doc, payload);
                        return new ReaderDocumentContext(readerCtx, docCtx, payload);
                    }

                    return readerCtx;
                }
                case DRIVER -> {
                    return new DriverContext(driver.getClass().getSimpleName(), payload);
                }
                case COMPOSER -> {
                    if (composer != null) {
                        Map<String, DUUIStatus> statusMap = composer.getPipelineStatusEnum();
                        if (statusMap == null) {
                            statusMap = Collections.emptyMap();
                        }
                        AtomicInteger progressAtomic = composer.getProgressAtomic();
                        if (progressAtomic == null) {
                            progressAtomic = new AtomicInteger(0);
                        }
                        return new ComposerContext(
                            StringUtils.defaultString(this.runKey),
                            statusMap,
                            progressAtomic,
                            composer.getDocumentCount(),
                            payload
                        );
                    }
                    return tlComposer != null ? tlComposer : new DefaultContext(payload);
                }
                case WORKER -> {
                    AtomicInteger workers = activeWorkers != null ? activeWorkers : new AtomicInteger(0);
                    return tlComposer != null
                        ? new WorkerContext(tlComposer, StringUtils.defaultString(workerName), workers, payload)
                        : new DefaultContext(payload);
                }
                default -> {
                    return new DefaultContext(payload);
                }
            }
        }
    }
}
