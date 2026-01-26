package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.InvalidParameterException;
import java.util.Optional;

import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIFallbackCommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIHttpRequestHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIHttpRequestHandler.Response;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.DUUIComponentDescriptors.IDUUIContainerComponentDescriptor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.base.AbstractPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.instances.DUUIInstanceDescriptors;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.instances.DUUIV1ContainerInstance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.instances.DUUIV1Instance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaCommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUIMsgPckCommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.model.AnnotatorDescriptor;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class DUUIV1Component
        extends AbstractPipelineComponent<DUUIInstanceDescriptors.IDUUIV1Instance, DUUIV1Component>
        implements IDUUIContainerComponentDescriptor<IDUUIPipelineComponent> {

    protected TypeSystemDescription typesystem;
    protected DUUIHttpRequestHandler handler;
    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public DUUIInstanceDescriptors.IDUUIV1InstanceOptions<?> createHttpInstance() {
        DUUIV1Instance instance = new DUUIV1Instance();
        pending.add(instance);
        return instance;
    }

    @Override
    public DUUIInstanceDescriptors.IDUUIContainerInstanceOptions<?> createContainerInstance() {
        DUUIV1ContainerInstance instance = new DUUIV1ContainerInstance();
        pending.add(instance);
        return instance;
    }

    @Override
    public DUUIV1Component finalization() throws Exception {
        if (pending.isEmpty()) {
            markFinalized();
            return this;
        }

        DUUIInstanceDescriptors.IDUUIV1Instance initInstance = (DUUIInstanceDescriptors.IDUUIV1Instance) pending.get(0);
        handler = handlerFor(initInstance.endpoint());

        this.annotatorDescriptor = requestAnnotatorDescriptor(handler);
        this.typesystem = requestTypesystem(handler);
        IDUUICommunicationLayer communicationLayer = requestCommunicationLayer(handler);

        for (var descriptor : pending) {
            DUUIInstanceDescriptors.IDUUIV1InstanceOptions<?> options = (DUUIInstanceDescriptors.IDUUIV1InstanceOptions<?>) descriptor;
            options.withCommunicationLayer(communicationLayer.copy());

            DUUIInstanceDescriptors.IDUUIV1Instance finalized = descriptor.finalization();
            pool.add(finalized);
        }

        pending.clear();
        markFinalized();
        return this;
    }

    @Override
    public TypeSystemDescription typesystem() {
        return typesystem;
    }

    @Override
    protected void processWithInstance(JCas jCas, DUUIInstanceDescriptors.IDUUIV1Instance instance) throws Exception {
        instance.communicationLayer().process(jCas, handler, parameters());
    }

    private DUUIHttpRequestHandler handlerFor(URI endpoint) {
        return new DUUIHttpRequestHandler(
                runtimeContext().httpClient(),
                endpoint.toString(),
                timeout().toSeconds()
        );
    }

    private TypeSystemDescription requestTypesystem(DUUIHttpRequestHandler handler) {
        Response res = handler.get(DUUIComposer.V1_COMPONENT_ENDPOINT_TYPESYSTEM);
        if (!res.ok()) {
            try {
                return TypeSystemDescriptionFactory.createTypeSystemDescription();
            } catch (ResourceInitializationException e) {
                throw new IllegalStateException("default typesystem creation failed", e);
            }
        }

        File tmp;
        try {
            tmp = File.createTempFile("duui-typesystem-", ".xml");
            tmp.deleteOnExit();
        } catch (IOException e) {
            throw new IllegalStateException("typesystem temp file create failed", e);
        }

        try (FileWriter out = new FileWriter(tmp, StandardCharsets.UTF_8)) {
            out.write(res.bodyUtf8());
        } catch (IOException e) {
            throw new IllegalStateException("typesystem temp file write failed", e);
        }

        try {
            return TypeSystemDescriptionFactory.createTypeSystemDescriptionFromPath(tmp.toURI().toString());
        } catch (Exception e) {
            throw new IllegalStateException("typesystem parse failed", e);
        }
    }

    private Optional<AnnotatorDescriptor> requestAnnotatorDescriptor(DUUIHttpRequestHandler handler) {
        
        DUUIHttpRequestHandler.Response resp =
                handler.get(DUUIComposer.V1_COMPONENT_ENDPOINT_DETAILS_INPUT_OUTPUT);

        if (resp.statusCode() != 200) {
            return Optional.empty();
        }

        try {
            return Optional.of(OBJECT_MAPPER.readValue(resp.body(), AnnotatorDescriptor.class));
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    private record DUUIBinV1Signal(String kind, String format, int version) {}
    private IDUUICommunicationLayer requestCommunicationLayer(DUUIHttpRequestHandler handler) throws Exception {
        DUUIHttpRequestHandler.Response resp =
                handler.get(DUUIComposer.V1_COMPONENT_ENDPOINT_COMMUNICATION_LAYER);

        if (resp.statusCode() == 404) {
            return new DUUIFallbackCommunicationLayer();
        }
        if (resp.statusCode() != 200) {
            throw new InvalidParameterException(
                    "Unexpected status for communication layer request: " + resp.statusCode()
            );
        }

        byte[] body = resp.body();
        String utf8 = resp.bodyUtf8().trim();

        if (utf8.startsWith("{")) {
            DUUIBinV1Signal sig = OBJECT_MAPPER.readValue(body, DUUIBinV1Signal.class);
            if ("duui-bin-v1".equals(sig.kind()) && "messagepack".equals(sig.format()) && sig.version() == 1) {
                AnnotatorDescriptor desc = this.annotatorDescriptor.orElse(null);
                if (desc == null) {
                    throw new IllegalStateException(
                            "duui-bin-v1 requires an AnnotatorDescriptor (" +
                                    DUUIComposer.V1_COMPONENT_ENDPOINT_DETAILS_INPUT_OUTPUT + ")"
                    );
                }
                return new DUUIMsgPckCommunicationLayer(desc, null);
            }
            throw new IllegalStateException("Unknown communication layer JSON: " + utf8);
        }

        String lua = resp.bodyAsString(Charset.defaultCharset());
        return new DUUILuaCommunicationLayer(lua, "requester", runtimeContext().luaContext());
    }
}
