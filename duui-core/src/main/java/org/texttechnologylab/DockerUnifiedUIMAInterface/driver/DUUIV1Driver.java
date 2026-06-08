package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.InvalidXMLException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.CommunicationLayerException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.PipelineComponentException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.duui.pipeline.component.DUUIComponent;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Config;
import org.texttechnologylab.duui.protocol.v1.DUUIV1TelemetryConfig;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstract base class for all V1 container-based drivers.
 * Provides shared fields and common implementations that eliminate
 * duplication across DUUIDockerDriver, DUUIPodmanDriver, DUUISwarmDriver,
 * DUUIKubernetesDriver, and DUUIRemoteDriver.
 *
 * <p>NOT intended for DUUIUIMADriver — UIMA has no container lifecycle.</p>
 *
 * @author Alexander Leonhardt
 */
public abstract class DUUIV1Driver implements IDUUIDriverInterface {

    /** Shared HTTP client for container communication. */
    protected HttpClient _client;

    /** Lua context for component communication layer negotiation. */
    protected DUUILuaContext _luaContext;

    /** Container startup timeout in milliseconds (default 10000). */
    protected int _containerTimeout = 10000;

    /** Map of UUID → instantiated pipeline component. */
    protected HashMap<String, IDUUIInstantiatedPipelineComponent> _activeComponents;

    /** When true, serde phases use virtual threads; when false, platform threads. */
    protected boolean _useVirtualThreads = false;

    /** V1 HTTP protocol settings used by V2 driver instantiation. Always uses streaming (DUUIAsyncBodyHandler). */
    protected String _v1ContentType = "application/octet-stream";
    protected DUUIV1TelemetryConfig _v1Telemetry = DUUIV1TelemetryConfig.disabled();

    protected DUUIV1Driver() {
        _client = HttpClient.newHttpClient();
    }

    @Override
    public void setLuaContext(DUUILuaContext luaContext) {
        this._luaContext = luaContext;
    }
    public DUUIV1Driver withTimeout(int ms) {
        _containerTimeout = ms;
        return this;
    }

    public DUUIV1Driver withVirtualThreads(boolean useVirtualThreads) {
        this._useVirtualThreads = useVirtualThreads;
        return this;
    }

    public boolean isUseVirtualThreads() {
        return _useVirtualThreads;
    }

    public DUUIV1Driver withV1Transport(boolean streamingTransport, String contentType) {
        this._v1ContentType = contentType == null || contentType.isBlank()
                ? "application/octet-stream"
                : contentType;
        return this;
    }

    public DUUIV1Driver withV1Telemetry(DUUIV1TelemetryConfig telemetry) {
        this._v1Telemetry = telemetry == null ? DUUIV1TelemetryConfig.disabled() : telemetry;
        return this;
    }

    protected DUUIV1Config v1Config(
            int concurrency,
            String sourceView,
            String targetView,
            Map<String, String> parameters
    ) {
        return new DUUIV1Config(
                concurrency,
                sourceView,
                targetView,
                parameters,
                _v1Telemetry,
                _v1ContentType);
    }

    @Override
    public TypeSystemDescription get_typesystem(String uuid) throws InterruptedException, IOException, SAXException,
            CompressorException, ResourceInitializationException {
        IDUUIInstantiatedPipelineComponent comp = _activeComponents.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException(
                    "Invalid UUID, this component has not been instantiated by this driver");
        }
        return IDUUIInstantiatedPipelineComponent.getTypesystem(uuid, comp);
    }

    @Override
    public int initReaderComponent(String uuid, Path filePath) throws Exception {
        IDUUIInstantiatedPipelineComponent comp = _activeComponents.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException(
                    "Invalid UUID, this component has not been instantiated by this driver");
        }
        return IDUUIInstantiatedPipelineReaderComponent.initComponent(comp, filePath);
    }

    @Override
    public void printConcurrencyGraph(String uuid) {
        IDUUIInstantiatedPipelineComponent comp = _activeComponents.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException(
                    "Invalid UUID, this component has not been instantiated by this driver");
        }
        DUUIPipelineComponent pipelineComponent = comp.getPipelineComponent();
        String driverName = pipelineComponent.getDriverSimpleName() != null
                ? pipelineComponent.getDriverSimpleName()
                : getClass().getSimpleName();
        System.out.printf("[%s][%s]: Component found\n", driverName, uuid);
    }

    @Override
    public void shutdown() {
    }

    @Override
    public abstract boolean canAccept(DUUIPipelineComponent component) throws InvalidXMLException, IOException, SAXException;

    @Override
    public abstract String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification,
            AtomicBoolean shutdown) throws Exception;
/**
 * V2 instantiation stub.
 */
public DUUIComponent<JCas> instantiateV2(DUUIPipelineComponent component, JCas jc, boolean skipVerification,
        AtomicBoolean shutdown) throws Exception {
    throw new UnsupportedOperationException("V2 instantiation not yet implemented for " + getClass().getSimpleName());
}

    @Override
    public abstract void run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf, DUUIComposer composer)
            throws CASException, PipelineComponentException, CompressorException, IOException, InterruptedException,
            SAXException, CommunicationLayerException;

    @Override
    public abstract boolean destroy(String uuid);
}
