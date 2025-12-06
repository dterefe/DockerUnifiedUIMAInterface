package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import static java.lang.String.format;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.javatuples.Triplet;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIFallbackCommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import static org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIRestDriver.sendWithRetries;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.CommunicationLayerException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.PipelineComponentException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaCommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.xml.sax.SAXException;

public abstract class DUUIRestDriver<T extends DUUIRestDriver<T, IC>, IC extends IDUUIInstantiatedPipelineComponent> implements IDUUIRestDriver {

    protected Duration _timeout = Duration.ofMillis(10_000);
    protected DUUILuaContext _luaContext;

    /**
     * Set Lua-Context
     *
     * @param luaContext
     */
    @Override
    public void setLuaContext(DUUILuaContext luaContext) {
        _luaContext = luaContext;
    }

    /**
     * Set Timeout
     *
     * @param timeout_ms
     * @return
     */
    @SuppressWarnings("unchecked")
    public T withTimeout(int timeout_ms) {
        _timeout = Duration.ofMillis(timeout_ms);

        return (T) this;
    }

    abstract protected Map<String, IC> getActiveComponents();
    
    protected IC getInstantiatedComponent(String uuid) {
        IC component = getActiveComponents().get(uuid);
        if (component == null) {
            throw new InvalidParameterException("[" + this.getClass().getSimpleName() + "] Invalid UUID, this component has not been instantiated by the driver");
        }

        return component;
    }

    /**
     * Show the maximum parallelism
     *
     * @param uuid
     */
    @Override
    public void printConcurrencyGraph(String uuid) {
        IDUUIInstantiatedPipelineComponent component = getInstantiatedComponent(uuid);
        
        System.out.printf("[DUUIDockerDriver][%s]: Maximum concurrency %d\n", uuid, component.getPipelineComponent().getScale());
    }

    protected record DUUICommunicationLayerRequestContext(
            String url,
            JCas jcas,
            Duration timeout,
            HttpClient client,
            DUUILuaContext luaContext,
            boolean skipVerification,
            String logPrefix) {

        protected DUUICommunicationLayerRequestContext {
            Objects.requireNonNull(url, "url");
            Objects.requireNonNull(jcas, "jcas");
            Objects.requireNonNull(timeout, "timeout");
            Objects.requireNonNull(client, "client");
            Objects.requireNonNull(logPrefix, "logPrefix");
        }
    }
    
    public static IDUUICommunicationLayer get_communication_layer(DUUICommunicationLayerRequestContext context) throws Exception {
        String prefix = context.logPrefix();
        System.out.printf(
                "%s Initializing communication layer for %s (timeout=%d ms, skipVerification=%b)%n",
                prefix,
                context.url(),
                context.timeout().toMillis(),
                context.skipVerification()
        );
        Instant start = Instant.now();
        Instant deadline = start.plus(context.timeout());
        IDUUICommunicationLayer layer = new DUUIFallbackCommunicationLayer();
        boolean fatal_error = false;

        boolean communicationLayerRetrieved = false;
        int iError = 0;
        OUTER:
        while (Instant.now().isBefore(deadline)) {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(context.url() + DUUIComposer.V1_COMPONENT_ENDPOINT_COMMUNICATION_LAYER))
                    .version(HttpClient.Version.HTTP_1_1)
                    .timeout(context.timeout())
                    .GET()
                    .build();
            try {
                HttpResponse<byte[]> resp = sendWithRetries(context.client(), request, deadline, context.timeout(), prefix);

                switch (resp.statusCode()) {
                    case 200 -> {
                        String body2 = new String(resp.body(), Charset.defaultCharset());
                        try {
                            System.out.printf("%s Component lua communication layer, loading...%n", prefix);
                            IDUUICommunicationLayer lua_com = new DUUILuaCommunicationLayer(body2, "requester", context.luaContext());
                            layer = lua_com;
                            System.out.printf("%s Component lua communication layer, loaded.%n", prefix);
                            communicationLayerRetrieved = true;
                            break OUTER;
                        } catch (Exception e) {
                            fatal_error = true;
                            throw new Exception(
                                format("%s Component provided a lua script which is not runnable.",
                                prefix
                            ), e);
                        }
                    }
                    case 404 -> {
                        System.out.printf("%s Component provided no own communication layer implementation using fallback.%n", prefix);
                        communicationLayerRetrieved = true;
                        break OUTER;
                    }
                    default -> {
                        int bodyLen = resp.body() != null ? resp.body().length : -1;
                        String preview = IDUUIRestDriver.preview(resp.body(), BODY_PREVIEW_LIMIT, Charset.defaultCharset());
                        System.err.printf("%s Got HTTP status: %d (body %d bytes)%n",
                                prefix,
                                resp.statusCode(),
                                bodyLen
                        );
                        if (!preview.isEmpty()) {
                            System.err.printf("%s Response preview: %s%n", prefix, preview);
                        }
                    }
                }
                Duration timeElapsed = Duration.between(start, Instant.now());
                if (timeElapsed.compareTo(context.timeout()) > 0) {
                    throw new TimeoutException(
                        format("%s The endpoint (%s) could not provide a response in %d milliseconds", 
                        prefix, context.url(),context.timeout().toMillis())
                    );
                }
            } catch (Exception e) {

                if (fatal_error) {
                    throw e;
                } else {
                    Thread.sleep(RETRY_DELAY.toMillis());
                    iError++;
                }

                if (iError > MAX_HTTP_RETRIES) {
                    throw e;
                }
            }
        }
        if (!communicationLayerRetrieved) {
            throw new TimeoutException(format("The Container did not provide one succesful answer in %d milliseconds", context.timeout().toMillis()));
        }
        if (context.skipVerification()) {
            System.out.printf("%s skipVerification=true, returning early.%n", prefix);
            return layer;
        }

        verifyComponentCompatibility(context, layer, prefix);

        return layer;
    }

    private static void verifyComponentCompatibility(
            DUUICommunicationLayerRequestContext context,
            IDUUICommunicationLayer layer,
            String prefix) throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            //TODO: Make this accept options to better check the instantiation!
            layer.serialize(context.jcas(), stream, null, "_InitialView");
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(
                format("%s The serialization step of the communication layer fails for implementing class %s", 
                prefix, layer.getClass().getCanonicalName())
            );
        }

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(context.url() + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS))
                .version(HttpClient.Version.HTTP_1_1)
                .timeout(context.timeout())
                .POST(HttpRequest.BodyPublishers.ofByteArray(stream.toByteArray()))
                .build();

        HttpResponse<byte[]> resp;
        try {
            resp = context.client().sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).join();
        } catch (Exception e) {
            System.err.printf(
                    "%s Verification request to %s failed: %s (%s)%n",
                    prefix,
                    request.uri(),
                    e.getClass().getSimpleName(),
                    e.getMessage()
            );
            if (e instanceof CompletionException && ((CompletionException) e).getCause() != null) {
                Throwable cause = ((CompletionException) e).getCause();
                System.err.printf(
                        "%s Verification CompletionException cause: %s (%s)%n",
                        prefix,
                        cause.getClass().getSimpleName(),
                        cause.getMessage()
                );
            }
            throw new Exception("Failed to send verification request to " + request.uri(), e);
        }

        if (resp.statusCode() == 200) {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(resp.body());
            try {
                layer.deserialize(context.jcas(), inputStream, "_InitialView");
            } catch (Exception e) {
                String preview = IDUUIRestDriver.preview(resp.body(), BODY_PREVIEW_LIMIT, StandardCharsets.UTF_8);
                System.err.printf(
                        "%s Deserialization failed for %s; response preview: %s%n",
                        prefix,
                        request.uri(),
                        preview
                );
                throw e;
            }
            return;
        } else {
            int bodyLen = resp.body() != null ? resp.body().length : -1;
            String preview = IDUUIRestDriver.preview(resp.body(), BODY_PREVIEW_LIMIT, StandardCharsets.UTF_8);
            System.err.printf(
                    "%s Verification request to %s returned status %d (body %d bytes)%n",
                    prefix,
                    request.uri(),
                    resp.statusCode(),
                    bodyLen
            );
            if (!preview.isEmpty()) {
                System.err.printf("%s Verification response preview: %s%n", prefix, preview);
            }
            throw new Exception(format("The container returned response with code %d for %s", resp.statusCode(), request.uri()));
        }
    } 
    
    /**
     * Return the TypeSystem used by the given Component
     *
     * @param uuid
     * @return
     * @throws InterruptedException
     * @throws IOException
     * @throws SAXException
     * @throws CompressorException
     * @throws ResourceInitializationException
     */
    @Override
    public TypeSystemDescription get_typesystem(String uuid) throws InterruptedException, IOException, SAXException, CompressorException, ResourceInitializationException {
        IDUUIInstantiatedPipelineComponent component = getInstantiatedComponent(uuid);

        return IDUUIInstantiatedPipelineComponent.getTypesystem(uuid, component);
    }
    
    /**
     * init reader component
     * @param uuid
     * @param filePath
     * @return
     */
    @Override
    public int initReaderComponent(String uuid, Path filePath) {
        IDUUIInstantiatedPipelineComponent component = getInstantiatedComponent(uuid);

        return IDUUIInstantiatedPipelineReaderComponent.initComponent(component, filePath);
    }
    
    /**
     * Execute a component in the driver
     *
     * @param uuid
     * @param aCas
     * @param perf
     * @param composer
     * @throws CASException
     * @throws PipelineComponentException
     */
    @Override
    public void run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf, DUUIComposer composer) 
        throws 
            CASException, 
            PipelineComponentException, 
            CompressorException, 
            IOException, 
            InterruptedException, 
            SAXException, 
            CommunicationLayerException {
        IDUUIInstantiatedPipelineComponent component = getInstantiatedComponent(uuid);

        IDUUIInstantiatedPipelineComponent.process(aCas, component, perf);
    }

    abstract static class IDUUIInstantiatedRestComponent<InstantiatedComponent extends IDUUIInstantiatedRestComponent<InstantiatedComponent>> implements IDUUIInstantiatedPipelineComponent {
        
        protected final DUUIPipelineComponent _component;

        protected String _uniqueComponentKey;
        protected String name;
        protected String description;

        protected int _scale;
        protected int _workers;
        protected Map<String, String> _parameters;

        protected String _sourceView;
        protected String _targetView;

        protected final ConcurrentLinkedQueue<IDUUIUrlAccessible> _components;
        protected final ConcurrentHashMap<String, IDUUIUrlAccessible> _total_instances;

        public IDUUIInstantiatedRestComponent(DUUIPipelineComponent component, String uuid) {

            _component = component;
            _uniqueComponentKey = uuid;

            name = component.getName();
            description = component.getDescription();

            _parameters = component.getParameters();
            _scale = component.getScale(1);
            _workers = component.getWorkers(1);

            _components = new ConcurrentLinkedQueue<>();
            _total_instances = new ConcurrentHashMap<>();
            
        }

        public String getName() {
            return name;
        }

        public String getDescription() {
            return description;
        }

        @Override
        public String getUniqueComponentKey() {
            return _uniqueComponentKey;
        }

        @Override
        public DUUIPipelineComponent getPipelineComponent() {
            return _component;
        }

        @Override
        public Triplet<IDUUIUrlAccessible, Long, Long> getComponent() {
            long mutexStart = System.nanoTime();
            IDUUIUrlAccessible inst = _components.poll();
            while (inst == null) {
                inst = _components.poll();
            }
            long mutexEnd = System.nanoTime();
            return Triplet.with(inst, mutexStart, mutexEnd);
        }

        @Override
        public void addComponent(IDUUIUrlAccessible item) {
            _total_instances.put(item.getUniqueInstanceKey(), item);
            _components.add(item);
        }

        protected Collection<IDUUIUrlAccessible> getTotalInstances() {
            return _total_instances.values();
        }

        protected ConcurrentLinkedQueue<IDUUIUrlAccessible> getInstances() {
            return _components;
        }

        public int getScale() {
            return _scale;
        }

        public int getWorkers() {
            return _workers;
        }

        @Override
        public Map<String, String> getParameters() {
            return _parameters;
        }

        @Override
        public String getSourceView() {
            return _sourceView;
        }

        @Override
        public String getTargetView() {
            return _targetView;
        }

        @Deprecated
        public boolean isWebsocket() {
            return false;
        }

        @Deprecated
        public int getWebsocketElements() {
            return 0;
        }

    }

}
