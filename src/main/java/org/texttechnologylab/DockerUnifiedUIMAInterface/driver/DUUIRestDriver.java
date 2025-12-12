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
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.javatuples.Triplet;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIFallbackCommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.CommunicationLayerException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.PipelineComponentException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaCommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIEvent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUILogContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUILogger;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUILoggers;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.ClassScopedLogger;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.xml.sax.SAXException;

/**
 * Base class for REST-based drivers in DUUI. 
 * 
 * This class provides standard implementation for:
 * - {@code get_communication_layer()} (previously {@code responsiveAfterTime()})
 * - {@code get_typesystem()} 
 * - {@code initReaderComponent()}
 * - {@code run()}
 * 
 * and unifies similarities across REST-based drivers. It also defines an abstract {@code InstantiatedRestComponent} 
 * to simplify its definition in subclasses.
 * 
 */
public abstract class DUUIRestDriver<T extends DUUIRestDriver<T, Ic>, Ic extends IDUUIInstantiatedPipelineComponent> implements IDUUIDriverInterface {

    protected Duration _timeout = Duration.ofMillis(10_000);
    protected DUUILuaContext _luaContext;
    static int MAX_HTTP_RETRIES = 10;
    static Duration RETRY_DELAY = Duration.ofMillis(2000);
    static int BODY_PREVIEW_LIMIT = 500;

    private static final DUUILogger LOG = DUUILoggers.getLogger(DUUIRestDriver.class);

    @Override
    public DUUILogger logger() {
        return LOG;
    }

    @Override
    public void setLogger(DUUILogger delegate) {
        if (LOG instanceof ClassScopedLogger scoped) {
            scoped.setDelegate(delegate);
        }
    }
 
    /**
     * Helper to send a request with a built in retry-policy.
     * 
     * @param client HttpClient that sends the request
     * @param request HttpRequest that is sent
     * @param deadline Deadline after which results are ignored
     * @param timeout Timeout applied to every request
     * @param retryDelay Delay before resending failed request
     * @param max_http_retries Amount of retries on failed request. 
     * @param prefix Log prefix of the calling driver to enhance logging.
     * @return  HttpResponse
     * @throws Exception
     */
    public static HttpResponse<byte[]> sendWithRetries(
        HttpClient client,
        HttpRequest request,
        Instant deadline,
        Duration timeout,
        Duration retryDelay,
        int max_http_retries,
        String prefix) throws Exception {
        DUUILogger log = DUUILogContext.getLogger();
        int attempts = 0;
        while (Instant.now().isBefore(deadline)) {
            try {
                HttpResponse<byte[]> resp = client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).join();
                log.debug(
                        "%s HTTP attempt #%d to %s succeeded with status %d%n",
                        prefix,
                        attempts + 1,
                        request.uri(),
                        resp.statusCode()
                );
                return resp;
            } catch (Exception e) {
                attempts++;
                log.debug(
                        "%s HTTP connection error on try #%d to %s: %s (%s)%n",
                        prefix,
                        attempts,
                        request.uri(),
                        e.getClass().getSimpleName(),
                        e.getMessage()
                );
                if (e instanceof CompletionException ce && ce.getCause() != null) {
                    log.debug(
                            "%s Error while calling endpoint: %s (%s)%n %s %n",
                            prefix,
                            ce.getCause().getClass().getSimpleName(),
                            ce.getCause().getMessage(),
                            ExceptionUtils.getStackTrace(e)
                    );
                }
                if (attempts >= max_http_retries) {
                    throw new IOException(format("%s The endpoint (%s) could not provide a response after #%d tries.",
                        prefix, request.uri(), attempts
                    ), e);
                }

                try {
                    Thread.sleep(retryDelay.toMillis());
                } catch (InterruptedException ex) {
                    log.error(
                            "%s Sleep interrupted while waiting to retry endpoint call: %s\n",
                            prefix,
                            ex.toString()
                    );
                    throw new RuntimeException(ex);
                }
            }
        }
        throw new TimeoutException(
            format("%s The endpoint (%s) could not provide a response after %d milliseconds.",
            prefix, request.uri(), timeout.toMillis()
        ));
    }

    static HttpResponse<byte[]> sendWithRetries(
        HttpClient client,
        HttpRequest request,
        Instant deadline,
        Duration timeout,
        String prefix) throws Exception {
        return sendWithRetries(client, request, deadline, timeout, RETRY_DELAY, MAX_HTTP_RETRIES, prefix);
    }


    /**
     * Helper to cut of long respone bodies.
     * 
     * @param body
     * @param maxLen
     * @param charset
     * @return Truncated body
     */
    private static String preview(byte[] body, int maxLen, Charset charset) {
        if (body == null || body.length == 0 || maxLen <= 0) {
            return "";
        }
        String text = new String(body, charset);
        if (text.length() > maxLen) {
            return text.substring(0, maxLen) + "...";
        }
        return text;
    }

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
     * Set Timeout in milliseconds
     *
     * @param timeout_ms
     * @return
     */
    @SuppressWarnings("unchecked")
    public T withTimeout(int timeout_ms) {
        _timeout = Duration.ofMillis(timeout_ms);

        return (T) this;
    }

    abstract protected Map<String, Ic> getActiveComponents();
    
    /**
     * Get the instantiated component by its UUID
     * 
     * @param uuid
     * @return the instantiated component
     * 
     * @throws InvalidParameterException if the UUID is not valid
     */
    protected Ic getInstantiatedComponent(String uuid) {
        Ic component = getActiveComponents().get(uuid);
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
        
        logger().info("[%s][%s]: Maximum concurrency %d\n",
            this.getClass().getSimpleName(), 
            uuid, 
            component.getPipelineComponent().getScale()
        );
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
    
    /**
     * Creation of the communication layer based on the Driver and optional component verification.
     *
     * @param context Arguments for the communication layer request
     * @return the communication layer
     * @throws Exception
     */
    public IDUUICommunicationLayer get_communication_layer(DUUICommunicationLayerRequestContext context) throws Exception {
        String prefix = context.logPrefix();
        logger().info(
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
                            logger().info("%s Component lua communication layer, loading...%n", prefix);

                            logger().debug(
                                DUUIEvent.Context.lua(context.url() + DUUIComposer.V1_COMPONENT_ENDPOINT_COMMUNICATION_LAYER, body2),
                                "%s Received lua file body", prefix
                            );
                            
                            IDUUICommunicationLayer lua_com = new DUUILuaCommunicationLayer(body2, "requester", context.luaContext());
                            layer = lua_com;
                            logger().info("%s Component lua communication layer, loaded.%n", prefix);
                            communicationLayerRetrieved = true;
                            break OUTER;
                        } catch (Exception e) {
                            fatal_error = true;
                            Exception nr = new Exception(
                                format("%s Component provided a lua script which is not runnable.",
                                prefix
                            ), e);

                            logger().error("%s: %s", nr.getCause(), nr.getMessage());

                            throw nr;
                        }
                    }
                    case 404 -> {
                        logger().error("%s Component provided no own communication layer implementation using fallback.%n", prefix);
                        communicationLayerRetrieved = true;
                        break OUTER;
                    }
                    default -> {
                        int bodyLen = resp.body() != null ? resp.body().length : -1;
                        String preview = preview(resp.body(), BODY_PREVIEW_LIMIT, Charset.defaultCharset());
                        logger().error("%s Got HTTP status: %d (body %d bytes)%n",
                                prefix,
                                resp.statusCode(),
                                bodyLen
                        );
                        if (!preview.isEmpty()) {
                            logger().error("%s Response preview: %s%n", prefix, preview);
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
            logger().info("%s skipVerification=true, returning early.%n", prefix);
            return layer;
        }

        verifyComponentCompatibility(context, layer, prefix);

        return layer;
    }

    /**
     * Verify that the component can be used with the communication layer.
     *
     * @param context Arguments for the communication layer request
     * @param layer the communication layer
     * @param prefix log prefix
     * @throws Exception
     */
    private void verifyComponentCompatibility(
            DUUICommunicationLayerRequestContext context,
            IDUUICommunicationLayer layer,
            String prefix) throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            //TODO: Make this accept options to better check the instantiation!
            layer.serialize(context.jcas(), stream, null, "_InitialView");
        } catch (Exception e) {
            DUUILogger log = DUUILogContext.getLogger();
            log.error(
                    "%s The serialization step of the communication layer fails for implementing class %s%n%s",
                    prefix,
                    layer.getClass().getCanonicalName(),
                    ExceptionUtils.getStackTrace(e)
            );
            throw new Exception(
                format("%s The serialization step of the communication layer fails for implementing class %s", 
                prefix, layer.getClass().getCanonicalName()),
                e
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
            logger().error(
                    "%s Verification request to %s failed: %s (%s)%n",
                    prefix,
                    request.uri(),
                    e.getClass().getSimpleName(),
                    e.getMessage()
            );
            if (e instanceof CompletionException && ((CompletionException) e).getCause() != null) {
                Throwable cause = ((CompletionException) e).getCause();
                logger().error(
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
                String preview = preview(resp.body(), BODY_PREVIEW_LIMIT, StandardCharsets.UTF_8);
                logger().error(
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
            String preview = preview(resp.body(), BODY_PREVIEW_LIMIT, StandardCharsets.UTF_8);
            logger().error(
                    "%s Verification request to %s returned status %d (body %d bytes)%n",
                    prefix,
                    request.uri(),
                    resp.statusCode(),
                    bodyLen
            );
            if (!preview.isEmpty()) {
                logger().error("%s Verification response preview: %s%n", prefix, preview);
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

    /**
     * Base class for REST-based instantiated components. 
     * Provides general access methods to the {@code DUUIPipelineComponent}.
     */
    abstract static class IDUUIInstantiatedRestComponent<IC extends IDUUIInstantiatedRestComponent<IC>> implements IDUUIInstantiatedPipelineComponent {
        
        protected final DUUIPipelineComponent _component;

        protected String _uniqueComponentKey;

        protected final ConcurrentLinkedQueue<IDUUIUrlAccessible> _components;
        protected final ConcurrentHashMap<String, IDUUIUrlAccessible> _total_instances;

        private DUUILogger logger = DUUILogContext.getLogger();

        public IDUUIInstantiatedRestComponent(DUUIPipelineComponent component, String uuid) {

            _component = component;
            _uniqueComponentKey = uuid;

            _components = new ConcurrentLinkedQueue<>();
            _total_instances = new ConcurrentHashMap<>();
        }
        
        protected String prefix(int replicaIndex) {
            return String.format("[%s][%s][Replica %d/%d]", 
                getPipelineComponent().getDriverSimpleName(),
                _uniqueComponentKey.substring(0, 5) + "...",
                replicaIndex,
                getScale()
            );
        }

        @Override
        public DUUILogger logger() {
            return logger;
        }

        @Override
        public void setLogger(DUUILogger logger) {
            this.logger = (logger != null) ? logger : DUUILogContext.getLogger();
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
