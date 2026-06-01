package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;


import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Ports;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.TypeSystemUtil;
import org.javatuples.Triplet;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIFallbackCommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.DUUIWebsocketAlt;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.CommunicationLayerException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.ImagePullException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.PipelineComponentException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaCommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.texttechnologylab.duui.clients.docker.DUUIDockerClient;
import org.texttechnologylab.duui.clients.http.DUUIHttpEndpoint;
import org.texttechnologylab.duui.clients.http.IDUUIEndpoint;
import org.texttechnologylab.duui.pipeline.component.DUUIComponent;
import org.texttechnologylab.duui.pipeline.component.DUUINode;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Annotator;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Config;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.getLocalhost;

/**
 * Interface for all drivers
 *
 * @author Alexander Leonhardt
 */
interface ResponsiveMessageCallback {
    public void operation(String message);
}

/**
 * Driver for the use of Docker
 *
 * @author Alexander Leonhardt
 */
public class DUUIDockerDriver extends DUUIV1Driver {
    private DUUIDockerClient _dockerClient;
    private IDUUIConnectionHandler _wsclient;

    private final static Logger LOGGER = Logger.getLogger(DUUIComposer.class.getName());

    public DUUIDockerDriver() throws IOException, UIMAException, SAXException {
        super();
        _dockerClient = new DUUIDockerClient();

        JCas _basic = JCasFactory.createJCas();
        _basic.setDocumentLanguage("en");
        _basic.setDocumentText("Hello World!");
        _containerTimeout = 10000;

        TypeSystemDescription desc = TypeSystemUtil.typeSystem2TypeSystemDescription(_basic.getTypeSystem());
        StringWriter wr = new StringWriter();
        desc.toXML(wr);
        _activeComponents = new HashMap<>();
    }

    /**
     * Constructor with built-in timeout
     *
     * @param timeout
     * @throws IOException
     * @throws UIMAException
     * @throws SAXException
     */
    public DUUIDockerDriver(int timeout) throws IOException, UIMAException, SAXException {
        super();
        _dockerClient = new DUUIDockerClient();
        _client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(timeout)).build();

        _containerTimeout = timeout;

        _activeComponents = new HashMap<>();
    }

    /**
     * Creation of the communication layer based on the Driver
     *
     * @param url
     * @param jc
     * @param timeout_ms
     * @param client
     * @param printfunc
     * @param context
     * @param skipVerification
     * @return
     * @throws Exception
     */
    public static IDUUICommunicationLayer responsiveAfterTime(
            String url,
            JCas jc,
            int timeout_ms,
            HttpClient client,
            ResponsiveMessageCallback printfunc,
            DUUILuaContext context,
            boolean skipVerification) throws Exception {
        System.out.printf(
                "[DUUIDockerDriver] Initializing communication layer for %s (timeout=%d ms, skipVerification=%b)%n",
                url,
                timeout_ms,
                skipVerification
        );
        long start = System.currentTimeMillis();
        long deadline = start + Math.max(1, timeout_ms);
        long retrySleepMs = Math.min(2000L, Math.max(250L, timeout_ms / 20L));
        IDUUICommunicationLayer layer = new DUUIFallbackCommunicationLayer();  // Hier wird layer zum ersten mal erstellt.
        boolean fatal_error = false;

        int iError = 0;
        OUTER:
        while (true) {
            HttpRequest request = null;
            try {
                request = HttpRequest.newBuilder()
                        .uri(URI.create(url + DUUIComposer.V1_COMPONENT_ENDPOINT_COMMUNICATION_LAYER))
                        .version(HttpClient.Version.HTTP_1_1)
                        .timeout(Duration.ofMillis(Math.max(1000L, Math.min(timeout_ms, 10_000L))))
                        .GET()
                        .build();
            } catch (Exception e) {
                throw new IllegalArgumentException("The Container did not provide a valid URL for communication layer retrieval.", e);
            }
            try {
                HttpResponse<byte[]> resp = null;
                boolean connectionError = true;
                int iCount = 0;
                while (connectionError && iCount < 10) {

                    try {
                        // Das hier geht beim KubernetesDriver nicht
                        resp = client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).join();
                        System.out.printf(
                                "[DUUIDockerDriver] HTTP attempt #%d to %s succeeded with status %d%n",
                                iCount + 1,
                                request.uri(),
                                resp.statusCode()
                        );
                        connectionError = false;
                    } catch (Exception e) {
                        System.err.printf(
                                "[DUUIDockerDriver] HTTP connection error on try #%d to %s: %s (%s)%n",
                                iCount + 1,
                                request.uri(),
                                e.getClass().getSimpleName(),
                                e.getMessage()
                        );
                        if (e instanceof java.net.ConnectException) {
                            System.err.printf(
                                    "[DUUIDockerDriver] ConnectException details: host=%s, port=%d%n",
                                    request.uri().getHost(),
                                    request.uri().getPort()
                            );
                            sleepUntilDeadline(retrySleepMs, deadline);
                            iCount++;
                        } else if (e instanceof CompletionException ce) {
                            if (ce.getCause() != null) {
                                System.err.printf(
                                        "[DUUIDockerDriver] CompletionException cause: %s (%s)%n",
                                        ce.getCause().getClass().getSimpleName(),
                                        ce.getCause().getMessage()
                                );
                            }
                            sleepUntilDeadline(retrySleepMs, deadline);
                            iCount++;
                        } else {
                            throw new Exception("The Container did not provide a valid answer for communication layer retrieval.", e);
                        }
                    }
                }
                if (resp == null) {
                    throw new Exception("No HTTP response after 10 tries!");
                }
                switch (resp.statusCode()) {
                    case 200 -> {
                        String body2 = new String(resp.body(), Charset.defaultCharset());
                        try {
                            printfunc.operation("Component lua communication layer, loading...");
                            IDUUICommunicationLayer lua_com = new DUUILuaCommunicationLayer(body2, "requester", context);
                            layer = lua_com;
                            printfunc.operation("Component lua communication layer, loaded.");
                            break OUTER;
                        }catch (Exception e) {
                            fatal_error = true;
                            throw new Exception("Component provided a lua script which is not runnable.", e);
                        }
                    }
                    case 404 -> {
                        printfunc.operation("Component provided no own communication layer implementation using fallback.");
                        break OUTER;
                    }
                    default -> {
                        int bodyLen = resp.body() != null ? resp.body().length : -1;
                        System.err.printf("[DUUIDockerDriver] Got HTTP status: %d (body %d bytes)%n",
                                resp.statusCode(),
                                bodyLen
                        );  if (resp.body() != null && bodyLen > 0) {
                            String preview = new String(resp.body(), Charset.defaultCharset());
                            if (preview.length() > 500) {
                                preview = preview.substring(0, 500) + "...";
                            }
                            System.err.printf("[DUUIDockerDriver] Response preview: %s%n", preview);
                        }
                    }
                }
                long finish = System.currentTimeMillis();
                long timeElapsed = finish - start;
                if (timeElapsed > timeout_ms) {
                    throw new TimeoutException(format("The Container did not provide one succesful answer in %d milliseconds", timeout_ms));
                }
            }catch (Exception e) {
                
                if (fatal_error) {
                    throw e;
                } else {
                    sleepUntilDeadline(retrySleepMs, deadline);
                    iError++;
                }

                if (iError > 10) {
                    throw e;
                }
            }
        }
        System.out.println("[DUUIDockerDriver] Communication layer ready; process verification is not part of instantiation.");
        return layer;
    }

    private static void sleepUntilDeadline(long requestedMs, long deadlineMs) throws InterruptedException, TimeoutException {
        long remainingMs = deadlineMs - System.currentTimeMillis();
        if (remainingMs <= 0L) {
            throw new TimeoutException("Timed out while waiting for component responsiveness");
        }
        Thread.sleep(Math.max(1L, Math.min(requestedMs, remainingMs)));
    }

    /**
     * Set Timeout
     *
     * @param container_timeout_ms
     * @return
     */
    @Override
    public DUUIDockerDriver withTimeout(int container_timeout_ms) {
        _containerTimeout = container_timeout_ms;
        return this;
    }

    /**
     * Check whether the image is available.
     *
     * @param comp
     * @return
     */
    @Override
    public boolean canAccept(DUUIPipelineComponent comp) {
        return comp.getDockerImageName() != null;
    }

    /**
     * Instantiate the component
     *
     * @param component
     * @param jc
     * @param skipVerification
     * @return
     * @throws Exception
     */
    @Override
    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification, AtomicBoolean shutdown) throws InterruptedException, PipelineComponentException {
        String uuid = UUID.randomUUID().toString();
        while (_activeComponents.containsKey(uuid.toString())) {
            uuid = UUID.randomUUID().toString();
        }


        InstantiatedComponent comp = new InstantiatedComponent(component, uuid);

        // Inverted if check because images will never be pulled if !comp.getImageFetching() is checked.
        if (comp.getImageFetching()) {
            if (comp.getUsername() != null) {
                System.out.printf("[DUUIDockerDriver] Attempting image %s download from secure remote registry\n", comp.getImageName());
            }
            try {
                pullDockerImage(comp.getImageName(), comp.getUsername(), comp.getPassword(), shutdown);
            } catch (ImagePullException imagePullException) {
                System.err.printf("[DUUIDockerDriver] Failed to pull image %s: %s%n", comp.getImageName(), imagePullException.getMessage());
                throw new PipelineComponentException(
                        format("Failed to pull docker image %s", comp.getImageName()),
                        imagePullException
                );
            }
            if (shutdown.get()) {
                return null;
            }

            System.out.printf("[DUUIDockerDriver] Pulled image with id %s\n", comp.getImageName());
        } else {
            if (!hasLocalDockerImage(comp.getImageName())) {
                throw new InvalidParameterException(format("Could not find local docker image \"%s\". Did you misspell it or forget with .withImageFetching() to fetch it from remote registry?", comp.getImageName()));
            }
        }
        System.out.printf("[DUUIDockerDriver] Assigned new pipeline component unique id %s\n", uuid);
        String digest = getDockerImageDigest(comp.getImageName());
        comp.getPipelineComponent().__internalPinDockerImage(comp.getImageName(), digest);
        System.out.printf("[DUUIDockerDriver] Transformed image %s to pinnable image name %s\n", comp.getImageName(), comp.getPipelineComponent().getDockerImageName());

        _activeComponents.put(uuid, comp);
        // TODO: Fragen, was hier genau gemacht wird.
        for (int i = 0; i < comp.getScale(); i++) {
            if (shutdown.get()) {
                return null;
            }

            String containerid = runDockerContainer(comp.getPipelineComponent().getDockerImageName(), comp.getEnv(), comp.usesGPU(), true, 9714, false);
            int port = extractDockerPortMapping(containerid);  // Dieser port hier ist im allgemeinen nicht (bzw nie) der Port 9714 aus dem Input.

            try {
                if (port == 0) {
                    throw new UnknownError("Could not read the container port!");
                }

                String containerURL = getDockerHostUrl(containerid, 9714);

                final int iCopy = i;
                final String uuidCopy = uuid;
                IDUUICommunicationLayer layer = responsiveAfterTime(containerURL, jc, _containerTimeout, _client, (msg) -> {
                    System.out.printf("[DUUIDockerDriver][%s][Docker Replication %d/%d] %s\n", uuidCopy, iCopy + 1, comp.getScale(), msg);
                }, _luaContext, skipVerification);
                System.out.printf("[DUUIDockerDriver][%s][Docker Replication %d/%d] Container for image %s is online (URL %s) and seems to understand DUUI V1 format!\n", uuid, i + 1, comp.getScale(), comp.getImageName(), containerURL);

                /**
                 * @see
                 * @edited
                 * Dawit Terefe
                 *
                 * Starts the websocket connection.
                 */
                if (comp.isWebsocket()) {
                    String wsUrl = containerURL.replaceFirst("^http", "ws") + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS_WEBSOCKET;
                    _wsclient = new DUUIWebsocketAlt(wsUrl, comp.getWebsocketElements());
                } else {
                    _wsclient = null;
                }
                /**
                 * @see
                 * @edited
                 * Dawit Terefe
                 *
                 * Saves websocket client in ComponentInstance for
                 * retrieval in process_handler-function.
                 */

                /// Add one replica of the instantiated component per worker
                for (int j = 0; j < comp.getWorkers(); j++) {
                    comp.addInstance(new ComponentInstance(containerid, port, layer, _wsclient, containerURL));
                }
            } catch (Exception e) {
                //_interface.stop_container(containerid);
                //throw e;
            }
        }
        return shutdown.get() ? null : uuid;
    }

    /**
     * Show the maximum parallelism
     *
     * @param uuid
     */
    @Override
    public void printConcurrencyGraph(String uuid) {
        InstantiatedComponent component = (InstantiatedComponent) _activeComponents.get(uuid);
        if (component == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        System.out.printf("[DUUIDockerDriver][%s]: Maximum concurrency %d\n", uuid, component.getInstances().size());
    }

    public List<String> getEndpointUrls(String uuid) {
        InstantiatedComponent component = (InstantiatedComponent) _activeComponents.get(uuid);
        if (component == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        return component.getInstances().stream()
                .map(ComponentInstance::generateURL)
                .distinct()
                .toList();
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
    public void run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf, DUUIComposer composer) throws CASException, PipelineComponentException, CompressorException, IOException, InterruptedException, SAXException, CommunicationLayerException {
        long mutexStart = System.nanoTime();
        InstantiatedComponent comp = (InstantiatedComponent) _activeComponents.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        if (comp.isWebsocket()) {
            IDUUIInstantiatedPipelineComponent.process_handler(aCas, comp, perf);
        } else {
            IDUUIInstantiatedPipelineComponent.process(aCas, comp, perf);
        }
    }

    /**
     * Shutdown of the Docker-Driver
     *
     * @hidden
     */
    @Override
    public void shutdown() {
        if (_dockerClient != null) {
            _dockerClient.shutdown();
        }
        super.shutdown();
    }

    /**
     * Terminate a component
     *
     * @param uuid
     */
    @Override
    public boolean destroy(String uuid) {
        InstantiatedComponent comp = (InstantiatedComponent) _activeComponents.remove(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        if (!comp.getRunningAfterExit()) {
            int counter = 1;
            for (ComponentInstance inst : comp.getInstances()) {
                System.out.printf("[DUUIDockerDriver][Replication %d/%d] Stopping docker container %s...\n", counter, comp.getInstances().size(), inst.getContainerId());
                stopDockerContainer(inst.getContainerId());
                counter += 1;
            }
        }

        return true;
    }

    /**
     * V2 instantiation for Docker driver.
     * <p>
     * Mirrors the container lifecycle from {@link #instantiate(DUUIPipelineComponent, JCas, boolean, AtomicBoolean)}
     * but packages the result as a {@link DUUIComponent}<JCas> instead of storing a UUID.
     * Each container replica becomes a {@link DUUIV1Annotator}, and nodes are distributed
     * round-robin across replicas based on {@code scale × concurrency}.
     *
     * @param component        the pipeline component describing the Docker image and configuration
     * @param jc               a JCas for type system baseline (used only if verification is not skipped)
     * @param skipVerification if {@code true}, skip the pre-verification round-trip
     * @param shutdown         cooperative shutdown flag; checked between container starts
     * @return a fully initialized {@link DUUIComponent}<JCas> ready for processing
     * @throws Exception if image pull fails, container startup fails, or annotator initialization fails
     */
    @Override
    public DUUIComponent<JCas> instantiateV2(DUUIPipelineComponent component, JCas jc, boolean skipVerification,
            AtomicBoolean shutdown) throws Exception {

        String imageName = component.getDockerImageName();
        if (imageName == null) {
            throw new InvalidParameterException(
                    "The image name was not set! This is mandatory for the DUUIDockerDriver Class.");
        }

        // --- 1. Pull/verify image (mirrors instantiate()) ---
        if (component.getDockerImageFetching(false)) {
            if (component.getDockerAuthUsername() != null) {
                System.out.printf("[DUUIDockerDriver][V2] Attempting image %s download from secure remote registry%n",
                        imageName);
            }
            try {
                pullDockerImage(imageName, component.getDockerAuthUsername(),
                        component.getDockerAuthPassword(), shutdown);
            } catch (ImagePullException e) {
                System.err.printf("[DUUIDockerDriver][V2] Failed to pull image %s: %s%n", imageName, e.getMessage());
                throw new PipelineComponentException(
                        format("Failed to pull docker image %s", imageName), e);
            }
            if (shutdown.get()) {
                return null;
            }
            System.out.printf("[DUUIDockerDriver][V2] Pulled image %s%n", imageName);
        } else {
            if (!hasLocalDockerImage(imageName)) {
                throw new InvalidParameterException(
                        format("Could not find local docker image \"%s\". Did you misspell it or forget"
                                + " .withImageFetching() to fetch it from remote registry?", imageName));
            }
        }

        // Pin image to a digest-based name so subsequent runs use the exact same image
        String digest = getDockerImageDigest(imageName);
        component.__internalPinDockerImage(imageName, digest);
        System.out.printf("[DUUIDockerDriver][V2] Transformed image %s to pinnable name %s%n",
                imageName, component.getDockerImageName());

        int scale = component.getScale(1);
        int workers = component.getWorkers(1);
        String componentId = component.getName() != null ? component.getName() : "docker-component";
        boolean runAfterExit = component.getDockerRunAfterExit(false);

        List<String> containerIds = new ArrayList<>(scale);
        List<DUUIV1Annotator> annotators = new ArrayList<>(scale);

        // --- 2. Create containers and annotators ---
        for (int replicaIdx = 0; replicaIdx < scale; replicaIdx++) {
            if (shutdown.get()) {
                // Clean up containers already started before bailing out
                for (String cid : containerIds) {
                    stopDockerContainer(cid);
                }
                return null;
            }

            String containerId = runDockerContainer(component.getDockerImageName(), component.getEnv(),
                    component.getDockerGPU(false), true, 9714, false);
            containerIds.add(containerId);

            String containerURL = getDockerHostUrl(containerId, 9714);
            String replicaId = componentId + "-replica-" + replicaIdx;

            System.out.printf("[DUUIDockerDriver][V2][Docker Replication %d/%d] Container %s started, waiting for"
                    + " responsiveness at %s...%n", replicaIdx + 1, scale, containerId, containerURL);

            // Wait for container responsiveness with retries (unless skipVerification)
            if (!skipVerification) {
                waitForContainerResponsive(containerURL, _containerTimeout);
            }

            // Build endpoint and config for this replica
            IDUUIEndpoint endpoint = new DUUIHttpEndpoint(URI.create(containerURL), _client);
            DUUIV1Config config = v1Config(workers,
                    component.getSourceView(), component.getTargetView(), component.getParameters());

            // DUUIV1Annotator constructor fetches documentation, typesystem, and
            // communication layer — this also acts as a final readiness check
            DUUIV1Annotator annotator = new DUUIV1Annotator(replicaId, endpoint, config);
            annotators.add(annotator);

            System.out.printf("[DUUIDockerDriver][V2][Docker Replication %d/%d] Annotator %s ready (URL %s)%n",
                    replicaIdx + 1, scale, replicaId, containerURL);
        }

        // --- 3. Build DUUIComponent with nodes distributed round-robin ---
        List<DUUINode<JCas>> nodes = new ArrayList<>(scale * workers);
        int slot = 0;
        for (DUUIV1Annotator annotator : annotators) {
            int concurrency = annotator.config().concurrency();
            for (int j = 0; j < concurrency; j++) {
                nodes.add(DUUINode.v1(componentId + "-slot-" + slot++, annotator));
            }
        }

        // closeAction stops and removes all containers unless runAfterExit is set
        AutoCloseable closeAction = () -> {
            if (!runAfterExit) {
                int counter = 1;
                for (String cid : containerIds) {
                    System.out.printf("[DUUIDockerDriver][V2][Replication %d/%d] Stopping docker container %s...%n",
                            counter, containerIds.size(), cid);
                    stopDockerContainer(cid);
                    counter++;
                }
            }
        };

        System.out.printf("[DUUIDockerDriver][V2] Component %s instantiated with %d nodes across %d replica(s)%n",
                componentId, nodes.size(), scale);

        return new DUUIComponent<>(componentId, nodes, closeAction);
    }

    /**
     * Waits for a container to become responsive by polling required DUUI protocol endpoints.
     * Documentation is optional, so readiness is based on communication layer and typesystem.
     *
     * @param containerURL the base URL of the container (e.g., {@code http://localhost:32768})
     * @param timeoutMs    maximum total wait time in milliseconds
     * @throws PipelineComponentException if the container does not respond within the timeout
     */
    private void waitForContainerResponsive(String containerURL, int timeoutMs) throws PipelineComponentException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        int attempt = 0;
        while (System.currentTimeMillis() < deadline) {
            attempt++;
            try {
                if (requiredEndpointReady(containerURL, DUUIComposer.V1_COMPONENT_ENDPOINT_COMMUNICATION_LAYER)
                        && requiredEndpointReady(containerURL, DUUIComposer.V1_COMPONENT_ENDPOINT_TYPESYSTEM)) {
                    System.out.printf("[DUUIDockerDriver][V2] Container %s responsive after %d attempt(s)%n",
                            containerURL, attempt);
                    return;
                }
            } catch (Exception ignored) {
                // Container not ready yet — retry after a short sleep
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new PipelineComponentException(
                        format("Interrupted while waiting for container %s to become responsive", containerURL), e);
            }
        }
        throw new PipelineComponentException(
                format("Container %s did not become responsive within %d ms", containerURL, timeoutMs));
    }

    private boolean requiredEndpointReady(String containerURL, String route) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(containerURL + route))
                    .version(HttpClient.Version.HTTP_1_1)
                    .timeout(Duration.ofSeconds(5))
                    .GET()
                    .build();
            HttpResponse<Void> resp = _client.send(req, HttpResponse.BodyHandlers.discarding());
            return resp.statusCode() == 200;
        } catch (Exception ignored) {
            return false;
        }
    }

    // === DUUIDockerClient delegation helpers ===

    private void pullDockerImage(String tag, String username, String password, AtomicBoolean shutdown)
            throws ImagePullException, InterruptedException {
        try {
            if (shutdown != null && shutdown.get()) return;
            if (username != null && password != null) {
                _dockerClient.registry(username, password).pull(tag);
            } else {
                _dockerClient.registry().pull(tag);
            }
        } catch (InterruptedException e) {
            if (shutdown != null) shutdown.set(true);
            throw e;
        } catch (Exception e) {
            throw new ImagePullException(tag,
                    format("Could not fetch image %s: %s", tag, e.getMessage()), e);
        }
    }

    private boolean hasLocalDockerImage(String imageName) {
        try {
            return _dockerClient.image(imageName).exists();
        } catch (Exception e) {
            return false;
        }
    }

    private String getDockerImageDigest(String imageName) {
        if (!imageName.contains(":")) {
            imageName = imageName + ":latest";
        }
        try {
            var digests = _dockerClient.image(imageName).digests();
            return digests.isEmpty() ? null : digests.get(0);
        } catch (Exception e) {
            return null;
        }
    }

    private String runDockerContainer(String imageId, List<String> env, boolean gpu,
            boolean autoRemove, int containerPort, boolean mapDaemon) throws InterruptedException {
        return _dockerClient.image(imageId).run(cmd -> {
            HostConfig cfg = new HostConfig().withPublishAllPorts(true);
            if (autoRemove) cfg = cfg.withAutoRemove(true);
            if (gpu) {
                cfg = cfg.withDeviceRequests(List.of(
                    new com.github.dockerjava.api.model.DeviceRequest()
                        .withCapabilities(List.of(List.of("gpu")))));
            }
            if (mapDaemon) {
                cfg = cfg.withBinds(com.github.dockerjava.api.model.Bind.parse(
                    "/var/run/docker.sock:/var/run/docker.sock"));
            }
            cmd.withHostConfig(cfg);
            cmd.withExposedPorts(ExposedPort.tcp(containerPort));
            if (env != null && !env.isEmpty()) cmd.withEnv(env);
        }).id();
    }

    private int extractDockerPortMapping(String containerId) {
        try {
            var bindings = _dockerClient.container(containerId).bindings(ExposedPort.tcp(9714));
            if (bindings.isPresent() && bindings.get().length > 0) {
                return Integer.parseInt(bindings.get()[0].getHostPortSpec());
            }
        } catch (Exception e) {
            // Fall through
        }
        return 0;
    }

    private String getDockerHostUrl(String containerId, int containerPort) {
        var container = _dockerClient.container(containerId);
        var bindings = container.bindings(ExposedPort.tcp(containerPort));
        if (bindings.isEmpty() || bindings.get().length == 0) {
            throw new IllegalStateException(
                "[DUUIDockerDriver] No host binding found for container port " + containerPort);
        }
        String hostPort = bindings.get()[0].getHostPortSpec();
        List<String> candidates = new ArrayList<>(List.of("localhost", "host.docker.internal"));
        String gw = getDockerHostIp();
        if (gw != null && !gw.isBlank() && !candidates.contains(gw)) candidates.add(gw);
        for (String host : candidates) {
            if (canConnect(host, Integer.parseInt(hostPort), 700)) {
                return "http://" + host + ":" + hostPort;
            }
        }
        throw new IllegalStateException("Could not reach container on any host IP: " + candidates);
    }

    private void stopDockerContainer(String containerId) {
        try {
            var c = _dockerClient.container(containerId);
            c.stop(10);
            c.remove(false, false);
        } catch (Exception e) {
            // Container may already be stopped
        }
    }

    static String getDockerHostIp() {
        try {
            ProcessBuilder pb = new ProcessBuilder("sh", "-c", "ip route | awk '/default/ { print $3 }'");
            Process p = pb.start();
            try (java.io.BufferedReader reader = new java.io.BufferedReader(
                    new java.io.InputStreamReader(p.getInputStream()))) {
                String line = reader.readLine();
                if (line != null && !line.isEmpty()) return line.trim();
            }
        } catch (Exception ignored) {
        }
        return null;
    }

    static boolean canConnect(String host, int port, int timeoutMs) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), timeoutMs);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static class ComponentInstance implements IDUUIUrlAccessible {
        private String _container_id;
        private int _port;
        private IDUUIConnectionHandler _handler;
        private IDUUICommunicationLayer _communicationLayer;
        private String _baseUrl;

        public ComponentInstance(String id, int port, IDUUICommunicationLayer communicationLayer) {
            this(id, port, communicationLayer, null, DUUIComposer.getLocalhost() + ":" + port);
        }

        public ComponentInstance(String id, int port, IDUUICommunicationLayer layer, IDUUIConnectionHandler handler) {
            this(id, port, layer, handler, DUUIComposer.getLocalhost() + ":" + port);
        }

        public ComponentInstance(String id, int port, IDUUICommunicationLayer layer, IDUUIConnectionHandler handler, String baseUrl) {
            _container_id = id;
            _port = port;
            _communicationLayer = layer;
            _handler = handler;
            _baseUrl = baseUrl;
        }

        public IDUUICommunicationLayer getCommunicationLayer() {
            return _communicationLayer;
        }

        String getContainerId() {
            return _container_id;
        }

        int getContainerPort() {
            return _port;
        }

        public String generateURL() {
            return _baseUrl;
        }

        String getContainerUrl() {
            return _baseUrl;
        }

        String getContainterId() {
            return _container_id;
        }

        public IDUUIConnectionHandler getHandler() {
            return _handler;
        }
    }

    static class InstantiatedComponent implements IDUUIInstantiatedPipelineComponent {
        private String _image_name;
        private final ConcurrentLinkedQueue<ComponentInstance> _instances;
        private final ConcurrentHashMap<String, ComponentInstance> _total_instances;
        private boolean _gpu;
        private List<String> _env;
        private boolean _keep_runnging_after_exit;
        private int _scale;
        private int _workers;
        private boolean _withImageFetching;
        private boolean _websocket;
        private int _ws_elements;

        private String _reg_password;
        private String _reg_username;
        private String _uniqueComponentKey;
        private Map<String, String> _parameters;
        private String _sourceView;
        private String _targetView;
        private DUUIPipelineComponent _component;


        @Override
        public Triplet<IDUUIUrlAccessible, Long, Long> getComponent() {
            long mutexStart = System.nanoTime();
            ComponentInstance inst = _instances.poll();
            while (inst == null) {
                inst = _instances.poll();
            }
            long mutexEnd = System.nanoTime();
            return Triplet.with(inst, mutexStart, mutexEnd);
        }

        @Override
        public void addComponent(IDUUIUrlAccessible access) {

            _total_instances.put(((ComponentInstance) access).getContainterId(),
                (ComponentInstance) access
            );
            _instances.add((ComponentInstance) access);
        }

        InstantiatedComponent(DUUIPipelineComponent comp, String uuid) {
            _component = comp;
            _image_name = comp.getDockerImageName();
            _parameters = comp.getParameters();
            _targetView = comp.getTargetView();
            _sourceView = comp.getSourceView();
            if (_image_name == null) {
                throw new InvalidParameterException("The image name was not set! This is mandatory for the DUUIDockerDriver Class.");
            }
            _withImageFetching = comp.getDockerImageFetching(false);

            _uniqueComponentKey = uuid;


            _instances = new ConcurrentLinkedQueue<>();
            _total_instances = new ConcurrentHashMap<>();

            _scale = comp.getScale(1);
            _workers = comp.getWorkers(1);

            _gpu = comp.getDockerGPU(false);

            _env = comp.getEnv();

            _keep_runnging_after_exit = comp.getDockerRunAfterExit(false);

            _reg_password = comp.getDockerAuthPassword();
            _reg_username = comp.getDockerAuthUsername();

            _websocket = comp.isWebsocket();
            _ws_elements = comp.getWebsocketElements();
        }

        public DUUIPipelineComponent getPipelineComponent() {
            return _component;
        }

        public String getUniqueComponentKey() {
            return _uniqueComponentKey;
        }

        public String getPassword() {
            return _reg_password;
        }

        public String getUsername() {
            return _reg_username;
        }

        public boolean getImageFetching() {
            return _withImageFetching;
        }

        public String getImageName() {
            return _image_name;
        }

        public int getScale() {
            return _scale;
        }

        public int getWorkers() {
            return _workers;
        }

        public boolean getRunningAfterExit() {
            return _keep_runnging_after_exit;
        }

        public void addInstance(ComponentInstance inst) {
            _instances.add(inst);
        }

        public boolean usesGPU() {
            return _gpu;
        }

        public List<String> getEnv() {
            return _env;
        }

        public Collection<ComponentInstance> getTotalInstances() {
            return _total_instances.values();
        }

        public ConcurrentLinkedQueue<ComponentInstance> getInstances() {
            return _instances;
        }

        public Map<String, String> getParameters() {
            return _parameters;
        }

        public String getSourceView() {return _sourceView; }

        public String getTargetView() {return _targetView; }

        public boolean isWebsocket() {
            return _websocket;
        }

        public int getWebsocketElements() {
            return _ws_elements;
        }
    }

    public static class Component {
        private DUUIPipelineComponent _component;

        public Component withParameter(String key, String value) {
            _component.withParameter(key, value);
            return this;
        }

        public Component withView(String viewName) {
            _component.withView(viewName);
            return this;
        }

        public Component withSourceView(String viewName) {
            _component.withSourceView(viewName);
            return this;
        }

        public Component withTargetView(String viewName) {
            _component.withTargetView(viewName);
            return this;
        }

        public Component(String target) throws URISyntaxException, IOException {
            _component = new DUUIPipelineComponent();
            _component.withDockerImageName(target);
        }

        public Component(DUUIPipelineComponent pComponent) throws URISyntaxException, IOException {
            _component = pComponent;
        }

        public Component withDescription(String description) {
            _component.withDescription(description);
            return this;
        }

        /**
         * Start the given number of parallel instances (containers).
         * @param scale Number of containers to start.
         * @return {@code this}
         */
        public Component withScale(int scale) {
            _component.withScale(scale);
            return this;
        }

        /**
         * Set the maximum concurrency-level of each component by instantiating the multiple replicas per container.
         * @param workers Number of replicas per container.
         * @return {@code this}
         */
        public Component withWorkers(int workers) {
            _component.withWorkers(workers);
            return this;
        }

        public Component withRegistryAuth(String username, String password) {
            _component.withDockerAuth(username, password);
            return this;
        }

        public Component withImageFetching() {
            return withImageFetching(true);
        }

        public Component withImageFetching(boolean imageFetching) {
            _component.withDockerImageFetching(imageFetching);
            return this;
        }

        public Component withGPU(boolean gpu) {
            _component.withDockerGPU(gpu);
            return this;
        }

        public Component withRunningAfterDestroy(boolean run) {
            _component.withDockerRunAfterExit(run);
            return this;
        }

        public Component withWebsocket(boolean b) {
            _component.withWebsocket(b);
            return this;
        }

        public Component withWebsocket(boolean b, int elements) {
            _component.withWebsocket(b, elements);
            return this;
        }

        public Component withSegmentationStrategy(DUUISegmentationStrategy strategy) {
            _component.withSegmentationStrategy(strategy);
            return this;
        }

        public <T extends DUUISegmentationStrategy> Component withSegmentationStrategy(Class<T> strategyClass) throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
            _component.withSegmentationStrategy(strategyClass.getDeclaredConstructor().newInstance());
            return this;
        }

        public Component withEnv(String... envString) {
            _component.withEnv(envString);
            return this;
        }

        public DUUIPipelineComponent build() {
            _component.withDriver(DUUIDockerDriver.class);
            return _component;
        }

        public Component withName(String name) {
            _component.withName(name);
            return this;
        }
    }
}
