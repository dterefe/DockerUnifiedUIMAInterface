package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;


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
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIDockerInterface;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIFallbackCommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.PipelineComponentException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaCommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import static java.lang.String.format;

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
public class DUUIDockerDriver implements IDUUIDriverInterface {
    private DUUIDockerInterface _interface;
    private HttpClient _client;
    private IDUUIConnectionHandler _wsclient;

    private HashMap<String, InstantiatedComponent> _active_components;
    private int _container_timeout;
    private DUUILuaContext _luaContext;

    private final static Logger LOGGER = Logger.getLogger(DUUIComposer.class.getName());

    public DUUIDockerDriver() throws IOException, UIMAException, SAXException {
        _interface = new DUUIDockerInterface();
        _client = HttpClient.newHttpClient();

        JCas _basic = JCasFactory.createJCas();
        _basic.setDocumentLanguage("en");
        _basic.setDocumentText("Hello World!");
        _container_timeout = 10000;


        TypeSystemDescription desc = TypeSystemUtil.typeSystem2TypeSystemDescription(_basic.getTypeSystem());
        StringWriter wr = new StringWriter();
        desc.toXML(wr);
        _active_components = new HashMap<String, InstantiatedComponent>();
        _luaContext = null;
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
        _interface = new DUUIDockerInterface();
        _client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(timeout)).build();

        _container_timeout = timeout;

        _active_components = new HashMap<String, InstantiatedComponent>();
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

        long start = System.currentTimeMillis();
        IDUUICommunicationLayer layer = new DUUIFallbackCommunicationLayer();
        boolean fatal_error = false;
        int iError = 0;

        System.out.printf("[DEBUG] Entered responsiveAfterTime. url=%s, timeout_ms=%d%n", url, timeout_ms);
        System.out.printf("[DEBUG] PID=%s, Inside docker? %b%n", java.lang.management.ManagementFactory.getRuntimeMXBean().getName(), isLikelyInsideDocker());
        System.out.printf("[DEBUG] HttpClient type: %s%n", client.getClass().getCanonicalName());

        while (true) {
            HttpRequest request = null;
            try {
                String requestUri = url + DUUIComposer.V1_COMPONENT_ENDPOINT_COMMUNICATION_LAYER;
                System.out.printf("[DEBUG] Preparing HttpRequest: %s%n", requestUri);

                request = HttpRequest.newBuilder()
                        .uri(URI.create(requestUri))
                        .version(HttpClient.Version.HTTP_1_1)
                        .timeout(Duration.ofSeconds(timeout_ms))
                        .GET()
                        .build();
            } catch (Exception e) {
                System.err.println("[DEBUG] Exception building HttpRequest:");
                e.printStackTrace();
            }

            try {
                HttpResponse<byte[]> resp = null;

                boolean connectionError = true;
                int iCount = 0;
                while (connectionError && iCount < 10) {
                    System.out.printf("[DEBUG] Attempt #%d: Sending HTTP GET to %s%n", iCount + 1, request.uri());
                    try {
                        resp = client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).join();
                        connectionError = false;
                        System.out.printf("[DEBUG] Got HTTP response: %d bytes, status: %d%n", resp.body() != null ? resp.body().length : -1, resp.statusCode());
                    } catch (Exception e) {
                        System.err.printf("[DEBUG] HTTP connection error on try #%d: %s (%s)\n", iCount + 1, e.getClass().getSimpleName(), e.getMessage());
                        if (e instanceof java.net.ConnectException) {
                            System.err.printf("[DEBUG] ConnectException: Host=%s, Port=%s%n", request.uri().getHost(), request.uri().getPort());
                            Thread.sleep(timeout_ms);
                            iCount++;
                        } else if (e instanceof CompletionException) {
                            Thread.sleep(timeout_ms);
                            iCount++;
                        } else {
                            System.err.printf("[DEBUG] Unexpected exception: %s%n", e);
                            throw e;
                        }
                    }
                }

                if (resp == null) {
                    throw new Exception("[DEBUG] No HTTP response after 10 tries!");
                }

                if (resp.statusCode() == 200) {
                    String body2 = new String(resp.body(), Charset.defaultCharset());
                    try {
                        printfunc.operation("Component lua communication layer, loading...");
                        System.out.println("[DEBUG] 200 OK: Body length = " + body2.length());
                        IDUUICommunicationLayer lua_com = new DUUILuaCommunicationLayer(body2, "requester", context);
                        layer = lua_com;
                        printfunc.operation("Component lua communication layer, loaded.");
                        break;
                    } catch (Exception e) {
                        fatal_error = true;
                        System.err.println("[DEBUG] Fatal error: Lua script is not runnable!");
                        e.printStackTrace();
                        throw new Exception("Component provided a lua script which is not runnable.");
                    }
                } else if (resp.statusCode() == 404) {
                    printfunc.operation("Component provided no own communication layer implementation using fallback.");
                    System.out.println("[DEBUG] 404 Not Found: Using fallback.");
                    break;
                } else {
                    System.err.printf("[DEBUG] Got HTTP status: %d (body %d bytes)%n", resp.statusCode(), resp.body() != null ? resp.body().length : -1);
                }

                long finish = System.currentTimeMillis();
                long timeElapsed = finish - start;
                if (timeElapsed > timeout_ms) {
                    System.err.printf("[DEBUG] Timeout: waited %d ms%n", timeElapsed);
                    throw new TimeoutException(String.format("The Container did not provide one successful answer in %d milliseconds", timeout_ms));
                }

            } catch (Exception e) {
                System.err.printf("[DEBUG] Exception in HTTP/Layer creation: %s%n", e.getMessage());
                if (fatal_error) {
                    throw e;
                } else {
                    Thread.sleep(2000l);
                    iError++;
                }

                if (iError > 10) {
                    System.err.println("[DEBUG] Too many errors, aborting.");
                    throw e;
                }
            }
        }

        if (skipVerification) {
            System.out.println("[DEBUG] skipVerification=true, returning early.");
            return layer;
        }
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            System.out.println("[DEBUG] Serializing layer for verification...");
            layer.serialize(jc, stream, null, "_InitialView");
        } catch (Exception e) {
            System.err.printf("[DEBUG] Serialization failed: %s%n", e.getMessage());
            throw new Exception(String.format("The serialization step of the communication layer fails for implementing class %s", layer.getClass().getCanonicalName()));
        }

        String processUri = url + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS;
        System.out.printf("[DEBUG] Sending POST request to: %s (bytes: %d)%n", processUri, stream.size());
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(processUri))
            .version(HttpClient.Version.HTTP_1_1)
            .POST(HttpRequest.BodyPublishers.ofByteArray(stream.toByteArray()))
            .build();
        HttpResponse<byte[]> resp = client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).join();
        System.out.printf("[DEBUG] POST response status: %d, length: %d%n", resp.statusCode(), resp.body() != null ? resp.body().length : -1);

        if (resp.statusCode() == 200) {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(resp.body());
            try {
                System.out.println("[DEBUG] Deserializing response into communication layer...");
                layer.deserialize(jc, inputStream, "_InitialView");
            } catch (Exception e) {
                System.err.printf("[DEBUG] Caught exception deserializing response: %s\nResponse: %s\n", e.getMessage(), new String(resp.body(), StandardCharsets.UTF_8));
                throw e;
            }
            return layer;
        } else {
            System.err.printf("[DEBUG] POST request failed, response code: %d, body: %s%n", resp.statusCode(), new String(resp.body(), StandardCharsets.UTF_8));
            throw new Exception(String.format("The container returned response with code != 200\nResponse %s", new String(resp.body(), StandardCharsets.UTF_8)));
        }
    }

    // Optional: Docker detection helper (very simple heuristic)
    private static boolean isLikelyInsideDocker() {
        File dockerEnv = new File("/.dockerenv");
        File cgroup = new File("/proc/1/cgroup");
        try {
            if (dockerEnv.exists()) return true;
            if (cgroup.exists()) {
                String cg = new String(java.nio.file.Files.readAllBytes(cgroup.toPath()));
                return cg.contains("docker") || cg.contains("kubepods") || cg.contains("containerd");
            }
        } catch (IOException ignored) {}
        return false;
    }


    /**
     * Set Lua-Context
     *
     * @param luaContext
     */
    public void setLuaContext(DUUILuaContext luaContext) {
        _luaContext = luaContext;
    }

    /**
     * Set Timeout
     *
     * @param container_timeout_ms
     * @return
     */
    public DUUIDockerDriver withTimeout(int container_timeout_ms) {
        _container_timeout = container_timeout_ms;
        return this;
    }

    /**
     * Check whether the image is available.
     *
     * @param comp
     * @return
     */
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
    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification, AtomicBoolean shutdown) throws InterruptedException {
        String uuid = UUID.randomUUID().toString();

        while (_active_components.containsKey(uuid)) {
            uuid = UUID.randomUUID().toString();
        }

        InstantiatedComponent comp = new InstantiatedComponent(component, uuid);

        // Inverted if check because images will never be pulled if !comp.getImageFetching() is checked.
        if (comp.getImageFetching()) {
            if (comp.getUsername() != null) {
                System.out.printf("[DockerLocalDriver] Attempting image %s download from secure remote registry\n", comp.getImageName());
            }
            _interface.pullImage(comp.getImageName(), comp.getUsername(), comp.getPassword(), shutdown);
            if (shutdown.get()) {
                return null;
            }

            System.out.printf("[DockerLocalDriver] Pulled image with id %s\n", comp.getImageName());
        } else {
//            _interface.pullImage(comp.getImageName());
            if (!_interface.hasLocalImage(comp.getImageName())) {
                throw new InvalidParameterException(format("Could not find local docker image \"%s\". Did you misspell it or forget with .withImageFetching() to fetch it from remote registry?", comp.getImageName()));
            }
        }
        System.out.printf("[DockerLocalDriver] Assigned new pipeline component unique id %s\n", uuid);
        String digest = _interface.getDigestFromImage(comp.getImageName());
        comp.getPipelineComponent().__internalPinDockerImage(comp.getImageName(), digest);
        System.out.printf("[DockerLocalDriver] Transformed image %s to pinnable image name %s\n", comp.getImageName(), comp.getPipelineComponent().getDockerImageName());

        _active_components.put(uuid, comp);
        // TODO: Fragen, was hier genau gemacht wird.
        for (int i = 0; i < comp.getScale(); i++) {
            if (shutdown.get()) {
                return null;
            }

            String containerid = _interface.run(comp.getPipelineComponent().getDockerImageName(), comp.usesGPU(), true, 9714, false);
            int port = _interface.extract_port_mapping(containerid);  // Dieser port hier ist im allgemeinen nicht (bzw nie) der Port 9714 aus dem Input.
            
            try {
                if (port == 0) {
                    throw new UnknownError("Could not read the container port!");
                }

                String containerURL = _interface.getHostUrl(containerid, 9714);

                final int iCopy = i;
                final String uuidCopy = uuid;
                IDUUICommunicationLayer layer = responsiveAfterTime(containerURL, jc, _container_timeout, _client, (msg) -> {
                    System.out.printf("[DockerLocalDriver][%s][Docker Replication %d/%d] %s\n", uuidCopy, iCopy + 1, comp.getScale(), msg);
                }, _luaContext, skipVerification);
                System.out.printf("[DockerLocalDriver][%s][Docker Replication %d/%d] Container for image %s is online (URL %s) and seems to understand DUUI V1 format!\n", 
                    uuid, i + 1, comp.getScale(), comp.getImageName(), containerURL);

                comp.addInstance(new ComponentInstance(containerid, port, layer, containerURL));
            } catch (Exception e) {
                //_interface.stop_container(containerid);
                // throw e;
            }
        }
        return shutdown.get() ? null : uuid;
    }

    /**
     * Show the maximum parallelism
     *
     * @param uuid
     */
    public void printConcurrencyGraph(String uuid) {
        InstantiatedComponent component = _active_components.get(uuid);
        if (component == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        System.out.printf("[DockerLocalDriver][%s]: Maximum concurrency %d\n", uuid, component.getInstances().size());
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
    public TypeSystemDescription get_typesystem(String uuid) throws InterruptedException, IOException, SAXException, CompressorException, ResourceInitializationException {
        InstantiatedComponent comp = _active_components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        return IDUUIInstantiatedPipelineComponent.getTypesystem(uuid, comp);
    }

    /**
     * init reader component
     * @param uuid
     * @param filePath
     * @return
     */
    @Override
    public int initReaderComponent(String uuid, Path filePath) {
        InstantiatedComponent comp = _active_components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        return IDUUIInstantiatedPipelineReaderComponent.initComponent(comp, filePath);
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
    public void run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf, DUUIComposer composer) throws CASException, PipelineComponentException {
        long mutexStart = System.nanoTime();
        InstantiatedComponent comp = _active_components.get(uuid);
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
    public void shutdown() {

        if (_interface != null) {
            _interface.shutdown();
        }
    }

    /**
     * Terminate a component
     *
     * @param uuid
     */
    public boolean destroy(String uuid) {
        InstantiatedComponent comp = _active_components.remove(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        if (!comp.getRunningAfterExit()) {
            int counter = 1;
            int containerCount = comp.getScale();
            String image = comp.getImageName();
            List<ComponentInstance> all = new ArrayList<>(comp._instances);
            for (ComponentInstance inst : all) {
                System.out.printf("[DockerLocalDriver][Replication %d/%d] Stopping docker container %s...\n",
                    counter, containerCount, image
                );

                _interface.stop_container(inst.getContainerId());
                counter++;
            }
        }

        return true;
    }

    public static class ComponentInstance implements IDUUIUrlAccessible {
        private String _container_id;
        private int _port;
        private String _url; 
        private IDUUICommunicationLayer _communicationLayer;

        public ComponentInstance(String id, int port, IDUUICommunicationLayer communicationLayer, String url) {
            _container_id = id;
            _port = port;
            _communicationLayer = communicationLayer;
            _url = url;
        }

        public IDUUICommunicationLayer getCommunicationLayer() {
            return _communicationLayer;
        }

        public ComponentInstance(String id, int port, IDUUICommunicationLayer layer) {
            _container_id = id;
            _port = port;
            _communicationLayer = layer;
        }

        String getContainerId() {
            return _container_id;
        }

        int getContainerPort() {
            return _port;
        }

        public String generateURL() {
            return format(_url);
        }

        String getContainerUrl() {
            return format(_url);
        }
    }

    static class InstantiatedComponent implements IDUUIInstantiatedPipelineComponent {
        private String _image_name;
        private ConcurrentLinkedQueue<ComponentInstance> _instances;
        private boolean _gpu;
        private boolean _keep_runnging_after_exit;
        private int _scale;
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


        public Triplet<IDUUIUrlAccessible, Long, Long> getComponent() {
            long mutexStart = System.nanoTime();
            ComponentInstance inst = _instances.poll();
            while (inst == null) {
                inst = _instances.poll();
            }
            long mutexEnd = System.nanoTime();
            return Triplet.with(inst, mutexStart, mutexEnd);
        }

        public void addComponent(IDUUIUrlAccessible access) {
            _instances.add((ComponentInstance) access);
        }

        InstantiatedComponent(DUUIPipelineComponent comp, String uuid) {
            _component = comp;
            _image_name = comp.getDockerImageName();
            _parameters = comp.getParameters();
            _targetView = comp.getTargetView();
            _sourceView = comp.getSourceView();
            if (_image_name == null) {
                throw new InvalidParameterException("The image name was not set! This is mandatory for the DockerLocalDriver Class.");
            }
            _withImageFetching = comp.getDockerImageFetching(false);

            _uniqueComponentKey = uuid;


            _instances = new ConcurrentLinkedQueue<ComponentInstance>();

            _scale = comp.getScale(1);

            _gpu = comp.getDockerGPU(false);

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

        public boolean getRunningAfterExit() {
            return _keep_runnging_after_exit;
        }

        public void addInstance(ComponentInstance inst) {
            _instances.add(inst);
        }

        public boolean usesGPU() {
            return _gpu;
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

        public Component withScale(int scale) {
            _component.withScale(scale);
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
