package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;


import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.*;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.javatuples.Triplet;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.DUUIWebsocketAlt;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.CommunicationLayerException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.ImagePullException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.PipelineComponentException;
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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;

/**
 *
 * @author Alexander Leonhardt
 */
public class DUUISwarmDriver extends DUUIV1Driver {
    private final DUUIDockerClient _dockerClient;
    private IDUUIConnectionHandler _wsclient;
    private String _withSwarmVisualizer;
    private String _host = "localhost";

    public DUUISwarmDriver() throws IOException {
        _dockerClient = new DUUIDockerClient();
        _containerTimeout = 10000;
        _withSwarmVisualizer = null;
        _activeComponents = new HashMap<>();
    }

    public DUUISwarmDriver(int timeout) throws IOException, UIMAException {
        _dockerClient = new DUUIDockerClient();
        _containerTimeout = timeout;
        _client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(timeout)).build();
        _activeComponents = new HashMap<>();
    }

    public DUUISwarmDriver withHostname(String sHostname) {
        this._host = sHostname;
        return this;
    }

    public String getHostname() {
        return this._host;
    }

    public DUUISwarmDriver withSwarmVisualizer() throws InterruptedException {
        return withSwarmVisualizer(null);
    }

    public DUUISwarmDriver withSwarmVisualizer(Integer port) throws InterruptedException {
        if (_withSwarmVisualizer == null) {
            try {
                _dockerClient.registry().pull("dockersamples/visualizer");
            } catch (Exception e) {
                throw new IllegalStateException("Unable to pull swarm visualizer image.", e);
            }
            if (port == null) {
                _withSwarmVisualizer = runContainer("dockersamples/visualizer", null, false, true, 8080, null, true);
            } else {
                _withSwarmVisualizer = runContainer("dockersamples/visualizer", null, false, true, 8080, port, true);
            }
            int port_mapping = extractPortMappingFor(_withSwarmVisualizer, 8080);
            System.out.printf("[DUUISwarmDriver] Running visualizer on address http://" + getHostname() + ":%d\n", port_mapping);
            Thread.sleep(1500);
        }
        return this;
    }

    @Override
    public void shutdown() {
        if (_withSwarmVisualizer != null) {
            System.out.println("[DUUISwarmDriver] Shutting down swarm visualizer now!");
            stopContainer(_withSwarmVisualizer);
            _withSwarmVisualizer = null;
        }
        super.shutdown();
    }

    @Override
    public DUUISwarmDriver withTimeout(int container_timeout_ms) {
        _containerTimeout = container_timeout_ms;
        return this;
    }

    @Override
    public boolean canAccept(DUUIPipelineComponent comp) {
        try {
            InstantiatedComponent s = new InstantiatedComponent(comp, "validation");
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification, AtomicBoolean shutdown) throws Exception {
        String uuid = UUID.randomUUID().toString();
        while (_activeComponents.containsKey(uuid)) {
            uuid = UUID.randomUUID().toString();
        }

        if (!isSwarmManagerNode()) {
            throw new InvalidParameterException("This node is not a Docker Swarm Manager, thus cannot create and schedule new services!");
        }
        DUUISwarmDriver.InstantiatedComponent comp = new DUUISwarmDriver.InstantiatedComponent(component, uuid);

        if (!hasLocalImage(comp.getImageName())) {
            try {
                _dockerClient.registry().pull(comp.getImageName());
            } catch (Exception e) {
                throw new PipelineComponentException(format("Failed to pull docker image %s", comp.getImageName()), e);
            }
            if (shutdown.get()) {
                return null;
            }
        }

        if (comp.isBackedByLocalImage()) {
            System.out.printf("[DockerSwarmDriver] Attempting to push local image %s to remote image registry %s\n", comp.getLocalImageName(), comp.getImageName());
            if (comp.getUsername() != null && comp.getPassword() != null) {
                System.out.println("[DockerSwarmDriver] Using provided password and username to authentificate against the remote registry");
            }
            pushImage(comp.getImageName(), comp.getLocalImageName(), comp.getUsername(), comp.getPassword());
        }
        System.out.printf("[DockerSwarmDriver] Assigned new pipeline component unique id %s\n", uuid);

        String digest = getDigestFromImage(comp.getImageName());
        comp.getPipelineComponent().__internalPinDockerImage(comp.getImageName(), digest);
        System.out.printf("[DockerSwarmDriver] Transformed image %s to pinnable image name %s\n", comp.getImageName(), digest);

        String serviceid = runService(digest, comp.getScale(), comp.getConstraints());
        int port = extractServicePortMapping(serviceid);

        System.out.printf("[DockerSwarmDriver][%s] Started service, waiting for it to become responsive...\n", uuid);

        if (port == 0) {
            throw new UnknownError("Could not read the service port!");
        }
        final String uuidCopy = uuid;
        IDUUICommunicationLayer layer = null;
        try {
            if (shutdown.get()) {
                return null;
            }
            layer = DUUIDockerDriver.responsiveAfterTime("http://" + getHostname() + ":" + port, jc, _containerTimeout, _client, (msg) -> {
                System.out.printf("[DockerSwarmDriver][%s][%d Replicas] %s\n", uuidCopy, comp.getScale(), msg);
            }, _luaContext, skipVerification);
        } catch (Exception e) {
            rmService(serviceid);
            throw e;
        }

        System.out.printf("[DockerSwarmDriver][%s][%d Replicas] Service for image %s is online (URL http://" + getHostname() + ":%d) and seems to understand DUUI V1 format!\n", uuid, comp.getScale(), comp.getImageName(), port);

        comp.initialise(serviceid, port, layer, this);
        Thread.sleep(500);

        _activeComponents.put(uuid, comp);

        return shutdown.get() ? null : uuid;
    }

    @Override
    public void run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf, DUUIComposer composer) throws CASException, PipelineComponentException, CompressorException, IOException, InterruptedException, SAXException, CommunicationLayerException {
        DUUISwarmDriver.InstantiatedComponent comp = (DUUISwarmDriver.InstantiatedComponent) _activeComponents.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }

        if (comp.isWebsocket()) {
            IDUUIInstantiatedPipelineComponent.process_handler(aCas, comp, perf);
        } else {
            IDUUIInstantiatedPipelineComponent.process(aCas, comp, perf);
        }
    }

    @Override
    public boolean destroy(String uuid) {
        DUUISwarmDriver.InstantiatedComponent comp = (DUUISwarmDriver.InstantiatedComponent) _activeComponents.remove(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the Swarm Driver");
        }
        if (!comp.getRunningAfterExit()) {
            System.out.printf("[DockerSwarmDriver] Stopping service %s...\n", comp.getServiceId());
            rmService(comp.getServiceId());
        }

        return true;
    }

    /**
     * V2 instantiation for Docker Swarm driver.
     */
    @Override
    public DUUIComponent<JCas> instantiateV2(DUUIPipelineComponent component, JCas jc, boolean skipVerification,
            AtomicBoolean shutdown) throws Exception {

        String imageName = component.getDockerImageName();
        if (imageName == null) {
            throw new InvalidParameterException(
                    "The image name was not set! This is mandatory for the DUUISwarmDriver Class.");
        }

        if (!isSwarmManagerNode()) {
            throw new InvalidParameterException(
                    "This node is not a Docker Swarm Manager, thus cannot create and schedule new services!");
        }

        // --- 1. Pull/verify image ---
        if (!hasLocalImage(imageName)) {
            try {
                _dockerClient.registry(
                        component.getDockerAuthUsername(),
                        component.getDockerAuthPassword()).pull(imageName);
            } catch (Exception e) {
                throw new PipelineComponentException(
                        format("Failed to pull docker image %s", imageName), e);
            }
            if (shutdown.get()) {
                return null;
            }
            System.out.printf("[SwarmDriver][V2] Pulled image %s%n", imageName);
        }

        // Pin image to a digest-based name
        String digest = getDigestFromImage(imageName);
        component.__internalPinDockerImage(imageName, digest);
        System.out.printf("[SwarmDriver][V2] Transformed image %s to pinnable name %s%n",
                imageName, component.getDockerImageName());

        int scale = component.getScale(1);
        int workers = component.getWorkers(1);
        String componentId = component.getName() != null ? component.getName() : "swarm-component";
        boolean runAfterExit = component.getDockerRunAfterExit(false);

        // --- 2. Create Swarm service ---
        String serviceId = runService(digest, scale, component.getConstraints());
        int port = extractServicePortMapping(serviceId);

        if (port == 0) {
            throw new UnknownError("Could not read the service port!");
        }

        String serviceURL = "http://" + _host + ":" + port;

        System.out.printf("[SwarmDriver][V2][%d Replicas] Service %s created, waiting for responsiveness at %s...%n",
                scale, serviceId, serviceURL);

        // --- 3. Wait for service responsiveness ---
        if (!skipVerification) {
            waitForContainerResponsive(serviceURL, _containerTimeout);
        }

        // --- 4. Build annotators ---
        List<DUUIV1Annotator> annotators = new ArrayList<>(scale);
        for (int replicaIdx = 0; replicaIdx < scale; replicaIdx++) {
            String replicaId = componentId + "-replica-" + replicaIdx;
            IDUUIEndpoint endpoint = new DUUIHttpEndpoint(URI.create(serviceURL), _client);
            DUUIV1Config config = new DUUIV1Config(workers,
                    component.getSourceView(), component.getTargetView(), component.getParameters());

            DUUIV1Annotator annotator = new DUUIV1Annotator(replicaId, endpoint, config);
            annotators.add(annotator);

            System.out.printf("[SwarmDriver][V2][Replica %d/%d] Annotator %s ready (URL %s)%n",
                    replicaIdx + 1, scale, replicaId, serviceURL);
        }

        // --- 5. Build DUUIComponent ---
        List<DUUINode<JCas>> nodes = new ArrayList<>(scale * workers);
        int slot = 0;
        for (DUUIV1Annotator annotator : annotators) {
            int concurrency = annotator.config().concurrency();
            for (int j = 0; j < concurrency; j++) {
                nodes.add(DUUINode.v1(componentId + "-slot-" + slot++, annotator));
            }
        }

        AutoCloseable closeAction = () -> {
            if (!runAfterExit) {
                System.out.printf("[SwarmDriver][V2] Removing service %s...%n", serviceId);
                rmService(serviceId);
            }
        };

        System.out.printf("[SwarmDriver][V2] Component %s instantiated with %d nodes across %d replica(s)%n",
                componentId, nodes.size(), scale);

        return new DUUIComponent<>(componentId, nodes, closeAction);
    }

    // === DUUIDockerClient delegation helpers ===

    private boolean isSwarmManagerNode() {
        return _dockerClient.docker().infoCmd().exec().getSwarm().getControlAvailable();
    }

    private boolean hasLocalImage(String imageName) {
        try {
            return _dockerClient.image(imageName).exists();
        } catch (Exception e) {
            return false;
        }
    }

    private String getDigestFromImage(String imageName) {
        if (!imageName.contains(":")) imageName = imageName + ":latest";
        try {
            var digests = _dockerClient.image(imageName).digests();
            return digests.isEmpty() ? null : digests.get(0);
        } catch (Exception e) {
            return null;
        }
    }

    private String runContainer(String imageId, List<String> env, boolean gpu, boolean autoRemove,
            int containerPort, Integer hostPort, boolean mapDaemon) throws InterruptedException {
        return _dockerClient.image(imageId).run(cmd -> {
            HostConfig cfg = new HostConfig().withPublishAllPorts(true);
            if (autoRemove) cfg = cfg.withAutoRemove(true);
            if (gpu) {
                cfg = cfg.withDeviceRequests(List.of(
                    new DeviceRequest().withCapabilities(List.of(List.of("gpu")))));
            }
            if (hostPort != null && hostPort > 0) {
                cfg.withPortBindings(new PortBinding(
                    new Ports.Binding(null, String.valueOf(hostPort)),
                    new ExposedPort(containerPort)));
            }
            if (mapDaemon) {
                cfg = cfg.withBinds(Bind.parse("/var/run/docker.sock:/var/run/docker.sock"));
            }
            cmd.withHostConfig(cfg);
            cmd.withExposedPorts(ExposedPort.tcp(containerPort));
            if (env != null && !env.isEmpty()) cmd.withEnv(env);
        }).id();
    }

    private int extractPortMappingFor(String containerId, int port) {
        try {
            var bindings = _dockerClient.container(containerId).bindings(ExposedPort.tcp(port));
            if (bindings.isPresent() && bindings.get().length > 0) {
                return Integer.parseInt(bindings.get()[0].getHostPortSpec());
            }
        } catch (Exception ignored) { }
        return 0;
    }

    private void stopContainer(String containerId) {
        try {
            var c = _dockerClient.container(containerId);
            c.stop(10);
            c.remove(false, false);
        } catch (Exception ignored) { }
    }

    private void pushImage(String remoteName, String localName, String username, String password) throws InterruptedException {
        if (!hasLocalImage(localName)) {
            throw new InvalidParameterException(format("Could not find local image %s", localName));
        }
        var docker = _dockerClient.docker();
        docker.tagImageCmd(localName, remoteName, "latest").exec();
        var pushCmd = docker.pushImageCmd(remoteName);
        if (username != null && password != null) {
            AuthConfig cfg = new AuthConfig().withPassword(password).withUsername(username);
            pushCmd.withAuthConfig(cfg);
        }
        pushCmd.start().awaitCompletion();
    }

    private String runService(String imageName, int scale, List<String> constraints) throws InterruptedException {
        var docker = _dockerClient.docker();
        ServiceSpec spec = new ServiceSpec();
        ServiceModeConfig cfg = new ServiceModeConfig();
        ServiceReplicatedModeOptions opts = new ServiceReplicatedModeOptions();
        cfg.withReplicated(opts.withReplicas(scale));
        spec.withMode(cfg);

        TaskSpec task = new TaskSpec();
        ContainerSpec cont = new ContainerSpec().withImage(imageName);
        task.withContainerSpec(cont);
        if (constraints != null && !constraints.isEmpty()) {
            task.withPlacement(new ServicePlacement().withConstraints(constraints));
        }
        spec.withTaskTemplate(task);

        EndpointSpec end = new EndpointSpec();
        List<PortConfig> portcfg = new LinkedList<>();
        portcfg.add(new PortConfig().withTargetPort(9714).withPublishMode(PortConfig.PublishMode.ingress));
        end.withPorts(portcfg);
        spec.withEndpointSpec(end);

        return docker.createServiceCmd(spec).exec().getId();
    }

    private int extractServicePortMapping(String serviceId) throws InterruptedException {
        Thread.sleep(1000);
        var service = _dockerClient.docker().inspectServiceCmd(serviceId).exec();
        Endpoint end = service.getEndpoint();
        for (PortConfig p : end.getPorts()) {
            return p.getPublishedPort();
        }
        return -1;
    }

    private void rmService(String serviceId) {
        _dockerClient.docker().removeServiceCmd(serviceId).withServiceId(serviceId).exec();
    }

    private void waitForContainerResponsive(String containerURL, int timeoutMs) throws PipelineComponentException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        int attempt = 0;
        while (System.currentTimeMillis() < deadline) {
            attempt++;
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(containerURL + "/v1/documentation"))
                        .version(HttpClient.Version.HTTP_1_1)
                        .timeout(Duration.ofSeconds(5))
                        .GET()
                        .build();
                HttpResponse<Void> resp = _client.send(req, HttpResponse.BodyHandlers.discarding());
                if (resp.statusCode() == 200) {
                    System.out.printf("[SwarmDriver][V2] Service %s responsive after %d attempt(s)%n",
                            containerURL, attempt);
                    return;
                }
            } catch (Exception ignored) { }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new PipelineComponentException(
                        format("Interrupted while waiting for service %s to become responsive", containerURL), e);
            }
        }
        throw new PipelineComponentException(
                format("Service %s did not become responsive within %d ms", containerURL, timeoutMs));
    }

    private static class ComponentInstance implements IDUUIUrlAccessible {
        String _host;
        IDUUIConnectionHandler _handler;
        IDUUICommunicationLayer _communication_layer;

        public ComponentInstance(String url, IDUUICommunicationLayer layer) {
            _host = url;
            _communication_layer = layer;
        }

        public IDUUICommunicationLayer getCommunicationLayer() {
            return _communication_layer;
        }

        public ComponentInstance(String url, IDUUICommunicationLayer layer, IDUUIConnectionHandler handler) {
            _host = url;
            _communication_layer = layer;
            _handler = handler;
        }

        public String generateURL() {
            return _host;
        }

        public IDUUIConnectionHandler getHandler() {
            return _handler;
        }
    }

    private static class InstantiatedComponent implements IDUUIInstantiatedPipelineComponent {
        private final String _image_name;
        private String _service_id;
        private int _service_port;
        private final Boolean _keep_runnging_after_exit;
        private final int _scale;
        private final String _fromLocalImage;
        private final ConcurrentLinkedQueue<ComponentInstance> _components;
        private final boolean _websocket;
        private final int _ws_elements;

        private final List<String> _constraints = new ArrayList<>(0);

        private final String _reg_password;
        private final String _reg_username;
        private final Map<String, String> _parameters;
        private String _sourceView;
        private String _targetView;
        private DUUIPipelineComponent _component;
        private String sHost = "localhost";
        private final String _uniqueComponentKey;


        InstantiatedComponent(DUUIPipelineComponent comp, String uniqueComponentKey) {
            _component = comp;
            _uniqueComponentKey = uniqueComponentKey;
            _image_name = comp.getDockerImageName();
            if (_image_name == null) {
                throw new InvalidParameterException("The image name was not set! This is mandatory for the DockerLocalDriver Class.");
            }

            _parameters = comp.getParameters();
            _targetView = comp.getTargetView();
            _sourceView = comp.getSourceView();
            _scale = comp.getScale(1);
            _constraints.addAll(comp.getConstraints());
            _components = new ConcurrentLinkedQueue<>();

            _keep_runnging_after_exit = comp.getDockerRunAfterExit(false);

            _fromLocalImage = null;
            _reg_password = comp.getDockerAuthPassword();
            _reg_username = comp.getDockerAuthUsername();

            _websocket = comp.isWebsocket();
            _ws_elements = comp.getWebsocketElements();
        }


        public IDUUIInstantiatedPipelineComponent withHost(String sHost) {
            this.sHost = sHost;
            return this;
        }

        public String getHost() {
            return this.sHost;
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

        public boolean isBackedByLocalImage() {
            return _fromLocalImage != null;
        }

        public String getLocalImageName() {
            return _fromLocalImage;
        }

        public boolean isWebsocket() {
            return _websocket;
        }

        public int getWebsocketElements() {
            return _ws_elements;
        }


        public InstantiatedComponent initialise(String service_id, int container_port, IDUUICommunicationLayer layer, DUUISwarmDriver swarmDriver) throws IOException, InterruptedException {

            _service_id = service_id;
            _service_port = container_port;

            if (_websocket) {
                swarmDriver._wsclient = new DUUIWebsocketAlt(
                    getServiceUrl().replaceFirst("http", "ws") + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS_WEBSOCKET, _ws_elements);
            } else {
                swarmDriver._wsclient = null;
            }
            for (int i = 0; i < _scale; i++) {
                _components.add(new ComponentInstance(getServiceUrl(), layer.copy(), swarmDriver._wsclient));

            }
            return this;
        }

        public String getServiceUrl() {
            return format("http://" + getHost() + ":%d", _service_port);
        }


        public String getImageName() {
            return _image_name;
        }

        public String getServiceId() {
            return _service_id;
        }

        public int getServicePort() {
            return _service_port;
        }

        public int getScale() {
            return _scale;
        }

        public List<String> getConstraints() {
            return _constraints;
        }

        public boolean getRunningAfterExit() {
            return _keep_runnging_after_exit;
        }

        public Map<String, String> getParameters() {
            return _parameters;
        }

        public String getSourceView() {return _sourceView; }

        public String getTargetView() {return _targetView; }

        public Triplet<IDUUIUrlAccessible, Long, Long> getComponent() {
            long mutexStart = System.nanoTime();
            ComponentInstance inst = _components.poll();
            while (inst == null) {
                inst = _components.poll();
            }
            long mutexEnd = System.nanoTime();
            return Triplet.with(inst, mutexStart, mutexEnd);
        }

        public void addComponent(IDUUIUrlAccessible item) {
            _components.add((ComponentInstance) item);
        }
    }

    public static class Component {
        private DUUIPipelineComponent component;

        public Component(String globalRegistryImageName) throws URISyntaxException, IOException {
            component = new DUUIPipelineComponent();
            component.withDockerImageName(globalRegistryImageName);
        }

        public Component(DUUIPipelineComponent pComponent) {
            component = pComponent;
        }

        public Component withDescription(String description) {
            component.withDescription(description);
            return this;
        }

        public Component withParameter(String key, String value) {
            component.withParameter(key, value);
            return this;
        }

        public Component withView(String viewName) {
            component.withView(viewName);
            return this;
        }

        public Component withSourceView(String viewName) {
            component.withSourceView(viewName);
            return this;
        }

        public Component withTargetView(String viewName) {
            component.withTargetView(viewName);
            return this;
        }

        public Component withScale(int scale) {
            component.withScale(scale);
            return this;
        }

        public Component withSegmentationStrategy(DUUISegmentationStrategy strategy) {
            component.withSegmentationStrategy(strategy);
            return this;
        }

        public <T extends DUUISegmentationStrategy> Component withSegmentationStrategy(Class<T> strategyClass) throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
            component.withSegmentationStrategy(strategyClass.getDeclaredConstructor().newInstance());
            return this;
        }

        public Component withConstraintHost(String sHost) {
            component.withConstraint("node.hostname==" + sHost);
            return this;
        }

        public Component withConstraintLabel(String sKey, String sValue) {
            component.withConstraint("node.labels." + sKey + "==" + sValue);
            return this;
        }

        public Component withConstraints(List<String> constraints) {
            component.withConstraints(constraints);
            return this;
        }

        public DUUISwarmDriver.Component withRegistryAuth(String username, String password) {
            component.withDockerAuth(username, password);
            return this;
        }


        public Component withRunningAfterDestroy(boolean run) {
            component.withDockerRunAfterExit(run);
            return this;
        }

        public Component withWebsocket(boolean b) {
            component.withWebsocket(b);
            return this;
        }

        public Component withWebsocket(boolean b, int elements) {
            component.withWebsocket(b, elements);
            return this;
        }

        public DUUIPipelineComponent build() {
            component.withDriver(DUUISwarmDriver.class);
            return component;
        }
    }
}
