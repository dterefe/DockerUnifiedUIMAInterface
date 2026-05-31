package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;


import io.fabric8.kubernetes.api.model.NodeSelectorTerm;
import io.fabric8.kubernetes.api.model.Service;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.InvalidXMLException;
import org.javatuples.Triplet;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.duui.clients.docker.DUUIDockerClient;
import org.texttechnologylab.duui.clients.kubernetes.DUUIKubernetesClient;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.DUUIWebsocketAlt;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.CommunicationLayerException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.ImagePullException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.PipelineComponentException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.duui.clients.http.DUUIHttpEndpoint;
import org.texttechnologylab.duui.clients.http.IDUUIEndpoint;
import org.texttechnologylab.duui.pipeline.component.DUUIComponent;
import org.texttechnologylab.duui.pipeline.component.DUUINode;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Annotator;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Config;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
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
import java.util.logging.Logger;

import static java.lang.String.format;

/**
 * Driver for the running of components in Kubernetes.
 * Now uses {@link DUUIKubernetesClient} for all Kubernetes API operations
 * instead of raw fabric8 API calls.
 *
 * @author Markos Genios, Filip Fitzermann
 */
public class DUUIKubernetesDriver extends DUUIV1Driver {

    private static final Logger LOGGER = Logger.getLogger(DUUIKubernetesDriver.class.getName());

    private final DUUIKubernetesClient _k8s;
    private final DUUIDockerClient _dockerClient;

    private IDUUIConnectionHandler _wsclient;

    private int iScaleBuffer = 0;

    private static int _port = 9715;
    private static String sNamespace = "default";

    /**
     * Constructor.
     *
     * @throws IOException
     * @author Markos Genios
     */
    public DUUIKubernetesDriver() throws IOException {
        super();
        _k8s = new DUUIKubernetesClient();
        _dockerClient = new DUUIDockerClient();
        _containerTimeout = 1000;
        _activeComponents = new HashMap<>();
    }

    /**
     * Returns the underlying {@link DUUIKubernetesClient} for advanced operations.
     */
    public DUUIKubernetesClient k8s() {
        return _k8s;
    }

    public DUUIKubernetesDriver withScaleBuffer(int iValue) {
        this.iScaleBuffer = iValue;
        return this;
    }

    public DUUIKubernetesDriver withScaleBuffer() {
        this.iScaleBuffer = 1;
        return this;
    }

    public int getScaleBuffer() {
        return this.iScaleBuffer;
    }

    @Override
    public boolean canAccept(DUUIPipelineComponent component) throws InvalidXMLException, IOException, SAXException {
        return component.getDockerImageName() != null;
    }

    // ── Deployment / Service lifecycle (instance methods, delegated to DUUIKubernetesClient) ──

    /**
     * Creates a Deployment in the Kubernetes cluster.
     *
     * @param name     Name of the deployment
     * @param image    Image that the pods are running
     * @param replicas number of pods (or more general: threads) to be created
     * @param labels   Use only servers with the specified labels.
     */
    private void createDeployment(String name, String image, int replicas, List<String> labels) {
        List<String> effectiveLabels = (labels == null || labels.isEmpty())
                ? List.of("disktype=all")
                : labels;
        if (labels == null || labels.isEmpty()) {
            System.out.println("(KubernetesDriver) defaulting to label disktype=all");
        }

        _k8s.deployment(sNamespace, name)
                .create(image, replicas, _port, effectiveLabels);
    }

    /**
     * Creates a LoadBalancer Service for the Kubernetes cluster, matched by selector labels
     * to the previously created deployment.
     *
     * @param name the service/deployment name
     * @return the created fabric8 {@link Service} for port extraction
     */
    private Service createService(String name) {
        _k8s.service(sNamespace, name)
                .createLoadBalancer(
                        Collections.singletonMap("pipeline-uid", name),
                        "k-port",
                        _port,
                        9714);

        io.fabric8.kubernetes.api.model.Service svc = _k8s.service(sNamespace, name).inspect();
        LOGGER.info(() -> "Created service with name " + (svc != null ? svc.getMetadata().getName() : name));

        String serviceURL = _k8s.service(sNamespace, name).getEndpointUrl("k-port");
        LOGGER.info(() -> "Service URL " + serviceURL);

        return svc;
    }

    /**
     * Deletes the Deployment from the Kubernetes cluster.
     */
    private void deleteDeployment(String name) {
        _k8s.deployment(sNamespace, name).delete();
    }

    /**
     * Deletes the Service from the Kubernetes cluster.
     */
    private void deleteService(String name) {
        _k8s.service(sNamespace, name).delete();
    }

    /**
     * Creates a list of NodeSelectorTerms from a list of labels. Delegates to
     * {@link DUUIKubernetesClient#nodeSelectorTerms(List)}.
     */
    public static List<NodeSelectorTerm> getNodeSelectorTerms(List<String> rawLabels) {
        return DUUIKubernetesClient.nodeSelectorTerms(rawLabels);
    }

    /**
     * Checks whether the used server is the master-node of the Kubernetes cluster.
     * Note: Function can give false-negative results, therefore is not used in the working code.
     *
     * @throws SocketException
     * @author Markos Genios
     */
    public boolean isMasterNode() throws SocketException {
        String masterNodeIP = _k8s.k8s().getMasterUrl().getHost();
        Enumeration<NetworkInterface> networkInterfaceEnumeration = NetworkInterface.getNetworkInterfaces();
        while (networkInterfaceEnumeration.hasMoreElements()) {
            for (InterfaceAddress interfaceAddress : networkInterfaceEnumeration.nextElement().getInterfaceAddresses()) {
                if (interfaceAddress.getAddress().isSiteLocalAddress()) {
                    if (interfaceAddress.getAddress().getHostAddress().equals(masterNodeIP)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    // ── V1 lifecycle (legacy) ──────────────────────────────────────────

    /**
     * Creates Deployment and Service. Puts the new component, which includes the Pods
     * with their image to the active components.
     *
     * @author Markos Genios
     */
    @Override
    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification, AtomicBoolean shutdown) throws Exception {
        String uuid = UUID.randomUUID().toString();
        while (_activeComponents.containsKey(uuid.toString())) {
            uuid = UUID.randomUUID().toString();
        }
        InstantiatedComponent comp = new InstantiatedComponent(component, uuid);

        String dockerImage = comp.getImageName();
        int scale = comp.getScale();

        Service service;
        try {
            // Add "a" prefix — Kubernetes names must start with an alphabetic character
            createDeployment("a" + uuid, dockerImage, scale + getScaleBuffer(), comp.getLabels());
            service = createService("a" + uuid);
        } catch (Exception e) {
            deleteDeployment("a" + uuid);
            deleteService("a" + uuid);
            throw e;
        }
        if (shutdown.get()) return null;

        int port = service.getSpec().getPorts().get(0).getNodePort();
        final String uuidCopy = uuid;
        IDUUICommunicationLayer layer = null;

        try {
            System.out.println("Port " + port);
            layer = DUUIDockerDriver.responsiveAfterTime("http://localhost:" + port, jc, _containerTimeout, _client, (msg) -> {
                System.out.printf("[KubernetesDriver][%s][%d Replicas] %s\n", uuidCopy, comp.getScale(), msg);
            }, _luaContext, skipVerification);
        } catch (Exception e) {
            deleteDeployment("a" + uuid);
            deleteService("a" + uuid);
            throw e;
        }

        System.out.printf("[KubernetesDriver][%s][%d Replicas] Service for image %s is online (URL http://localhost:%d) and seems to understand DUUI V1 format!\n", uuid, comp.getScale(), comp.getImageName(), port);

        comp.initialise(port, layer, this);
        Thread.sleep(500);

        _activeComponents.put(uuid, comp);
        return shutdown.get() ? null : uuid;
    }

    public List<String> getEndpointUrls(String uuid) {
        InstantiatedComponent comp = (InstantiatedComponent) _activeComponents.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        return comp._components.stream()
                .map(ComponentInstance::generateURL)
                .distinct()
                .toList();
    }

    @Override
    public void run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf, DUUIComposer composer) throws CASException, PipelineComponentException, CompressorException, IOException, InterruptedException, SAXException, CommunicationLayerException {
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
     * Deletes both the deployment and the service from the Kubernetes cluster, if they exist.
     *
     * @author Markos Genios
     */
    @Override
    public boolean destroy(String uuid) {
        InstantiatedComponent comp = (InstantiatedComponent) _activeComponents.remove(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        if (!comp.getRunningAfterExit()) {
            deleteDeployment("a" + uuid);
            deleteService("a" + uuid);
        }

        return true;
    }

    // ── V2 instantiation ───────────────────────────────────────────────

    /**
     * V2 instantiation for Kubernetes driver.
     * <p>
     * Creates a Kubernetes deployment and service, then wraps each replica as a
     * {@link DUUIV1Annotator}. All annotators point to the same LoadBalancer
     * service URL; the K8s control plane distributes traffic across pods.
     *
     * @param component        the pipeline component describing the container image and configuration
     * @param jc               a JCas for type system baseline (used only if verification is not skipped)
     * @param skipVerification if {@code true}, skip the pre-verification round-trip
     * @param shutdown         cooperative shutdown flag; checked between container starts
     * @return a fully initialized {@link DUUIComponent}{@code <JCas>} ready for processing
     * @throws Exception if image pull fails, deployment fails, or annotator initialization fails
     */
    @Override
    public DUUIComponent<JCas> instantiateV2(DUUIPipelineComponent component, JCas jc, boolean skipVerification,
            AtomicBoolean shutdown) throws Exception {

        String imageName = component.getDockerImageName();
        if (imageName == null) {
            throw new InvalidParameterException(
                    "The image name was not set! This is mandatory for the DUUIKubernetesDriver Class.");
        }

        // --- 1. Pull/verify image ---
        if (component.getDockerImageFetching(false)) {
            if (component.getDockerAuthUsername() != null) {
                System.out.printf("[KubernetesDriver][V2] Attempting image %s download from secure remote registry%n",
                        imageName);
            }
            try {
                pullDockerImage(imageName, component.getDockerAuthUsername(),
                        component.getDockerAuthPassword());
            } catch (RuntimeException e) {
                System.err.printf("[KubernetesDriver][V2] Failed to pull image %s: %s%n", imageName, e.getMessage());
                throw new PipelineComponentException(
                        format("Failed to pull docker image %s", imageName), e);
            }
            if (shutdown.get()) {
                return null;
            }
            System.out.printf("[KubernetesDriver][V2] Pulled image %s%n", imageName);
        }

        // Pin image to a digest-based name so subsequent runs use the exact same image
        String digest = getDockerImageDigest(imageName);
        component.__internalPinDockerImage(imageName, digest);
        System.out.printf("[KubernetesDriver][V2] Transformed image %s to pinnable name %s%n",
                imageName, component.getDockerImageName());

        int scale = component.getScale(1);
        int workers = component.getWorkers(1);
        String componentId = component.getName() != null ? component.getName() : "kubernetes-component";
        boolean runAfterExit = component.getDockerRunAfterExit(false);

        String uuid = UUID.randomUUID().toString();

        // --- 2. Create Kubernetes deployment and service ---
        List<String> labels = component.getConstraints();
        try {
            createDeployment("a" + uuid, digest, scale, labels);
        } catch (Exception e) {
            throw new PipelineComponentException(
                    format("Failed to create Kubernetes deployment for %s", imageName), e);
        }
        Service service;
        try {
            service = createService("a" + uuid);
        } catch (Exception e) {
            deleteDeployment("a" + uuid);
            throw new PipelineComponentException(
                    format("Failed to create Kubernetes service for %s", imageName), e);
        }

        if (shutdown.get()) {
            deleteService("a" + uuid);
            deleteDeployment("a" + uuid);
            return null;
        }

        int port = service.getSpec().getPorts().get(0).getNodePort();
        String serviceURL = "http://localhost:" + port;

        System.out.printf("[KubernetesDriver][V2][%s][%d Replicas] Service for image %s is online (URL %s), waiting"
                + " for responsiveness...%n", uuid, scale, imageName, serviceURL);

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

            System.out.printf("[KubernetesDriver][V2][Replica %d/%d] Annotator %s ready (URL %s)%n",
                    replicaIdx + 1, scale, replicaId, serviceURL);
        }

        // --- 5. Build DUUIComponent with nodes distributed round-robin ---
        List<DUUINode<JCas>> nodes = new ArrayList<>(scale * workers);
        int slot = 0;
        for (DUUIV1Annotator annotator : annotators) {
            int concurrency = annotator.config().concurrency();
            for (int j = 0; j < concurrency; j++) {
                nodes.add(DUUINode.v1(componentId + "-slot-" + slot++, annotator));
            }
        }

        // closeAction deletes the deployment and service unless runAfterExit is set
        AutoCloseable closeAction = () -> {
            if (!runAfterExit) {
                System.out.printf("[KubernetesDriver][V2] Deleting deployment and service for component %s...%n",
                        componentId);
                deleteDeployment("a" + uuid);
                deleteService("a" + uuid);
            }
        };

        System.out.printf("[KubernetesDriver][V2] Component %s instantiated with %d nodes across %d replica(s)%n",
                componentId, nodes.size(), scale);

        return new DUUIComponent<>(componentId, nodes, closeAction);
    }

    // ── Helpers ────────────────────────────────────────────────────────

    /**
     * Waits for a container/service to become responsive by polling its {@code /v1/documentation} endpoint.
     */
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
                    System.out.printf("[KubernetesDriver][V2] Service %s responsive after %d attempt(s)%n",
                            containerURL, attempt);
                    return;
                }
            } catch (Exception ignored) {
                // Service not ready yet — retry after a short sleep
            }
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

    private void pullDockerImage(String tag, String username, String password) {
        try {
            if (username != null && password != null) {
                _dockerClient.registry(username, password).pull(tag);
            } else {
                _dockerClient.registry().pull(tag);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to pull docker image " + tag, e);
        }
    }

    private String getDockerImageDigest(String imageName) {
        if (!imageName.contains(":")) imageName = imageName + ":latest";
        try {
            var digests = _dockerClient.image(imageName).digests();
            return digests.isEmpty() ? null : digests.get(0);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void shutdown() {
        _k8s.shutdown();
        super.shutdown();
    }

    // ── Inner classes ──────────────────────────────────────────────────

    /**
     * Class to represent a Kubernetes pod: An Instance to process an entire document.
     *
     * @author Markos Genios
     */
    public static class ComponentInstance implements IDUUIUrlAccessible {
        private String _pod_ip;
        private IDUUIConnectionHandler _handler;
        private IDUUICommunicationLayer _communicationLayer;

        public ComponentInstance(String pod_ip, IDUUICommunicationLayer communicationLayer) {
            _pod_ip = pod_ip;
            _communicationLayer = communicationLayer;
        }

        public IDUUICommunicationLayer getCommunicationLayer() {
            return _communicationLayer;
        }

        public ComponentInstance(String pod_ip, IDUUICommunicationLayer layer, IDUUIConnectionHandler handler) {
            _pod_ip = pod_ip;
            _communicationLayer = layer;
            _handler = handler;
        }

        @Override
        public String generateURL() {
            return _pod_ip;
        }

        public IDUUIConnectionHandler getHandler() {
            return _handler;
        }
    }

    static class InstantiatedComponent implements IDUUIInstantiatedPipelineComponent {

        private String _image_name;
        private int _service_port;
        private boolean _gpu;
        private final ConcurrentLinkedQueue<ComponentInstance> _components;
        private boolean _keep_running_after_exit;
        private int _scale;
        private boolean _withImageFetching;
        private Map<String, String> _parameters;
        private String _sourceView;
        private String _targetView;
        private DUUIPipelineComponent _component;

        private final boolean _websocket;

        private int _ws_elements;
        private List<String> _labels;
        private String _uniqueComponentKey = "";

        InstantiatedComponent(DUUIPipelineComponent comp, String uniqueComponentKey) {
            _component = comp;
            _uniqueComponentKey = uniqueComponentKey;
            _image_name = comp.getDockerImageName();
            _parameters = comp.getParameters();
            _targetView = comp.getTargetView();
            _sourceView = comp.getSourceView();
            if (_image_name == null) {
                throw new InvalidParameterException("The image name was not set! This is mandatory for the DockerLocalDriver Class.");
            }
            _withImageFetching = comp.getDockerImageFetching(false);

            _scale = comp.getScale(1);

            _gpu = comp.getDockerGPU(false);

            _labels = comp.getConstraints();

            _keep_running_after_exit = comp.getDockerRunAfterExit(false);

            _components = new ConcurrentLinkedQueue<>();

            _ws_elements = comp.getWebsocketElements();

            _websocket = comp.isWebsocket();
        }

        public InstantiatedComponent initialise(int service_port, IDUUICommunicationLayer layer, DUUIKubernetesDriver kubeDriver) throws IOException, InterruptedException {
            _service_port = service_port;

            if (_websocket) {
                kubeDriver._wsclient = new DUUIWebsocketAlt(
                    getServiceUrl().replaceFirst("http", "ws") + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS_WEBSOCKET, _ws_elements);
            } else {
                kubeDriver._wsclient = null;
            }
            for (int i = 0; i < _scale; i++) {
                _components.add(new ComponentInstance(getServiceUrl(), layer.copy(), kubeDriver._wsclient));

            }
            return this;
        }

        public String getServiceUrl() {
            return format("http://localhost:%d", _service_port);
        }

        @Override
        public DUUIPipelineComponent getPipelineComponent() {
            return _component;
        }

        @Override
        public Triplet<IDUUIUrlAccessible, Long, Long> getComponent() {
            long mutexStart = System.nanoTime();
            ComponentInstance inst = _components.poll();
            while (inst == null) {
                inst = _components.poll();
            }
            long mutexEnd = System.nanoTime();
            return Triplet.with(inst, mutexStart, mutexEnd);
        }

        @Override
        public void addComponent(IDUUIUrlAccessible item) {
            _components.add((ComponentInstance) item);
        }

        @Override
        public Map<String, String> getParameters() {
            return _parameters;
        }

        public String getSourceView() {return _sourceView; }

        public String getTargetView() {return _targetView; }

        @Override
        public String getUniqueComponentKey() {
            return _uniqueComponentKey;
        }

        public boolean getRunningAfterExit() {
            return _keep_running_after_exit;
        }

        public String getImageName() {
            return _image_name;
        }

        public int getScale() {
            return _scale;
        }

        public void set_service_port(int servicePort) {
            this._service_port = servicePort;
        }

        public int getWebsocketElements() {
            return _ws_elements;
        }

        public boolean isWebsocket() {
            return _websocket;
        }

        public boolean getGPU() {
            return _gpu;
        }

        public List<String> getLabels() {
            return _labels;
        }
    }

    /**
     * Instance of this class is input to composer.add-method and is added to the _Pipeline-attribute of the composer.
     *
     * @author Markos Genios
     */
    public static class Component {
        private DUUIPipelineComponent _component;

        public Component(String globalRegistryImageName) throws URISyntaxException, IOException {
            _component = new DUUIPipelineComponent();
            _component.withDockerImageName(globalRegistryImageName);
        }

        public Component withLabels(String... labels) {
            _component.withConstraints(List.of(labels));
            return this;
        }

        public Component withLabels(List<String> labels) {
            _component.withConstraints(labels);
            return this;
        }

        public Component withScale(int scale) {
            _component.withScale(scale);
            return this;
        }

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

        public DUUIPipelineComponent build() {
            _component.withDriver(DUUIKubernetesDriver.class);
            return _component;
        }

        public Component withName(String name) {
            _component.withName(name);
            return this;
        }
    }
}
