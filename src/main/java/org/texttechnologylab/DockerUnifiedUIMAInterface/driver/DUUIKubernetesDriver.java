package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;


import java.io.IOException;
import static java.lang.String.format;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.uima.jcas.JCas;
import org.apache.uima.util.InvalidXMLException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIDockerInterface;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRestDriver.IDUUIInstantiatedRestComponent;
import org.xml.sax.SAXException;

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;
import io.fabric8.kubernetes.api.model.NodeSelectorTerm;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import static io.fabric8.kubernetes.client.impl.KubernetesClientImpl.logger;

/**
 * Driver for the running of components in Kubernetes
 *
 * @author Markos Genios, Filip Fitzermann
 */
public class DUUIKubernetesDriver extends DUUIRestDriver<DUUIKubernetesDriver, DUUIKubernetesDriver.InstantiatedComponent> {

    private final KubernetesClient _kube_client;

    private final HashMap<String, InstantiatedComponent> _active_components;
    private final DUUIDockerInterface _interface;
    private final HttpClient _client;

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
        _kube_client = new KubernetesClientBuilder().build();

        _interface = new DUUIDockerInterface();

        _timeout = Duration.ofMillis(1_000);
        _client = HttpClient.newHttpClient();

        _active_components = new HashMap<>();
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
    protected Map<String, InstantiatedComponent> getActiveComponents() {
        return _active_components;
    }

    @Override
    public boolean canAccept(DUUIPipelineComponent component) throws InvalidXMLException, IOException, SAXException {
        return component.getDockerImageName() != null;
    }

    /**
     * Creates Deployment for the kubernetes cluster.
     *
     * @param name:     Name of the deployment
     * @param image:    Image that the pods are running
     * @param replicas: number of pods (or more general: threads) to be created
     * @param labels:   Use only gpu-servers with the specified labels.
     * @author Markos Genios
     */
    public static void createDeployment(String name, String image, int replicas, List<String> labels) {

        if (labels.isEmpty()) {
            labels = List.of("disktype=all");
            System.out.println("(KubernetesDriver) defaulting to label disktype=all");
        }

        List<NodeSelectorTerm> terms = getNodeSelectorTerms(labels);

//        Map tMap = new HashMap();
//        tMap.put("vke.volcengine.com/container-multiple-gpu", "1");

        try (KubernetesClient k8s = new KubernetesClientBuilder().build()) {
            // Load Deployment YAML Manifest into Java object
            Deployment deployment;
            deployment = new DeploymentBuilder()
                .withNewMetadata()
                .withName(name)
                .endMetadata()
                .withNewSpec()
                .withReplicas(replicas)
                .withNewTemplate()
                .withNewMetadata()
                .addToLabels("pipeline-uid", name)
                .endMetadata()
                .withNewSpec()
                .addNewContainer()
                .withName(name)
                .withImage(image)
                .addNewPort()
                    .withContainerPort(_port)
                .endPort()
                .endContainer()
                .withNewAffinity()
                .withNewNodeAffinity()
                .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                .addAllToNodeSelectorTerms(terms)
                .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .endAffinity()
                .endSpec()

                .endTemplate()
                .withNewSelector()
                .addToMatchLabels("pipeline-uid", name)
                .endSelector()
                .endSpec()
                .build();

            deployment = k8s.apps().deployments().inNamespace(sNamespace).resource(deployment).create();
        }
    }

    /**
     * Creates Service for kubernetes cluster which is matched by selector labels to the previously created deployment.
     *
     * @param name
     * @return
     */
    public static Service createService(String name) {
        try (KubernetesClient client = new KubernetesClientBuilder().build()) {
            String namespace = Optional.ofNullable(client.getNamespace()).orElse(sNamespace);
            Service service = new ServiceBuilder()
                .withNewMetadata()
                .withName(name)
                .endMetadata()
                .withNewSpec()
                .withSelector(Collections.singletonMap("pipeline-uid", name))  // Has to match the label of the deployment.
                .addNewPort()
                .withName("k-port")
                .withProtocol("TCP")
                    .withPort(_port)
                .withTargetPort(new IntOrString(9714))
                .endPort()
                .withType("LoadBalancer")
                .endSpec()
                .build();

            service = client.services().inNamespace(namespace).resource(service).create();
            logger.info("Created service with name {}", service.getMetadata().getName());

            String serviceURL = client.services().inNamespace(namespace).withName(service.getMetadata().getName())
                .getURL("k-port");
            logger.info("Service URL {}", serviceURL);

            return service;
        }
    }

    /**
     * Creates a list of NodeSelectorTerms from a list of labels. If added to a deployment the pods are scheduled onto
     * any node that has one or multiple of the given labels.
     * Each label must be given in the format "key=value".
     *
     * @param rawLabels
     * @return {@code List<NodeSelectorTerm>}
     */
    public static List<NodeSelectorTerm> getNodeSelectorTerms(List<String> rawLabels) {
        List<NodeSelectorTerm> terms = new ArrayList<>();

//        Splits each label in string form at the "=" and adds the resulting strings into a new
//        NodeSelectorTerm as key value pairs.
        for (String rawLabel : rawLabels) {
            String[] l = rawLabel.split("=");
            NodeSelectorTerm term = new NodeSelectorTerm();
            NodeSelectorRequirement requirement = new NodeSelectorRequirement(l[0], "In", List.of(l[1]));
            term.setMatchExpressions(List.of(requirement));
            terms.add(term);
        }
        return terms;
    }

    /**
     * Checks, whether the used Server is the master-node of kubernetes cluster.
     * Note: Function can give false-negative results, therefore is not used in the working code.
     *
     * @param kubeClient
     * @return
     * @throws SocketException
     * @author Markos Genios
     */
    public boolean isMasterNode(KubernetesClient kubeClient) throws SocketException {
        String masterNodeIP = kubeClient.getMasterUrl().getHost();  // IP-Adresse des Master Node
        Enumeration<NetworkInterface> networkInterfaceEnumeration = NetworkInterface.getNetworkInterfaces();
        // Source of code snippet: https://www.educative.io/answers/how-to-get-the-ip-address-of-a-localhost-in-java
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

    /**
     * Creates Deployment and Service. Puts the new component, which includes the Pods with their image to the active components.
     *
     * @param component
     * @param jc
     * @param skipVerification
     * @param shutdown
     * @return
     * @throws Exception
     * @author Markos Genios
     */
    @Override
    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification, AtomicBoolean shutdown) throws Exception {
        String uuid = UUID.randomUUID().toString();  // Erstelle ID für die neue Komponente.
        while (_active_components.containsKey(uuid.toString())) {  // Stelle sicher, dass ID nicht bereits existiert (?)
            uuid = UUID.randomUUID().toString();
        }
        InstantiatedComponent comp = new InstantiatedComponent(component, uuid);  // Initialisiere Komponente

        String dockerImage = comp.getImageName();  // Image der Komponente als String
        int scale = comp.getScale(); // Anzahl der Replicas in dieser Kubernetes-Komponente

        Service service;
        try {
            /**
             * Add "a" in front of the name, because according to the kubernetes-rules the names must start
             * with alphabetical character (must not start with digit)
             */
            createDeployment("a" + uuid, dockerImage, scale + getScaleBuffer(), comp.getLabels());  // Erstelle Deployment
            service = createService("a" + uuid);  // Erstelle service und gebe diesen zurück
        } catch (Exception e) {
            deleteDeployment("a" + uuid);
            deleteService("a" + uuid);
            throw e;
        }
        if (shutdown.get()) return null;

        int port = service.getSpec().getPorts().get(0).getNodePort();  // NodePort
//            service.getSpec().getPorts().forEach(p->{
//                System.out.println("Node-port: "+p.getNodePort());
//                System.out.println("Port: "+p.getPort());
//                System.out.println("Target-port: "+p.getTargetPort());
//            });
        final String uuidCopy = uuid;
        IDUUICommunicationLayer layer = null;
        
        String prefix = String.format("[DUUIKubernetesDriver][%s][Replicas %d]", uuidCopy.substring(0, 5) + "...", comp.getScale());
        String kubeUrl = _interface.getHostUrl(port);

        try {



            DUUICommunicationLayerRequestContext requestContext = new DUUICommunicationLayerRequestContext(
                kubeUrl,
                jc,
                _timeout,
                _client,
                _luaContext,
                skipVerification,
                prefix
            );

            System.out.println("Port " + port);
            layer = get_communication_layer(requestContext);

        } catch (Exception e) {
            deleteDeployment("a" + uuid);
            deleteService("a" + uuid);
            throw e;
        }


        System.out.printf("%s Service for image %s is online (URL %s) and seems to understand DUUI V1 format!\n", 
            prefix, comp.getImageName(), kubeUrl
        );

        comp.initialise(port, layer, this);
        Thread.sleep(500);

        _active_components.put(uuid, comp);
        return shutdown.get() ? null : uuid;
    }


    /**
     * Deletes the Deployment from the kubernetes cluster.
     *
     * @author Markos Genios
     */
    public static void deleteDeployment(String name) {
        try (KubernetesClient k8s = new KubernetesClientBuilder().build()) {
            // Argument namespace could be generalized.
            k8s.apps().deployments().inNamespace(sNamespace)
                .withName(name)
                .delete();
        }
    }

    /**
     * Deletes the service from the kubernetes cluster.
     *
     * @author Markos Genios
     */
    public static void deleteService(String name) {
        try (KubernetesClient client = new DefaultKubernetesClient()) {
            // Argument namespace could be generalized.
            client.services().inNamespace(sNamespace).withName(name).delete();
        }
    }

    /**
     * Deletes both the deployment and the service from the kubernetes cluster, if they exist.
     *
     * @param uuid
     * @author Markos Genios
     */
    @Override
    public boolean destroy(String uuid) {
        InstantiatedComponent comp = _active_components.remove(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        if (!comp.getRunningAfterExit()) {
            deleteDeployment("a" + uuid);
            deleteService("a" + uuid);
        }

        return true;
    }


    @Override
    public void shutdown() {

    }

    /**
     * Class to represent a kubernetes pod: An Instance to process an entire document.
     *
     * @author Markos Genios
     */
    private static record ComponentInstance(
            String _container_id,
            String _pod_ip,
            IDUUICommunicationLayer _communicationLayer
    ) implements IDUUIUrlAccessible {
        @Override
        public String getUniqueInstanceKey() {
            return _container_id;
        }

        @Override
        public String generateURL() {
            return _pod_ip;
        }
        
        @Override
        public IDUUICommunicationLayer getCommunicationLayer() {
            return _communicationLayer;
        }
    }

    protected static class InstantiatedComponent extends IDUUIInstantiatedRestComponent<InstantiatedComponent> {

        private String _image_name;
        private int _service_port;

        private boolean _gpu;
        private boolean _keep_running_after_exit;
        private boolean _withImageFetching;

        private List<String> _labels;

        InstantiatedComponent(DUUIPipelineComponent component, String uniqueComponentKey) {
            super(component, uniqueComponentKey);

            _image_name = component.getDockerImageName();
            if (_image_name == null) {
                throw new InvalidParameterException("The image name was not set! This is mandatory for the DockerLocalDriver Class.");
            }
            
            _withImageFetching = component.getDockerImageFetching(false);
            _gpu = component.getDockerGPU(false);
            _keep_running_after_exit = component.getDockerRunAfterExit(false);

            
            _labels = component.getConstraints();
        }

        public InstantiatedComponent initialise(int service_port, IDUUICommunicationLayer layer, DUUIKubernetesDriver kubeDriver) throws IOException, InterruptedException {
            _service_port = service_port;

            for (int i = 0; i < _scale; i++) {
                String _container_id = UUID.randomUUID().toString();
                _components.add(new ComponentInstance(_container_id, getServiceUrl(), layer.copy()));

            }
            return this;
        }

        /**
         * @return Url of the kubernetes-service.
         */
        public String getServiceUrl() {
            return format("http://localhost:%d", _service_port);
        }

        public boolean getRunningAfterExit() {
            return _keep_running_after_exit;
        }

        /**
         * @return name of the image.
         */
        public String getImageName() {
            return _image_name;
        }

        /**
         * sets the service port.
         *
         * @param servicePort
         */
        public void set_service_port(int servicePort) {
            this._service_port = servicePort;
        }

        /**
         * returns true, iff only gpu-servers are used.
         *
         * @return
         */
        public boolean getGPU() {
            return _gpu;
        }

        /**
         * Returns the labels, to which the pods must be assigned.
         *
         * @return
         */
        public List<String> getLabels() {
            return _labels;
        }
    }

    /**
     * Instance of this class is input to composer.add-method and is added to the _Pipeline-attribute of the composer.
     *
     * @author Markos Genios
     */
    public static class Component extends IDUUIDriverInterface.ComponentBuilder<Component> {

        /**
         * Constructor. Creates Instance of Class Component.
         *
         * @param globalRegistryImageName
         * @throws URISyntaxException
         * @throws IOException
         */
        public Component(String globalRegistryImageName) throws URISyntaxException, IOException {
            super(new DUUIPipelineComponent());
            _component.withDockerImageName(globalRegistryImageName);
        }

        /**
         * If used, the Pods get assigned only to GPU-Servers with the specified label.
         *
         * @return
         */
        public Component withLabels(String... labels) {  // Can be extended to "String..." to use more than one label!
            _component.withConstraints(List.of(labels));
            return this;
        }

        public Component withLabels(List<String> labels) {  // Can be extended to "String..." to use more than one label!
            _component.withConstraints(labels);
            return this;
        }

        /**
         * Builds the component.
         *
         * @return
         */
        public DUUIPipelineComponent build() {
            _component.withDriver(DUUIKubernetesDriver.class);
            return _component;
        }
    }
}
