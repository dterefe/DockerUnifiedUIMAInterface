package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;


import static java.lang.String.format;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.TypeSystemUtil;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIDockerInterface;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.ImagePullException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.PipelineComponentException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIContexts;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUILogger;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUILoggers;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIStatus;
import org.xml.sax.SAXException;

/**
 * Driver for the use of Docker
 *
 * @author Alexander Leonhardt
 */
public class DUUIDockerDriver extends DUUIRestDriver<DUUIDockerDriver, DUUIDockerDriver.InstantiatedComponent> {
    private DUUIDockerInterface _interface;
    private HttpClient _client;

    private HashMap<String, InstantiatedComponent> _active_components;

    private static final DUUILogger LOG = DUUILoggers.getLogger(DUUIDockerDriver.class);

    public DUUIDockerDriver() throws IOException, UIMAException, SAXException {
        _interface = new DUUIDockerInterface();
        _client = HttpClient.newHttpClient();
        _timeout = Duration.ofMillis(10_000);
        _luaContext = null;
        _active_components = new HashMap<>();

        JCas _basic = JCasFactory.createJCas();
        _basic.setDocumentLanguage("en");
        _basic.setDocumentText("Hello World!");


        TypeSystemDescription desc = TypeSystemUtil.typeSystem2TypeSystemDescription(_basic.getTypeSystem());
        StringWriter wr = new StringWriter();
        desc.toXML(wr);
    }

    @Override
    public DUUILogger logger() {
        return LOG;
    }

    /**
     * Constructor with built-in timeout
     *
     * @param timeout
     * @throws IOException
     * @throws UIMAException
     * @throws SAXException
     */
    public DUUIDockerDriver(int timeout) throws IOException {
        _interface = new DUUIDockerInterface();
        _client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(timeout)).build();
        
        _timeout = Duration.ofMillis(timeout);

        _active_components = new HashMap<>();
    }

    @Override
    protected Map<String, InstantiatedComponent> getActiveComponents() {
        return _active_components;
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
        while (_active_components.containsKey(uuid)) {
            uuid = UUID.randomUUID().toString();
        }

        InstantiatedComponent comp = new InstantiatedComponent(component, uuid);

        try(var ignored = logger().withContext(DUUIContexts.component(comp).status(DUUIStatus.INSTANTIATING)))  {
            // Inverted if check because images will never be pulled if !comp.getImageFetching() is checked.
            if (comp.getImageFetching()) {
                if (comp.getUsername() != null) {
                    logger().info("[DUUIDockerDriver] Attempting image %s download from secure remote registry %n", comp.getImageName());
                }
                try {
                    _interface.pullImage(comp.getImageName(), comp.getUsername(), comp.getPassword(), shutdown);
                } catch (ImagePullException imagePullException) {
                    logger().debug("[DUUIDockerDriver] Failed to pull image %s: %s%n", comp.getImageName(), imagePullException.getMessage());
                    throw new PipelineComponentException(
                            format("Failed to pull docker image %s", comp.getImageName()),
                            imagePullException
                    );
                }
                if (shutdown.get()) {
                    return null;
                }

                logger().info("[DUUIDockerDriver] Pulled image with id %s%n", comp.getImageName());
            } else {
    //            _interface.pullImage(comp.getImageName());
                if (!_interface.hasLocalImage(comp.getImageName())) {
                    throw new InvalidParameterException(format("Could not find local docker image \"%s\". Did you misspell it or forget with .withImageFetching() to fetch it from remote registry?", comp.getImageName()));
                }
            }
            logger().info("[DUUIDockerDriver] Assigned new pipeline component unique id %s%n", uuid);
            String digest = _interface.getDigestFromImage(comp.getImageName());
            comp.getPipelineComponent().__internalPinDockerImage(comp.getImageName(), digest);
            logger().info("[DUUIDockerDriver] Transformed image %s to pinnable image name %s%n", comp.getImageName(), comp.getPipelineComponent().getDockerImageName());

            _active_components.put(uuid, comp);

            for (int i = 0; i < comp.getScale(); i++) {
                if (shutdown.get()) {
                    return null;
                }

                String containerid = _interface.run(comp.getPipelineComponent().getDockerImageName(), comp.getEnv(), comp.usesGPU(), true, 9714, false);
                int port = _interface.extract_port_mapping(containerid);  // Dieser port hier ist im allgemeinen nicht (bzw nie) der Port 9714 aus dem Input.

                try {
                    if (port == 0) {
                        throw new UnknownError("Could not read the container port!");
                    }

                    String containerURL = _interface.getHostUrl(containerid, 9714);

                    final int iCopy = i + 1;
                    final String uuidCopy = uuid;
                    String prefix = String.format("[DUUIDockerDriver][%s][Replica %d/%d]", uuidCopy.substring(0, 5) + "...", iCopy, comp.getScale());

                    DUUICommunicationLayerRequestContext requestContext = new DUUICommunicationLayerRequestContext(
                        containerURL,
                        jc,
                        _timeout,
                        _client,
                        _luaContext,
                        skipVerification,
                        prefix
                    );

                    IDUUICommunicationLayer layer = get_communication_layer(requestContext);

                    logger().info("%s Container for image %s is online (URL %s) and seems to understand DUUI V1 format!%n", 
                        prefix, comp.getImageName(), containerURL
                    );

                    // Add one replica of the instantiated component per worker
                    for (int j = 0; j < comp.getWorkers(); j++) {
                        String instanceIdentifier = "%s-%s-Replica-%d-Worker-%d".formatted(
                            comp.getName(),
                            uuidCopy.substring(0, 5),
                            iCopy,
                            j + 1 
                        );
                        IDUUIUrlAccessible instance = new ComponentInstance(instanceIdentifier, containerid, containerURL, port, layer.copy());
                        comp.addComponent(instance);
                        logger().trace(
                            DUUIContexts.component(comp, instance).status(DUUIStatus.INACTIVE),
                            "[DUUIDockerDriver] Started instance: %s", 
                            instanceIdentifier
                        );
                    }
                    
                    logger().info(
                        DUUIContexts.component(comp).status(DUUIStatus.INSTANTIATING),
                        "%s Replicas for image %s initialised!", 
                    prefix, comp.getImageName()
                    );
                } catch (Exception e) {
                    //_interface.stop_container(containerid);
                    //throw e;
                }
            }
        } finally {
            comp.setLogger(logger());
        }


        return shutdown.get() ? null : uuid;
    }

    /**
     * Shutdown of the Docker-Driver
     *
     * @hidden
     */
    @Override
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
    @Override
    public boolean destroy(String uuid) {
        var comp = _active_components.remove(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        if (!comp.getRunningAfterExit()) {
            int counter = 1;
            for (IDUUIUrlAccessible inst : comp.getTotalInstances()) {
                logger().info(
                    DUUIContexts.component(comp, inst).status(DUUIStatus.SHUTDOWN),
                    "[DUUIDockerDriver][Replication %d/%d] Stopping docker container %s...%n", 
                    counter, comp.getTotalInstances().size(), ((ComponentInstance)inst).getContainerId()
                );
                _interface.stop_container(((ComponentInstance)inst).getContainerId());
                counter += 1;
            }
        }

        return true;
    }

    static record ComponentInstance(
            String _identifier,
            String _container_id,
            String _host, 
            int _port,
            IDUUICommunicationLayer _communicationLayer
    ) implements IDUUIUrlAccessible {
        @Override
        public String getUniqueInstanceKey() {
            return _identifier;
        }

        public String getContainerId() {
            return _container_id;
        }

        @Override
        public String generateURL() {
            return _host;
        }
        
        int getContainerPort() {
            return _port;
        }
        
        String getContainerUrl() {
            return _host;
        }

        @Override
        public IDUUICommunicationLayer getCommunicationLayer() {
            return _communicationLayer;
        }
    }

    protected static class InstantiatedComponent extends DUUIRestDriver.IDUUIInstantiatedRestComponent<InstantiatedComponent> {
        
        private String _image_name;
        private String _reg_password;
        private String _reg_username;
        
        private List<String> _env;
        private boolean _gpu;
        private boolean _keep_runnging_after_exit;
        private boolean _withImageFetching;


        InstantiatedComponent(DUUIPipelineComponent component, String uuid) {
            super(component, uuid);
            _image_name = component.getDockerImageName();

            if (_image_name == null) {
                throw new InvalidParameterException("The image name was not set! This is mandatory for the DUUIDockerDriver Class.");
            }
            _withImageFetching = component.getDockerImageFetching(false);

            _reg_password = component.getDockerAuthPassword();
            _reg_username = component.getDockerAuthUsername();
            
            _env = component.getEnv();
            
            _gpu = component.getDockerGPU(false);
            _keep_runnging_after_exit = component.getDockerRunAfterExit(false);
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

        public boolean getRunningAfterExit() {
            return _keep_runnging_after_exit;
        }

        public boolean usesGPU() {
            return _gpu;
        }

        public List<String> getEnv() {
            return _env;
        }
        
    }

    public static class Component extends IDUUIDriverInterface.ComponentBuilder<Component> {

        public Component(String target) throws URISyntaxException, IOException {
            super(new DUUIPipelineComponent());
            _component.withDockerImageName(target);
        }

        public Component(DUUIPipelineComponent pComponent) throws URISyntaxException, IOException {
            super(pComponent);
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

        public Component withEnv(String... envString) {
            _component.withEnv(envString);
            return this;
        }

        public DUUIPipelineComponent build() {
            _component.withDriver(DUUIDockerDriver.class);
            return _component;
        }

    }
}
