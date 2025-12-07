package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;


import static java.lang.String.format;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIDockerInterface;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.ImagePullException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.PipelineComponentException;

/**
 *
 * @author Alexander Leonhardt
 */
public class DUUISwarmDriver extends DUUIRestDriver<DUUISwarmDriver, DUUISwarmDriver.InstantiatedComponent> {
    private final DUUIDockerInterface _interface;
    private HttpClient _client;
    private final HashMap<String, DUUISwarmDriver.InstantiatedComponent> _active_components;
    private String _withSwarmVisualizer;
    private String _host = "localhost";

    public DUUISwarmDriver() throws IOException {
        _interface = new DUUIDockerInterface();

        _timeout = Duration.ofMillis(10_000);
        _client = HttpClient.newHttpClient();

        _active_components = new HashMap<>();
        _withSwarmVisualizer = null;
    }

    public DUUISwarmDriver(int timeout_ms) throws IOException, UIMAException {
        _interface = new DUUIDockerInterface();

        JCas _basic = JCasFactory.createJCas();
        _basic.setDocumentLanguage("en");
        _basic.setDocumentText("Hello World!");
        _timeout = Duration.ofMillis(timeout_ms);
        _client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(timeout_ms)).build();

        _active_components = new HashMap<>();
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
                _interface.pullImage("dockersamples/visualizer", null, null);
            } catch (ImagePullException e) {
                throw new IllegalStateException("Unable to pull swarm visualizer image.", e);
            }
            if (port == null) {
                _withSwarmVisualizer = _interface.run("dockersamples/visualizer", false, true, 8080, true);
            } else {
                _withSwarmVisualizer = _interface.run("dockersamples/visualizer", false, true, 8080, port, true);
            }
            int port_mapping = _interface.extract_port_mapping(_withSwarmVisualizer, 8080);
            System.out.printf("[DUUISwarmDriver] Running visualizer on address http://" + getHostname() + ":%d\n", port_mapping);
            Thread.sleep(1500);
        }
        return this;
    }

    @Override
    protected Map<String, DUUISwarmDriver.InstantiatedComponent> getActiveComponents() {
        return _active_components;
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

    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification, AtomicBoolean shutdown) throws Exception {
        String uuid = UUID.randomUUID().toString();
        while (_active_components.containsKey(uuid)) {
            uuid = UUID.randomUUID().toString();
        }

        if (!_interface.isSwarmManagerNode()) {
            throw new InvalidParameterException("This node is not a Docker Swarm Manager, thus cannot create and schedule new services!");
        }
        DUUISwarmDriver.InstantiatedComponent comp = new DUUISwarmDriver.InstantiatedComponent(component, uuid);

        if (_interface.getLocalImage(comp.getImageName()) == null) {
            // If image is not available try to pull it
            try {
                _interface.pullImage(comp.getImageName(), null, null);
            } catch (ImagePullException e) {
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
            _interface.push_image(comp.getImageName(), comp.getLocalImageName(), comp.getUsername(), comp.getPassword());
        }
        System.out.printf("[DockerSwarmDriver] Assigned new pipeline component unique id %s\n", uuid);

        String digest = _interface.getDigestFromImage(comp.getImageName());
        comp.getPipelineComponent().__internalPinDockerImage(comp.getImageName(), digest);
        System.out.printf("[DockerSwarmDriver] Transformed image %s to pinnable image name %s\n", comp.getImageName(), digest);

        String serviceid = _interface.run_service(digest, comp.getScale(), comp.getConstraints());

        int port = _interface.extract_service_port_mapping(serviceid);

        System.out.printf("[DockerSwarmDriver][%s] Started service, waiting for it to become responsive...\n", uuid);

        if (port == 0) {
            throw new UnknownError("Could not read the service port!");
        }
        final String uuidCopy = uuid;
        IDUUICommunicationLayer layer = null;
        String prefix = String.format("[DockerSwarmDriver][%s][Replicas %d]", uuidCopy.substring(0, 5) + "...", comp.getScale());
        String swarmUrl = _interface.getHostUrl(port);

        try {
            if (shutdown.get()) {
                return null;
            }


            DUUICommunicationLayerRequestContext requestContext = new DUUICommunicationLayerRequestContext(
                swarmUrl,
                jc,
                _timeout,
                _client,
                _luaContext,
                skipVerification,
                prefix
            );

            layer = get_communication_layer(requestContext);
        } catch (Exception e) {
            _interface.rm_service(serviceid);
            throw e;
        }

        System.out.printf("%s Service for image %s is online (URL %s) and seems to understand DUUI V1 format!\n", 
            prefix, comp.getImageName(), swarmUrl
        );

        comp.initialise(serviceid, port, layer, this);
        Thread.sleep(500);

        _active_components.put(uuid, comp);

        return shutdown.get() ? null : uuid;
    }

    @Override
    public void shutdown() {
        if (_withSwarmVisualizer != null) {
            System.out.println("[DUUISwarmDriver] Shutting down swarm visualizer now!");
            _interface.stop_container(_withSwarmVisualizer);
            _withSwarmVisualizer = null;
        }
    }

    @Override
    public boolean destroy(String uuid) {
        DUUISwarmDriver.InstantiatedComponent comp = _active_components.remove(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the Swarm Driver");
        }
        if (!comp.getRunningAfterExit()) {
            System.out.printf("[DockerSwarmDriver] Stopping service %s...\n", comp.getServiceId());
            _interface.rm_service(comp.getServiceId());
        }

        return true;
    }

    private static record ComponentInstance(
            String _identifier,
            String _host, 
            IDUUICommunicationLayer _communicationLayer
    ) implements IDUUIUrlAccessible {
        @Override
        public String getUniqueInstanceKey() {
            return _identifier;
        }

        @Override
        public String generateURL() {
            return _host;
        }

        @Override
        public IDUUICommunicationLayer getCommunicationLayer() {
            return _communicationLayer;
        }
    }

    protected static class InstantiatedComponent extends DUUIRestDriver.IDUUIInstantiatedRestComponent<InstantiatedComponent>  {
        private final String _image_name;
        private String _service_id;
        private int _service_port;
        private final Boolean _keep_runnging_after_exit;
        private final String _fromLocalImage;

        private final List<String> _constraints = new ArrayList<>(0);

        private final String _reg_password;
        private final String _reg_username;
        private String sHost = "localhost";


        InstantiatedComponent(DUUIPipelineComponent component, String uniqueComponentKey) {
            super(component, uniqueComponentKey);

            _image_name = component.getDockerImageName();
            if (_image_name == null) {
                throw new InvalidParameterException("The image name was not set! This is mandatory for the DockerLocalDriver Class.");
            }

            _constraints.addAll(component.getConstraints());

            _keep_runnging_after_exit = component.getDockerRunAfterExit(false);

            _fromLocalImage = null;
            _reg_password = component.getDockerAuthPassword();
            _reg_username = component.getDockerAuthUsername();
        }


        public IDUUIInstantiatedPipelineComponent withHost(String sHost) {
            this.sHost = sHost;
            return this;
        }

        public String getHost() {
            return this.sHost;
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

        public InstantiatedComponent initialise(String service_id, int container_port, IDUUICommunicationLayer layer, DUUISwarmDriver swarmDriver) throws IOException, InterruptedException {

            _service_id = service_id;
            _service_port = container_port;
            for (int i = 0; i < _scale; i++) {
                String instanceIdentifier = "%s-%s-Replica-%d".formatted(
                    getName(),                          
                    _uniqueComponentKey.substring(0, 5),
                    i + 1                               
                );
                _components.add(new ComponentInstance(instanceIdentifier, getServiceUrl(), layer.copy()));

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

        public List<String> getConstraints() {
            return _constraints;
        }

        public boolean getRunningAfterExit() {
            return _keep_runnging_after_exit;
        }

    }

    public static class Component extends IDUUIDriverInterface.ComponentBuilder<Component> {

        public Component(String globalRegistryImageName) throws URISyntaxException, IOException {
            super(new DUUIPipelineComponent());
            _component.withDockerImageName(globalRegistryImageName);
        }

        public Component(DUUIPipelineComponent pComponent) {
            super(pComponent);
        }

        public Component withConstraintHost(String sHost) {
            _component.withConstraint("node.hostname==" + sHost);
            return this;
        }

        public Component withConstraintLabel(String sKey, String sValue) {
            _component.withConstraint("node.labels." + sKey + "==" + sValue);
            return this;
        }

        public Component withConstraints(List<String> constraints) {
            _component.withConstraints(constraints);
            return this;
        }

        public DUUISwarmDriver.Component withRegistryAuth(String username, String password) {
            _component.withDockerAuth(username, password);
            return this;
        }


        public Component withRunningAfterDestroy(boolean run) {
            _component.withDockerRunAfterExit(run);
            return this;
        }

        public DUUIPipelineComponent build() {
            _component.withDriver(DUUISwarmDriver.class);
            return _component;
        }
    }
}
