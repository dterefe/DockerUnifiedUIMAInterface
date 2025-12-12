package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import static java.lang.String.format;
import static org.awaitility.Awaitility.await;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.InvalidXMLException;
import org.json.JSONObject;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIDockerInterface;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver.ComponentInstance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.ImageException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIEvent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUILogContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUILogger;
import org.xml.sax.SAXException;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import podman.client.PodmanClient;
import podman.client.containers.ContainerCreateOptions;
import podman.client.containers.ContainerDeleteOptions;
import podman.client.containers.ContainerInspectOptions;

/**
 * Driver for using a local Podman instance to run DUUI components
 *
 * @author Giuseppe Abrami
 */
public class DUUIPodmanDriver extends DUUIRestDriver<DUUIPodmanDriver, DUUIDockerDriver.InstantiatedComponent> {

    private PodmanClient _interface = null;
    private HttpClient _client;

    private Vertx _vertx = null;

    private HashMap<String, DUUIDockerDriver.InstantiatedComponent> _active_components;


    public DUUIPodmanDriver() throws IOException, SAXException {

        VertxOptions vertxOptions = new VertxOptions().setPreferNativeTransport(true);
        _vertx = Vertx.vertx(vertxOptions);
        _client = HttpClient.newHttpClient();
        _timeout = Duration.ofMillis(10_000);
        _luaContext = null;

        _active_components = new HashMap<>();

        logger().debug("[PodmanDriver] Is Native Transport Enabled: %s\n", _vertx.isNativeTransportEnabled());

        PodmanClient.Options options = new PodmanClient.Options().setSocketPath(podmanSocketPath());

        _interface = PodmanClient.create(_vertx, options);
    }

    public static String podmanSocketPath() {
        String path = System.getenv("PODMAN_SOCKET_PATH");

        if (path == null) {
            DUUILogger log = DUUILogContext.getLogger();
            String uid = System.getenv("UID");
            if (uid == null) {
                try {
                    ProcessBuilder pb = new ProcessBuilder("id", "-u");
                    Process process = pb.start();

                    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                    uid = reader.readLine(); // UID aus der Ausgabe lesen
                } catch (IOException e) {
                    log.error(
                            "[PodmanDriver] Failed to resolve UID for PODMAN_SOCKET_PATH: %s%n%s",
                            e.toString(),
                            ExceptionUtils.getStackTrace(e)
                    );
                }
            }
            path = "/run/user/" + uid + "/podman/podman.sock";
            log.debug("[PodmanDriver] Using podman socket path: %s", path);
        }

        return path;
    }

    private static <T> T awaitResult(Future<T> future) throws Throwable {
        AtomicBoolean done = new AtomicBoolean();
        AtomicReference<T> result = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        future.onComplete(res -> {
            if (res.succeeded()) {
                result.set(res.result());
            } else {
                failure.set(res.cause());
            }
            done.set(true);
        });
        await().untilTrue(done);
        if (failure.get() != null) {
            throw failure.get();
        } else {
            return result.get();
        }
    }

    @Override
    protected HashMap<String, DUUIDockerDriver.InstantiatedComponent> getActiveComponents() {
        return _active_components;
    }

    @Override
    public boolean canAccept(DUUIPipelineComponent component) throws InvalidXMLException, IOException, SAXException {
        return component.getDockerImageName() != null;
    }

    public static void pull(String sImagename) throws ImageException {

//        _interface.images().pull(sImagename, new ImagePullOptions())
//                .subscribe(new Flow.Subscriber<JsonObject>() {
//            @Override
//            public void onSubscribe(Flow.Subscription subscription) {
//                System.out.println(subscription.toString());
//            }
//
//            @Override
//            public void onNext(JsonObject item) {
//                System.out.println(item.toString());
//            }
//
//            @Override
//            public void onError(Throwable throwable) {
//                throwable.printStackTrace();
//            }
//
//            @Override
//            public void onComplete() {
//                System.out.println("finish");
//            }
//        });

        DUUILogger log = DUUILogContext.getLogger();

        ProcessBuilder pb = new ProcessBuilder("podman", "pull", sImagename);
        Process process = null;

        try {
            process = pb.start();
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
                BufferedReader brError = new BufferedReader(new InputStreamReader(process.getErrorStream()));
                String input;
                while ((input = br.readLine()) != null) {
                    log.info("[PodmanDriver] podman pull output: %s", input);
                }
                StringBuilder sb = new StringBuilder();
                while ((input = brError.readLine()) != null) {
                    // Print the input
                    if (sb.length() > 0) {
                        sb.append("\n");
                    }
                    sb.append(input);
                }
                if (sb.length() > 0) {
                    throw new ImageException(sb.toString());
                }

            } catch (IOException e) {
                log.error(
                        "[PodmanDriver] Error while reading podman pull output: %s%n%s",
                        e.toString(),
                        ExceptionUtils.getStackTrace(e)
                );
            }

            process.waitFor();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification, AtomicBoolean shutdown) throws Exception {

        String uuid = UUID.randomUUID().toString();
        while (_active_components.containsKey(uuid.toString())) {
            uuid = UUID.randomUUID().toString();
        }


        DUUIDockerDriver.InstantiatedComponent comp = new DUUIDockerDriver.InstantiatedComponent(component, uuid);

        try(var ignored = logger().withContext(DUUIEvent.Context.from(this, comp)))  {
            // Inverted if check because images will never be pulled if !comp.getImageFetching() is checked.
            if (comp.getImageFetching()) {
                if (comp.getUsername() != null) {
                    logger().info("[PodmanDriver] Attempting image %s download from secure remote registry", comp.getImageName());
                }
                try {
                    pull(comp.getImageName());
    //            _interface.images().pull(comp.getImageName(), new ImagePullOptions());

                    if (shutdown.get()) {
                        return null;
                    }

                    logger().info("[PodmanDriver] Pulled image with id %s", comp.getImageName());
                } catch (ImageException e) {
                    logger().error("%s during image fetching: %s", e, e.getMessage());
                }


            } else {
    //            _interface.pullImage(comp.getImageName());
                try {
                    if (!awaitResult(_interface.images().exists(comp.getImageName()))) {
                        throw new InvalidParameterException(format("Could not find local image \"%s\". Did you misspell it or forget with .withImageFetching() to fetch it from remote registry?", comp.getImageName()));
                    }
                } catch (Exception e) {
                    throw e;
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }

            try {
                if (awaitResult(_interface.images().exists(comp.getImageName()))) {
                    logger().info("[PodmanDriver] Assigned new pipeline component unique id %s", uuid);

                    _active_components.put(uuid, comp);

                    for (int i = 0; i < comp.getScale(); i++) {
                        if (shutdown.get()) {
                            return null;
                        }


                        ContainerCreateOptions pOptions = new ContainerCreateOptions();
                        pOptions.image(comp.getImageName());
                        pOptions.remove(true);
                        pOptions.publishImagePorts(true);

                        if (comp.usesGPU()) {
                            List<ContainerCreateOptions.LinuxDevice> linuxDevices = new ArrayList<>();
                            linuxDevices.add(new ContainerCreateOptions.LinuxDevice(0666, 0, 195, 0, "/dev/nvidia0", "c", 0));
                            //                linuxDevices.add(new ContainerCreateOptions.LinuxDevice(0666, 0, 195, 255, "/dev/nvidiactl", "c", 0));
                            //                linuxDevices.add(new ContainerCreateOptions.LinuxDevice(0666, 0, 236, 0, "/dev/nvidia-uvm", "c", 0));

                            //                pOptions.devices(linuxDevices);
                            pOptions.hostDeviceList(linuxDevices);
                        }


                        JsonObject pObject = null;
                        JsonObject iObject = null;
                        String containerId = "";
                        int port = -1;
                        try {
                            pObject = awaitResult(_interface.containers().create(pOptions));
                            containerId = pObject.getString("Id");

                            _interface.containers().start(containerId);

                            DUUILogger log = DUUILogContext.getLogger();
                            log.debug("[PodmanDriver] Created container: %s", pObject);


                            iObject = awaitResult(_interface.containers().inspect(containerId, new ContainerInspectOptions().setSize(false)));
                            JSONObject nObject = new JSONObject(iObject);
                            log.debug("[PodmanDriver] Inspect container result: %s", nObject);
                            port = nObject.getJSONObject("map").getJSONObject("HostConfig").getJSONObject("PortBindings").getJSONArray("9714/tcp").getJSONObject(0).getInt("HostPort");


                        } catch (Throwable e) {
                            logger().debug(
                                "%s during container build: %s%n",
                                e.getClass().getSimpleName(),
                                e.getMessage()
                            );
                            stop_container(containerId, true);
                            throw new RuntimeException(e);
                        }

                        try {
                            if (port == 0) {
                                throw new UnknownError("Could not read the container port!");
                            }

                            String containerUrl = resolveHostUrl(port);

                            final int iCopy = i + 1;
                            final String uuidCopy = uuid;
                            String prefix = comp.prefix(iCopy);

                            DUUICommunicationLayerRequestContext requestContext = new DUUICommunicationLayerRequestContext(
                                containerUrl,
                                jc,
                                _timeout,
                                _client,
                                _luaContext,
                                skipVerification,
                                prefix
                            );

                            IDUUICommunicationLayer layer = get_communication_layer(requestContext);

                            logger().info(
                                    "%s Container for image %s is online (URL %s) and seems to understand DUUI V1 format!\n",
                                    prefix, comp.getImageName(), containerUrl
                            );

                            // Add one replica of the instantiated component per worker
                            for (int j = 0; j < comp.getWorkers(); j++) {
                                String instanceIdentifier = "%s-%s-Replica-%d-Worker-%d".formatted(
                                    comp.getName(),
                                    uuidCopy.substring(0, 5),
                                    iCopy,
                                    j + 1 
                                );
                                comp.addComponent(
                                        new DUUIDockerDriver.ComponentInstance(
                                                instanceIdentifier,
                                                containerId,
                                                containerUrl,
                                                port,
                                                layer.copy()
                                        )
                                );
                            }
                        } catch (Exception e) {
                            logger().error(
                                "%s during component initialization: %s%n%s",
                                e.getClass().getSimpleName(),
                                e.getMessage(),
                                ExceptionUtils.getStackTrace(e)
                            );
                            //throw e;
                        }


                    }

                }
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
        return shutdown.get() ? null : uuid;
    }

    /**
     * Resolve a host URL for a published container port, similar in spirit
     * to {@link org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIDockerInterface#getHostUrl}.
     *
     * @param port published host port
     * @return URL like "http://host:port" that is reachable from this process
     */
    private String resolveHostUrl(int port) {
        List<String> candidates = new ArrayList<>();
        candidates.add(DUUIComposer.getLocalhost());
        candidates.add("localhost");
        candidates.add("host.docker.internal");

        String gw = DUUIDockerInterface.getDockerHostIp();
        if (gw != null && !gw.isBlank() && !candidates.contains(gw)) {
            candidates.add(gw);
        }

        for (String host : candidates) {
            if (DUUIDockerInterface.canConnectDebug(host, port, 700)) {
                return "http://" + host + ":" + port;
            }
        }

        throw new IllegalStateException("Could not reach Podman container on any host IP: " + candidates);
    }

    private void stop_container(String containerId) {
        stop_container(containerId, true);
    }

    private void stop_container(String containerId, boolean bDelete) {
        _interface.containers().stop(containerId, false, 1);
        if (bDelete) {
            _interface.containers().delete(containerId, new ContainerDeleteOptions().setTimeout(1).setIgnore(true));
        }
    }

    @Override
    public boolean destroy(String uuid) {
        DUUIDockerDriver.InstantiatedComponent comp = _active_components.remove(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        if (!comp.getRunningAfterExit()) {
            int counter = 1;
            for (IDUUIUrlAccessible inst : comp.getTotalInstances()) {
                logger().info("[Replica %d/%d] Stopping docker container %s...", counter, comp.getInstances().size(), ((ComponentInstance)inst).getContainerId());
                stop_container(((ComponentInstance)inst).getContainerId(), true);

                counter += 1;
            }
        }

        return true;
    }

    @Override
    public void shutdown() {
        for (String s : _active_components.keySet()) {
            destroy(s);
        }
        try {
            Thread.sleep(3000l);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static class Component extends IDUUIDriverInterface.ComponentBuilder<Component>  {

        public Component(String target) throws URISyntaxException, IOException {
            super(new DUUIPipelineComponent());
            _component.withDockerImageName(target);
        }

        public Component(DUUIPipelineComponent pComponent) throws URISyntaxException, IOException {
            super(pComponent);
        }

        public DUUIPodmanDriver.Component withRegistryAuth(String username, String password) {
            _component.withDockerAuth(username, password);
            return this;
        }

        public DUUIPodmanDriver.Component withImageFetching() {
            return withImageFetching(true);
        }

        public DUUIPodmanDriver.Component withImageFetching(boolean imageFetching) {
            _component.withDockerImageFetching(imageFetching);
            return this;
        }

        public DUUIPodmanDriver.Component withGPU(boolean gpu) {
            _component.withDockerGPU(gpu);
            return this;
        }

        public DUUIPodmanDriver.Component withRunningAfterDestroy(boolean run) {
            _component.withDockerRunAfterExit(run);
            return this;
        }

        public DUUIPipelineComponent build() {
            _component.withDriver(DUUIPodmanDriver.class);
            return _component;
        }

    }

}
