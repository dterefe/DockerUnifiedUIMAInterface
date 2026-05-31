package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.InvalidXMLException;
import org.json.JSONObject;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.CommunicationLayerException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.ImageException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.PipelineComponentException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.texttechnologylab.duui.clients.http.DUUIHttpEndpoint;
import org.texttechnologylab.duui.clients.http.IDUUIEndpoint;
import org.texttechnologylab.duui.pipeline.component.DUUIComponent;
import org.texttechnologylab.duui.pipeline.component.DUUINode;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Annotator;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Config;
import org.xml.sax.SAXException;
import podman.client.PodmanClient;
import podman.client.containers.ContainerCreateOptions;
import podman.client.containers.ContainerDeleteOptions;
import podman.client.containers.ContainerInspectOptions;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.String.format;
import static org.awaitility.Awaitility.await;
import static org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.getLocalhost;
import static org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver.responsiveAfterTime;

/**
 * Driver for using a local Podman instance to run DUUI components
 *
 * @author Giuseppe Abrami
 */
public class DUUIPodmanDriver extends DUUIV1Driver {

    private PodmanClient _interface = null;

    private Vertx _vertx = null;


    public DUUIPodmanDriver() throws IOException, SAXException {
        super();

        VertxOptions vertxOptions = new VertxOptions().setPreferNativeTransport(true);
        _vertx = Vertx.vertx(vertxOptions);

        System.out.printf("[PodmanDriver] Is Native Transport Enabled: %s\n", _vertx.isNativeTransportEnabled());

        PodmanClient.Options options = new PodmanClient.Options().setSocketPath(podmanSocketPath());

        _interface = PodmanClient.create(_vertx, options);
        _containerTimeout = 10000;
        _activeComponents = new HashMap<>();

    }

    public static String podmanSocketPath() {
        String path = System.getenv("PODMAN_SOCKET_PATH");

        if (path == null) {
            String uid = System.getenv("UID");
            if (uid == null) {
                try {
                    ProcessBuilder pb = new ProcessBuilder("id", "-u");
                    Process process = pb.start();

                    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                    uid = reader.readLine(); // UID aus der Ausgabe lesen
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            path = "/run/user/" + uid + "/podman/podman.sock";
            System.out.println(path);
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

    private static JsonObject readOnlyBindMount(String path) {
        return new JsonObject()
                .put("type", "bind")
                .put("source", path)
                .put("destination", path)
                .put("options", new JsonArray(List.of("ro")));
    }

    private static JsonArray nvidiaDriverLibraryMounts() {
        JsonArray mounts = new JsonArray();
        Set<String> mountedPaths = new LinkedHashSet<>();
        List<String> candidates = List.of(
                "/usr/lib/x86_64-linux-gnu/libcuda.so",
                "/usr/lib/x86_64-linux-gnu/libcuda.so.1",
                "/usr/lib/x86_64-linux-gnu/libnvidia-ml.so",
                "/usr/lib/x86_64-linux-gnu/libnvidia-ml.so.1"
        );
        for (String path : candidates) {
            if (new File(path).exists() && mountedPaths.add(path)) {
                mounts.add(readOnlyBindMount(path));
            }
        }
        File libDir = new File("/usr/lib/x86_64-linux-gnu");
        File[] versionedLibraries = libDir.listFiles((dir, name) ->
                name.startsWith("libcuda.so.") || name.startsWith("libnvidia-ml.so."));
        if (versionedLibraries != null) {
            for (File library : versionedLibraries) {
                String path = library.getAbsolutePath();
                if (mountedPaths.add(path)) {
                    mounts.add(readOnlyBindMount(path));
                }
            }
        }
        return mounts;
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


        ProcessBuilder pb = new ProcessBuilder("podman", "pull", sImagename);
        Process process = null;

        try {
            process = pb.start();
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
                BufferedReader brError = new BufferedReader(new InputStreamReader(process.getErrorStream()));
                String input;
                while ((input = br.readLine()) != null) {
                    // Print the input
                    System.out.println(input);
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
                e.printStackTrace();
            }

            process.waitFor();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }


    }

    public List<String> getEndpointUrls(String uuid) {
        DUUIDockerDriver.InstantiatedComponent comp = (DUUIDockerDriver.InstantiatedComponent) _activeComponents.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        return comp.getInstances().stream()
                .map(DUUIDockerDriver.ComponentInstance::generateURL)
                .distinct()
                .toList();
    }

    @Override
    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification, AtomicBoolean shutdown) throws Exception {

        String uuid = UUID.randomUUID().toString();
        while (_activeComponents.containsKey(uuid.toString())) {
            uuid = UUID.randomUUID().toString();
        }


        component.withV1Transport(_v1StreamingTransport, _v1ContentType);
        DUUIDockerDriver.InstantiatedComponent comp = new DUUIDockerDriver.InstantiatedComponent(component, uuid);

        // Inverted if check because images will never be pulled if !comp.getImageFetching() is checked.
        if (comp.getImageFetching()) {
            if (comp.getUsername() != null) {
                System.out.printf("[PodmanDriver] Attempting image %s download from secure remote registry\n", comp.getImageName());
            }
            try {
                pull(comp.getImageName());
//            _interface.images().pull(comp.getImageName(), new ImagePullOptions());

                if (shutdown.get()) {
                    return null;
                }

                System.out.printf("[PodmanDriver] Pulled image with id %s\n", comp.getImageName());
            } catch (ImageException e) {
                System.err.println(e.getMessage());
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
                System.out.printf("[PodmanDriver] Assigned new pipeline component unique id %s\n", uuid);

                _activeComponents.put(uuid, comp);

                for (int i = 0; i < comp.getScale(); i++) {
                    if (shutdown.get()) {
                        return null;
                    }


                    ContainerCreateOptions pOptions = new ContainerCreateOptions();
                    pOptions.image(comp.getImageName());
                    pOptions.remove(true);
                    pOptions.publishImagePorts(true);
                    pOptions.env(podmanEnv(comp.getPipelineComponent()));

                    // Explicit port mapping for port 9714 — works even for images without EXPOSE
                    pOptions.portMappings(List.of(
                        new ContainerCreateOptions.PortMapping(9714, "", 0, "tcp", 0)
                    ));

                    if (comp.usesGPU()) {
                        List<ContainerCreateOptions.LinuxDevice> linuxDevices = new ArrayList<>();
                        for (int gpuIndex : podmanGpuDevices(comp.getPipelineComponent())) {
                            linuxDevices.add(new ContainerCreateOptions.LinuxDevice(0666, 0, 195, gpuIndex, "/dev/nvidia" + gpuIndex, "c", 0));
                        }
                        linuxDevices.add(new ContainerCreateOptions.LinuxDevice(0666, 0, 195, 255, "/dev/nvidiactl", "c", 0));
                        linuxDevices.add(new ContainerCreateOptions.LinuxDevice(0666, 0, 195, 254, "/dev/nvidia-modeset", "c", 0));
                        linuxDevices.add(new ContainerCreateOptions.LinuxDevice(0666, 0, 510, 0, "/dev/nvidia-uvm", "c", 0));
                        linuxDevices.add(new ContainerCreateOptions.LinuxDevice(0666, 0, 510, 1, "/dev/nvidia-uvm-tools", "c", 0));
                        pOptions.hostDeviceList(linuxDevices);
                        pOptions.json().put("mounts", nvidiaDriverLibraryMounts());
                    }


                    JsonObject pObject = null;
                    JsonObject iObject = null;
                    String containerId = "";
                    int port = -1;
                    try {
                        pObject = awaitResult(_interface.containers().create(pOptions));
                        containerId = pObject.getString("Id");

                        _interface.containers().start(containerId);

                        System.out.println(pObject);


                        iObject = awaitResult(_interface.containers().inspect(containerId, new ContainerInspectOptions().setSize(false)));
                        JSONObject nObject = new JSONObject(iObject);
                        System.out.println(nObject);
                        try {
                            port = nObject.getJSONObject("map").getJSONObject("HostConfig").getJSONObject("PortBindings").getJSONArray("9714/tcp").getJSONObject(0).getInt("HostPort");
                        } catch (org.json.JSONException e) {
                            // Image has no EXPOSE — fall back to NetworkSettings or default port
                            try {
                                port = nObject.getJSONObject("map").getJSONObject("NetworkSettings").getJSONObject("Ports").getJSONArray("9714/tcp").getJSONObject(0).getInt("HostPort");
                            } catch (org.json.JSONException e2) {
                                System.err.println("[PodmanDriver] No port binding found for 9714/tcp, defaulting to internal port");
                                port = 9714;
                            }
                        }


                    } catch (Throwable e) {
                        e.printStackTrace();
                        stop_container(containerId, true);
                        throw new RuntimeException(e);
                    }

                    try {
                        if (port == 0) {
                            throw new UnknownError("Could not read the container port!");
                        }

                        String containerUrl = resolveHostUrl(port);

                        final int iCopy = i;
                        final String uuidCopy = uuid;
                        int containerTimeoutMs = containerStartupTimeoutMs(comp.getPipelineComponent());
                        IDUUICommunicationLayer layer = responsiveAfterTime(
                                containerUrl,
                                jc,
                                containerTimeoutMs,
                                _client,
                                (msg) -> System.out.printf("[PodmanDriver][%s][Podman Replication %d/%d] %s\n",
                                        uuidCopy, iCopy + 1, comp.getScale(), msg),
                                _luaContext,
                                skipVerification
                        );

                        System.out.printf(
                                "[PodmanDriver][%s][Podman Replication %d/%d] Container for image %s is online (URL %s) and seems to understand DUUI V1 format!\n",
                                uuid, i + 1, comp.getScale(), comp.getImageName(), containerUrl
                        );

                        // Add one replica of the instantiated component per worker
                        for (int j = 0; j < comp.getWorkers(); j++) {
                            comp.addInstance(
                                    new DUUIDockerDriver.ComponentInstance(
                                            containerId,
                                            port,
                                            layer.copy(),
                                            null,
                                            containerUrl
                                    )
                            );
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        stop_container(containerId, true);
                        throw e;
                    }


                }

            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
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

        String gw = DUUIDockerDriver.getDockerHostIp();
        if (gw != null && !gw.isBlank() && !candidates.contains(gw)) {
            candidates.add(gw);
        }

        for (String host : candidates) {
            if (DUUIDockerDriver.canConnect(host, port, 700)) {
                return "http://" + host + ":" + port;
            }
        }

        throw new IllegalStateException("Could not reach Podman container on any host IP: " + candidates);
    }

    @Override
    public DUUIPodmanDriver withTimeout(int container_timeout_ms) {
        _containerTimeout = container_timeout_ms;
        return this;
    }

    private int containerStartupTimeoutMs(DUUIPipelineComponent component) {
        long timeoutMs = component.getTimeout() * 1000L;
        if (timeoutMs <= 0L) {
            return _containerTimeout;
        }
        return (int) Math.min(Integer.MAX_VALUE, Math.max(_containerTimeout, timeoutMs));
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
    public void run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf, DUUIComposer composer) throws CASException, PipelineComponentException, CompressorException, CommunicationLayerException, IOException {
        long mutexStart = System.nanoTime();
        DUUIDockerDriver.InstantiatedComponent comp = (DUUIDockerDriver.InstantiatedComponent) _activeComponents.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        IDUUIInstantiatedPipelineComponent.process(aCas, comp, perf);

    }

    @Override
    public boolean destroy(String uuid) {
        DUUIDockerDriver.InstantiatedComponent comp = (DUUIDockerDriver.InstantiatedComponent) _activeComponents.remove(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        if (!comp.getRunningAfterExit()) {
            int counter = 1;
            for (DUUIDockerDriver.ComponentInstance inst : comp.getInstances()) {
                System.out.printf("[PodmanDriver][Replication %d/%d] Stopping docker container %s...\n", counter, comp.getInstances().size(), inst.getContainerId());
                stop_container(inst.getContainerId(), true);

                counter += 1;
            }
        }

        return true;
    }

    @Override
    public void shutdown() {
        for (String s : _activeComponents.keySet()) {
            destroy(s);
        }
        try {
            Thread.sleep(3000l);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        super.shutdown();
    }

    /**
     * V2 instantiation for Podman driver.
     * <p>
     * Mirrors the container lifecycle from {@link #instantiate(DUUIPipelineComponent, JCas, boolean, AtomicBoolean)}
     * but packages the result as a {@link DUUIComponent}<JCas> instead of storing a UUID.
     * Each container replica becomes a {@link DUUIV1Annotator}, and nodes are distributed
     * round-robin across replicas based on {@code scale × concurrency}.
     *
     * @param component        the pipeline component describing the container image and configuration
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
                    "The image name was not set! This is mandatory for the DUUIPodmanDriver Class.");
        }

        // --- 1. Pull/verify image (mirrors instantiate()) ---
        if (component.getDockerImageFetching(false)) {
            if (component.getDockerAuthUsername() != null) {
                System.out.printf("[PodmanDriver][V2] Attempting image %s download from secure remote registry%n",
                        imageName);
            }
            try {
                pull(imageName);
            } catch (ImageException e) {
                System.err.printf("[PodmanDriver][V2] Failed to pull image %s: %s%n", imageName, e.getMessage());
                throw new PipelineComponentException(
                        format("Failed to pull podman image %s", imageName), e);
            }
            if (shutdown.get()) {
                return null;
            }
            System.out.printf("[PodmanDriver][V2] Pulled image %s%n", imageName);
        } else {
            try {
                if (!awaitResult(_interface.images().exists(imageName))) {
                    throw new InvalidParameterException(
                            format("Could not find local podman image \"%s\". Did you misspell it or forget"
                                    + " .withImageFetching() to fetch it from remote registry?", imageName));
                }
            } catch (Exception e) {
                throw e;
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        // Pin image to a digest-based name so subsequent runs use the exact same image
        String digest = getDigestFromImage(imageName);
        component.__internalPinDockerImage(imageName, digest);
        System.out.printf("[PodmanDriver][V2] Transformed image %s to pinnable name %s%n",
                imageName, component.getDockerImageName());

        int scale = component.getScale(1);
        int workers = component.getWorkers(1);
        String componentId = component.getName() != null ? component.getName() : "podman-component";
        boolean runAfterExit = component.getDockerRunAfterExit(false);

        List<String> containerIds = new ArrayList<>(scale);
        List<DUUIV1Annotator> annotators = new ArrayList<>(scale);

        // --- 2. Create containers and annotators ---
        for (int replicaIdx = 0; replicaIdx < scale; replicaIdx++) {
            if (shutdown.get()) {
                // Clean up containers already started before bailing out
                for (String cid : containerIds) {
                    stop_container(cid);
                }
                return null;
            }

            ContainerCreateOptions pOptions = new ContainerCreateOptions();
            pOptions.image(component.getDockerImageName());
            pOptions.remove(true);
            pOptions.publishImagePorts(true);
            pOptions.env(podmanEnv(component));

            // Explicit port mapping for port 9714 — works even for images without EXPOSE
            pOptions.portMappings(List.of(
                    new ContainerCreateOptions.PortMapping(9714, "", 0, "tcp", 0)));

            if (component.getDockerGPU(false)) {
                List<ContainerCreateOptions.LinuxDevice> linuxDevices = new ArrayList<>();
                for (int gpuIndex : podmanGpuDevices(component)) {
                    linuxDevices.add(new ContainerCreateOptions.LinuxDevice(0666, 0, 195, gpuIndex, "/dev/nvidia" + gpuIndex, "c", 0));
                }
                linuxDevices.add(
                        new ContainerCreateOptions.LinuxDevice(0666, 0, 195, 255, "/dev/nvidiactl", "c", 0));
                linuxDevices.add(
                        new ContainerCreateOptions.LinuxDevice(0666, 0, 195, 254, "/dev/nvidia-modeset", "c", 0));
                linuxDevices.add(
                        new ContainerCreateOptions.LinuxDevice(0666, 0, 510, 0, "/dev/nvidia-uvm", "c", 0));
                linuxDevices.add(
                        new ContainerCreateOptions.LinuxDevice(0666, 0, 510, 1, "/dev/nvidia-uvm-tools", "c", 0));
                pOptions.hostDeviceList(linuxDevices);
                pOptions.json().put("mounts", nvidiaDriverLibraryMounts());
            }

            JsonObject pObject = null;
            JsonObject iObject = null;
            String containerId = "";
            int port = -1;
            try {
                pObject = awaitResult(_interface.containers().create(pOptions));
                containerId = pObject.getString("Id");
                containerIds.add(containerId);

                _interface.containers().start(containerId);

                System.out.println(pObject);

                iObject = awaitResult(_interface.containers().inspect(containerId,
                        new ContainerInspectOptions().setSize(false)));
                JSONObject nObject = new JSONObject(iObject);
                System.out.println(nObject);
                try {
                    port = nObject.getJSONObject("map").getJSONObject("HostConfig")
                            .getJSONObject("PortBindings").getJSONArray("9714/tcp").getJSONObject(0)
                            .getInt("HostPort");
                } catch (org.json.JSONException e) {
                    // Image has no EXPOSE — fall back to NetworkSettings or default port
                    try {
                        port = nObject.getJSONObject("map").getJSONObject("NetworkSettings")
                                .getJSONObject("Ports").getJSONArray("9714/tcp").getJSONObject(0)
                                .getInt("HostPort");
                    } catch (org.json.JSONException e2) {
                        System.err.println(
                                "[PodmanDriver][V2] No port binding found for 9714/tcp, defaulting to internal port");
                        port = 9714;
                    }
                }
            } catch (Throwable e) {
                e.printStackTrace();
                stop_container(containerId, true);
                throw new RuntimeException(e);
            }

            if (port == 0) {
                throw new UnknownError("Could not read the container port!");
            }

            String containerURL = resolveHostUrl(port);
            String replicaId = componentId + "-replica-" + replicaIdx;

            System.out.printf("[PodmanDriver][V2][Podman Replication %d/%d] Container %s started, waiting for"
                    + " responsiveness at %s...%n", replicaIdx + 1, scale, containerId, containerURL);

            try {
                // Wait for container responsiveness with retries (unless skipVerification)
                if (!skipVerification) {
                    waitForContainerResponsive(containerURL, containerStartupTimeoutMs(component));
                }

                // Build endpoint and config for this replica
                IDUUIEndpoint endpoint = new DUUIHttpEndpoint(URI.create(containerURL), _client);
                DUUIV1Config config = v1Config(workers,
                        component.getSourceView(), component.getTargetView(), component.getParameters());

                // DUUIV1Annotator constructor fetches documentation, typesystem, and
                // communication layer — this also acts as a final readiness check
                DUUIV1Annotator annotator = new DUUIV1Annotator(replicaId, endpoint, config);
                annotators.add(annotator);
            } catch (Exception e) {
                stop_container(containerId, true);
                throw e;
            }

            System.out.printf("[PodmanDriver][V2][Podman Replication %d/%d] Annotator %s ready (URL %s)%n",
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
                    System.out.printf("[PodmanDriver][V2][Replication %d/%d] Stopping podman container %s...%n",
                            counter, containerIds.size(), cid);
                    stop_container(cid, true);
                    counter++;
                }
            }
        };

        System.out.printf("[PodmanDriver][V2] Component %s instantiated with %d nodes across %d replica(s)%n",
                componentId, nodes.size(), scale);

        return new DUUIComponent<>(componentId, nodes, closeAction);
    }

    /**
     * Resolves a digest-based pin name for the given image using the Podman CLI.
     * Falls back to {@code null} (no pinning) if the digest cannot be obtained.
     *
     * @param imageName the image reference (e.g., {@code docker.io/library/ubuntu:latest})
     * @return the repo digest string (e.g., {@code docker.io/library/ubuntu@sha256:...}),
     *         or {@code null} if not available
     */
    private String getDigestFromImage(String imageName) {
        try {
            ProcessBuilder pb = new ProcessBuilder("podman", "image", "inspect",
                    "--format", "{{range .RepoDigests}}{{.}}{{end}}", imageName);
            pb.redirectErrorStream(true);
            Process process = pb.start();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()))) {
                String line = reader.readLine();
                process.waitFor();
                if (line != null && !line.isBlank()) {
                    // Take the first digest if multiple are returned
                    return line.trim().split("\\s+")[0];
                }
            }
        } catch (Exception e) {
            System.err.printf("[PodmanDriver][V2] Could not obtain digest for image %s: %s%n",
                    imageName, e.getMessage());
        }
        return null;
    }

    /**
     * Waits for a container to become responsive by polling its {@code /v1/documentation} endpoint.
     * Retries up to 30 times with a 1-second delay between attempts.
     *
     * @param containerURL the base URL of the container (e.g., {@code http://localhost:32768})
     * @param timeoutMs    maximum total wait time in milliseconds
     * @throws PipelineComponentException if the container does not respond within the timeout
     */
    private void waitForContainerResponsive(String containerURL, int timeoutMs)
            throws PipelineComponentException {
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
                    System.out.printf("[PodmanDriver][V2] Container %s responsive after %d attempt(s)%n",
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
                        format("Interrupted while waiting for container %s to become responsive",
                                containerURL),
                        e);
            }
        }
        throw new PipelineComponentException(
                format("Container %s did not become responsive within %d ms", containerURL,
                        timeoutMs));
    }

    public static class Component {
        private DUUIPipelineComponent _component;

        public Component(String target) throws URISyntaxException, IOException {
            _component = new DUUIPipelineComponent();
            _component.withDockerImageName(target);
        }

        public Component(DUUIPipelineComponent pComponent) throws URISyntaxException, IOException {
            _component = pComponent;
        }

        public DUUIPodmanDriver.Component withParameter(String key, String value) {
            _component.withParameter(key, value);
            return this;
        }

        public DUUIPodmanDriver.Component withView(String viewName) {
            _component.withView(viewName);
            return this;
        }

        public DUUIPodmanDriver.Component withSourceView(String viewName) {
            _component.withSourceView(viewName);
            return this;
        }

        public DUUIPodmanDriver.Component withTargetView(String viewName) {
            _component.withTargetView(viewName);
            return this;
        }

        public DUUIPodmanDriver.Component withDescription(String description) {
            _component.withDescription(description);
            return this;
        }

        /**
         * Start the given number of parallel instances (containers).
         * @param scale Number of containers to start.
         * @return {@code this}
         */
        public DUUIPodmanDriver.Component withScale(int scale) {
            _component.withScale(scale);
            return this;
        }

        /**
         * Set the maximum concurrency-level of each component by instantiating the multiple replicas per container.
         * @param workers Number of replicas per container.
         * @return {@code this}
         */
        public DUUIPodmanDriver.Component withWorkers(int workers) {
            _component.withWorkers(workers);
            return this;
        }

        public DUUIPodmanDriver.Component withTimeout(long seconds) {
            _component.withTimeout(seconds);
            return this;
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

        public DUUIPodmanDriver.Component withEnv(String... envString) {
            _component.withEnv(envString);
            return this;
        }

        public DUUIPodmanDriver.Component withRunningAfterDestroy(boolean run) {
            _component.withDockerRunAfterExit(run);
            return this;
        }

        public DUUIPodmanDriver.Component withSegmentationStrategy(DUUISegmentationStrategy strategy) {
            _component.withSegmentationStrategy(strategy);
            return this;
        }

        public <T extends DUUISegmentationStrategy> DUUIPodmanDriver.Component withSegmentationStrategy(Class<T> strategyClass) throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
            _component.withSegmentationStrategy(strategyClass.getDeclaredConstructor().newInstance());
            return this;
        }

        public DUUIPipelineComponent build() {
            _component.withDriver(DUUIPodmanDriver.class);
            return _component;
        }

        public DUUIPodmanDriver.Component withName(String name) {
            _component.withName(name);
            return this;
        }
    }

    private static Map<String, String> podmanEnv(DUUIPipelineComponent component) {
        Map<String, String> env = podmanEnvWithoutGpuDefaults(component);
        if (component.getDockerGPU(false)) {
            String devices = podmanGpuDevicesSpec(env);
            env.putIfAbsent("NVIDIA_VISIBLE_DEVICES", devices);
            env.putIfAbsent("CUDA_VISIBLE_DEVICES", devices);
        }
        return env;
    }

    private static Map<String, String> podmanEnvWithoutGpuDefaults(DUUIPipelineComponent component) {
        Map<String, String> env = new HashMap<>();
        for (String entry : component.getEnv()) {
            int split = entry.indexOf('=');
            if (split > 0) {
                env.put(entry.substring(0, split), entry.substring(split + 1));
            }
        }
        return env;
    }

    private static List<Integer> podmanGpuDevices(DUUIPipelineComponent component) {
        String spec = podmanGpuDevicesSpec(podmanEnvWithoutGpuDefaults(component));
        List<Integer> devices = new ArrayList<>();
        for (String part : spec.split(",")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                devices.add(Integer.parseInt(trimmed));
            }
        }
        return devices.isEmpty() ? List.of(0) : devices;
    }

    private static String podmanGpuDevicesSpec(Map<String, String> env) {
        String configured = env.getOrDefault("NVIDIA_VISIBLE_DEVICES", env.get("CUDA_VISIBLE_DEVICES"));
        if (configured == null || configured.isBlank() || configured.equalsIgnoreCase("all")) {
            configured = System.getProperty("duui.podman.gpu.devices");
        }
        if (configured == null || configured.isBlank() || configured.equalsIgnoreCase("all")) {
            configured = System.getenv("DUUI_PODMAN_GPU_DEVICES");
        }
        return configured == null || configured.isBlank() || configured.equalsIgnoreCase("all") ? "0" : configured;
    }

}
