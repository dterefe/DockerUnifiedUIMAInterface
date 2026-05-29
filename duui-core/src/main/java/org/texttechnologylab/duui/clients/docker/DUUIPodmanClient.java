package org.texttechnologylab.duui.clients.docker;

import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.texttechnologylab.duui.clients.handle.DUUIAddress;
import org.texttechnologylab.duui.clients.DUUIClient;
import org.texttechnologylab.duui.clients.handle.DUUIProxy;
import podman.client.PodmanClient;
import podman.client.containers.ContainerDeleteOptions;
import podman.client.containers.ContainerGetLogsOptions;
import podman.client.containers.ContainerInspectOptions;
import podman.client.containers.ContainerTopOptions;
import podman.client.containers.MultiplexedStreamFrame;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class DUUIPodmanClient implements DUUIClient<DUUIProxy> {
    private final PodmanClient podman;
    private final DUUIDockerClient docker;
    private final Vertx vertx;
    private final boolean ownsVertx;
    private final String socketPath;

    public DUUIPodmanClient() {
        this(defaultVertx(), defaultOptions(), true);
    }

    public DUUIPodmanClient(String socketPath) {
        this(defaultVertx(), new PodmanClient.Options().setSocketPath(socketPath), true);
    }

    DUUIPodmanClient(Vertx vertx, PodmanClient.Options options) {
        this(vertx, options, false);
    }

    DUUIPodmanClient(PodmanClient podman, String socketPath) {
        this.podman = Objects.requireNonNull(podman, "podman");
        this.docker = podmanDockerClient(socketPath);
        this.vertx = null;
        this.ownsVertx = false;
        this.socketPath = socketPath;
    }

    private DUUIPodmanClient(Vertx vertx, PodmanClient.Options options, boolean ownsVertx) {
        this.vertx = Objects.requireNonNull(vertx, "vertx");
        PodmanClient.Options effectiveOptions = options == null ? defaultOptions() : options;
        this.podman = PodmanClient.create(vertx, effectiveOptions);
        this.docker = podmanDockerClient(effectiveOptions.getSocketPath());
        this.ownsVertx = ownsVertx;
        this.socketPath = effectiveOptions.getSocketPath();
    }

    PodmanClient podman() {
        return podman;
    }

    public String socketPath() {
        return socketPath;
    }

    public DUUIDockerClient.Image image(String reference) {
        return docker.image(reference);
    }

    public Container container(String id) {
        JsonObject inspected = await(podman.containers().inspect(
                Objects.requireNonNull(id, "id"),
                new ContainerInspectOptions().setSize(false)));
        String imageReference = firstString(inspected, "ImageName")
                .or(() -> firstString(inspected, "Image"))
                .or(() -> firstString(inspected, "ImageID"))
                .orElse("unknown");
        String containerId = firstString(inspected, "Id").orElse(id);
        return new Container(containerId, image(imageReference));
    }

    public DUUIDockerClient.Registry registry() {
        return docker.registry();
    }

    public DUUIDockerClient.Registry registry(String username, String password) {
        return docker.registry(username, password);
    }

    public DUUIDockerClient.Registry registry(String username, String password, String email, String serverAddress) {
        return docker.registry(username, password, email, serverAddress);
    }

    @Override
    public DUUIProxy proxy(DUUIAddress address) {
        Objects.requireNonNull(address, "address");
        return switch (Objects.requireNonNull(address.authority(), "address.authority")) {
            case "image" -> image(pathValue(address));
            case "container" -> container(pathValue(address));
            case "registry" -> registry();
            default -> throw new IllegalArgumentException("Unsupported Podman proxy address: " + address.value());
        };
    }

    @Override
    public void shutdown() {
        docker.shutdown();
        await(podman.close());
        if (ownsVertx && vertx != null) {
            await(vertx.close());
        }
    }

    public final class Container implements DUUIProxy {
        private final String id;
        private final DUUIDockerClient.Image image;

        private Container(String id, DUUIDockerClient.Image image) {
            this.id = Objects.requireNonNull(id, "id");
            this.image = Objects.requireNonNull(image, "image");
        }

        public DUUIPodmanClient client() {
            return DUUIPodmanClient.this;
        }

        @Override
        public DUUIAddress address() {
            return DUUIPodmanClient.address("container", id);
        }

        public String id() {
            return id;
        }

        public DUUIDockerClient.Image image() {
            return image;
        }

        public boolean exists() {
            return await(podman.containers().exists(id));
        }

        JsonObject inspect() {
            return inspect(new ContainerInspectOptions().setSize(false));
        }

        JsonObject inspect(ContainerInspectOptions options) {
            return await(podman.containers().inspect(id, options == null ? new ContainerInspectOptions() : options));
        }

        public String name() {
            return firstString(inspect(), "Name").map(name -> name.replaceFirst("^/", "")).orElse(id);
        }

        public Instant createdAt() {
            return firstString(inspect(), "Created").flatMap(DUUIPodmanClient::parseInstant).orElse(null);
        }

        JsonObject state() {
            return inspect().getJsonObject("State", new JsonObject());
        }

        public boolean running() {
            JsonObject state = state();
            Boolean running = state.getBoolean("Running");
            if (running != null) {
                return running;
            }
            String status = state.getString("Status");
            return status != null && status.equalsIgnoreCase("running");
        }

        public Integer exitCode() {
            return state().getInteger("ExitCode");
        }

        JsonObject portBindings() {
            JsonObject inspected = inspect();
            JsonObject networkSettings = inspected.getJsonObject("NetworkSettings");
            if (networkSettings != null && networkSettings.getJsonObject("Ports") != null) {
                return networkSettings.getJsonObject("Ports");
            }
            JsonObject map = inspected.getJsonObject("map");
            if (map != null && map.getJsonObject("HostConfig") != null) {
                return map.getJsonObject("HostConfig").getJsonObject("PortBindings", new JsonObject());
            }
            return new JsonObject();
        }

        Optional<JsonArray> bindings(String containerPortProtocol) {
            return Optional.ofNullable(portBindings().getJsonArray(containerPortProtocol));
        }

        public Optional<Integer> hostPort(String containerPortProtocol) {
            JsonArray bindings = portBindings().getJsonArray(containerPortProtocol);
            if (bindings == null || bindings.isEmpty()) {
                return Optional.empty();
            }
            Object value = bindings.getJsonObject(0).getValue("HostPort");
            if (value instanceof Number number) {
                return Optional.of(number.intValue());
            }
            if (value instanceof String string && !string.isBlank()) {
                return Optional.of(Integer.parseInt(string));
            }
            return Optional.empty();
        }

        JsonObject top() {
            return top(new ContainerTopOptions());
        }

        JsonObject top(ContainerTopOptions options) {
            return await(podman.containers().top(id, options == null ? new ContainerTopOptions() : options));
        }

        Flow.Publisher<MultiplexedStreamFrame> logs() {
            return logs(new ContainerGetLogsOptions());
        }

        Flow.Publisher<MultiplexedStreamFrame> logs(ContainerGetLogsOptions options) {
            return podman.containers().logs(id, options == null ? new ContainerGetLogsOptions() : options);
        }

        public Container start() {
            await(podman.containers().start(id));
            return this;
        }

        public Container start(String detachKeys) {
            await(podman.containers().start(id, detachKeys));
            return this;
        }

        public Container stop() {
            return stop(false, 10);
        }

        public Container stop(boolean ignoreIfStopped, int timeoutSeconds) {
            await(podman.containers().stop(id, ignoreIfStopped, timeoutSeconds));
            return this;
        }

        public Container restart() {
            return restart(10);
        }

        public Container restart(int timeoutSeconds) {
            await(podman.containers().restart(id, timeoutSeconds));
            return this;
        }

        public Container pause() {
            await(podman.containers().pause(id));
            return this;
        }

        public Container unpause() {
            await(podman.containers().unpause(id));
            return this;
        }

        public Container kill() {
            await(podman.containers().kill(id));
            return this;
        }

        public Container kill(String signal) {
            await(podman.containers().kill(id, signal));
            return this;
        }

        JsonArray delete() {
            return delete(new ContainerDeleteOptions());
        }

        JsonArray delete(ContainerDeleteOptions options) {
            return await(podman.containers().delete(id, options == null ? new ContainerDeleteOptions() : options));
        }

        @Override
        public String toString() {
            return id;
        }
    }

    private static PodmanClient.Options defaultOptions() {
        return new PodmanClient.Options().setSocketPath(defaultSocketPath());
    }

    private static Vertx defaultVertx() {
        return Vertx.vertx(new VertxOptions().setPreferNativeTransport(true));
    }

    private static DUUIDockerClient podmanDockerClient(String socketPath) {
        String effectiveSocketPath = socketPath == null || socketPath.isBlank() ? defaultSocketPath() : socketPath;
        URI host = URI.create("unix://" + effectiveSocketPath);
        DefaultDockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder()
                .withDockerHost(host.toString())
                .build();
        DockerHttpClient http = new ApacheDockerHttpClient.Builder()
                .dockerHost(host)
                .sslConfig(config.getSSLConfig())
                .build();
        return new DUUIDockerClient(DockerClientBuilder.getInstance(config).withDockerHttpClient(http).build(), host);
    }

    private static DUUIAddress address(String authority, String value) {
        String path = value == null || value.isBlank() ? "" : "/" + value;
        return new DUUIAddress("podman", authority, path, null, null);
    }

    private static String pathValue(DUUIAddress address) {
        String path = Objects.requireNonNull(address.path(), "address.path");
        return path.startsWith("/") ? path.substring(1) : path;
    }

    public static String defaultSocketPath() {
        String path = System.getenv("PODMAN_SOCKET_PATH");
        if (path != null && !path.isBlank()) {
            return path;
        }
        String uid = System.getenv("UID");
        if (uid == null || uid.isBlank()) {
            uid = readUid();
        }
        return "/run/user/" + uid + "/podman/podman.sock";
    }

    private static String readUid() {
        try {
            Process process = new ProcessBuilder("id", "-u").start();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String uid = reader.readLine();
                if (uid != null && !uid.isBlank()) {
                    return uid;
                }
            }
        } catch (IOException ignored) {
        }
        return "0";
    }

    private static <T> T await(Future<T> future) {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<T> result = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        future.onComplete(done -> {
            if (done.succeeded()) {
                result.set(done.result());
            } else {
                failure.set(done.cause());
            }
            latch.countDown();
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for Podman operation", e);
        }
        if (failure.get() != null) {
            throw new IllegalStateException("Podman operation failed", failure.get());
        }
        return result.get();
    }

    static <T> void awaitPublisher(Flow.Publisher<T> publisher) throws InterruptedException {
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        publisher.subscribe(new Flow.Subscriber<>() {
            private Flow.Subscription subscription;

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                this.subscription = subscription;
                subscription.request(1);
            }

            @Override
            public void onNext(T item) {
                subscription.request(1);
            }

            @Override
            public void onError(Throwable throwable) {
                failure.set(throwable);
                done.countDown();
            }

            @Override
            public void onComplete() {
                done.countDown();
            }
        });
        while (!done.await(1, TimeUnit.MINUTES)) {
            // Keep waiting so long pulls are not treated as failed work.
        }
        if (failure.get() != null) {
            throw new IllegalStateException("Podman stream failed", failure.get());
        }
    }

    private static Optional<String> firstString(JsonObject object, String key) {
        if (object == null) {
            return Optional.empty();
        }
        Object value = object.getValue(key);
        return value == null ? Optional.empty() : Optional.of(value.toString());
    }

    private static Optional<Instant> parseInstant(String value) {
        if (value == null || value.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(Instant.parse(value));
        } catch (RuntimeException ignored) {
            return Optional.empty();
        }
    }
}
