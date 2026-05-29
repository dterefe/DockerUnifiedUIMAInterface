package org.texttechnologylab.duui.clients.docker;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.BuildImageCmd;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.command.InspectImageResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.AuthConfig;
import com.github.dockerjava.api.model.ContainerNetwork;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.api.model.Statistics;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import org.texttechnologylab.duui.clients.handle.DUUIAddress;
import org.texttechnologylab.duui.clients.DUUIClient;
import org.texttechnologylab.duui.clients.handle.DUUIProxy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

public final class DUUIDockerClient implements DUUIClient<DUUIProxy> {
    private final DockerClient docker;
    public DUUIDockerClient() {
        this(defaultDockerClient(), defaultDockerHost());
    }

    DUUIDockerClient(DockerClient docker) {
        this(docker, defaultDockerHost());
    }

    DUUIDockerClient(DockerClient docker, URI host) {
        this.docker = Objects.requireNonNull(docker, "docker");
    }

    DockerClient docker() {
        return docker;
    }

    public Image image(String reference) {
        return new Image(reference);
    }

    public Container container(String id) {
        InspectContainerResponse inspected = docker.inspectContainerCmd(Objects.requireNonNull(id, "id")).exec();
        String imageReference = inspected.getConfig() == null ? inspected.getImageId() : inspected.getConfig().getImage();
        return new Container(inspected.getId() == null ? id : inspected.getId(), image(imageReference));
    }

    public Registry registry() {
        return new Registry(null);
    }

    Registry registry(AuthConfig auth) {
        return new Registry(auth);
    }

    public Registry registry(String username, String password) {
        return registry(auth(username, password, null, null));
    }

    public Registry registry(String username, String password, String email, String serverAddress) {
        return registry(auth(username, password, email, serverAddress));
    }

    @Override
    public DUUIProxy proxy(DUUIAddress address) {
        Objects.requireNonNull(address, "address");
        return switch (Objects.requireNonNull(address.authority(), "address.authority")) {
            case "image" -> image(pathValue(address));
            case "container" -> container(pathValue(address));
            case "registry" -> registry();
            default -> throw new IllegalArgumentException("Unsupported Docker proxy address: " + address.value());
        };
    }

    @Override
    public void shutdown() {
        try {
            docker.close();
        } catch (IOException e) {
            throw new IllegalStateException("Could not shutdown Docker client", e);
        }
    }

    public final class Image implements DUUIProxy {
        private final String reference;

        private Image(String reference) {
            this.reference = Objects.requireNonNull(reference, "reference");
        }

        public DUUIDockerClient client() {
            return DUUIDockerClient.this;
        }

        @Override
        public DUUIAddress address() {
            return DUUIDockerClient.address("image", reference);
        }

        public String reference() {
            return reference;
        }

        InspectImageResponse inspect() {
            return docker.inspectImageCmd(reference).exec();
        }

        Optional<InspectImageResponse> inspectIfExists() {
            try {
                return Optional.of(inspect());
            } catch (NotFoundException ignored) {
                return Optional.empty();
            }
        }

        public boolean exists() {
            return inspectIfExists().isPresent();
        }

        public String id() {
            return inspect().getId();
        }

        public List<String> tags() {
            List<String> tags = inspect().getRepoTags();
            return tags == null ? List.of() : List.copyOf(tags);
        }

        public List<String> digests() {
            List<String> digests = inspect().getRepoDigests();
            return digests == null ? List.of() : List.copyOf(digests);
        }

        public Long size() {
            return inspect().getSize();
        }

        public Instant createdAt() {
            return parseInstant(inspect().getCreated()).orElse(null);
        }

        public Image tag(String repository, String tag) {
            return tag(repository, tag, true);
        }

        public Image tag(String repository, String tag, boolean force) {
            docker.tagImageCmd(reference, repository, tag).withForce(force).exec();
            return image(tag == null || tag.isBlank() ? repository : repository + ":" + tag);
        }

        public Image remove() {
            return remove(false, false);
        }

        public Image remove(boolean force, boolean noPrune) {
            docker.removeImageCmd(reference).withForce(force).withNoPrune(noPrune).exec();
            return this;
        }

        public Container create() {
            return create(command -> { });
        }

        Container create(Consumer<CreateContainerCmd> configure) {
            CreateContainerCmd command = docker.createContainerCmd(reference);
            if (configure != null) {
                configure.accept(command);
            }
            CreateContainerResponse response = command.exec();
            return container(response.getId());
        }

        public Container run() {
            return run(command -> { });
        }

        Container run(Consumer<CreateContainerCmd> configure) {
            return create(configure).start();
        }

        @Override
        public String toString() {
            return reference;
        }
    }

    public final class Registry implements DUUIProxy {
        private final AuthConfig auth;

        private Registry(AuthConfig auth) {
            this.auth = auth;
        }

        public DUUIDockerClient client() {
            return DUUIDockerClient.this;
        }

        @Override
        public DUUIAddress address() {
            return DUUIDockerClient.address("registry", null);
        }

        Optional<AuthConfig> auth() {
            return Optional.ofNullable(auth);
        }

        public Image image(String reference) {
            return DUUIDockerClient.this.image(reference);
        }

        public Image pull(String reference) throws InterruptedException {
            var command = docker.pullImageCmd(reference);
            if (auth != null) {
                command.withAuthConfig(auth);
            }
            command.start().awaitCompletion();
            return image(reference);
        }

        public Image push(Image image) throws InterruptedException {
            Objects.requireNonNull(image, "image");
            var command = docker.pushImageCmd(image.reference());
            if (auth != null) {
                command.withAuthConfig(auth);
            }
            command.start().awaitCompletion();
            return image;
        }

        public Image push(String reference) throws InterruptedException {
            return push(image(reference));
        }

        public Image build(Path context, String tag) throws InterruptedException {
            return build(context, context.resolve("Dockerfile"), Set.of(tag), Map.of(), Map.of(), command -> { });
        }

        public Image build(
                Path context,
                Path dockerfile,
                Set<String> tags,
                Map<String, String> buildArgs,
                Map<String, String> labels,
                Consumer<BuildImageCmd> configure
        ) throws InterruptedException {
            Objects.requireNonNull(context, "context");
            BuildImageCmd command = docker.buildImageCmd()
                    .withBaseDirectory(context.toFile())
                    .withDockerfile(dockerfile == null ? context.resolve("Dockerfile").toFile() : dockerfile.toFile())
                    .withPull(true)
                    .withRemove(true);
            if (tags != null && !tags.isEmpty()) {
                command.withTags(new LinkedHashSet<>(tags));
            }
            if (buildArgs != null) {
                buildArgs.forEach(command::withBuildArg);
            }
            if (labels != null && !labels.isEmpty()) {
                command.withLabels(labels);
            }
            if (configure != null) {
                configure.accept(command);
            }
            String imageId = command.start().awaitImageId();
            if (tags != null && !tags.isEmpty()) {
                return image(tags.iterator().next());
            }
            return image(imageId);
        }

        public Stream<Image> images() {
            return docker.listImagesCmd().withShowAll(true).exec().stream()
                    .flatMap(image -> references(image).stream())
                    .map(DUUIDockerClient.this::image);
        }

        Stream<Image> images(Consumer<com.github.dockerjava.api.command.ListImagesCmd> configure) {
            var command = docker.listImagesCmd();
            if (configure != null) {
                configure.accept(command);
            }
            return command.exec().stream().flatMap(image -> references(image).stream()).map(DUUIDockerClient.this::image);
        }

        public InputStream save(Image image) {
            return docker.saveImageCmd(Objects.requireNonNull(image, "image").reference()).exec();
        }

        public void save(Image image, OutputStream output) throws IOException {
            try (InputStream input = save(image)) {
                input.transferTo(output);
            }
        }

        public void load(InputStream imageTar) {
            docker.loadImageCmd(Objects.requireNonNull(imageTar, "imageTar")).exec();
        }

        public Registry withoutAuth() {
            return new Registry(null);
        }

        Registry withAuth(AuthConfig auth) {
            return new Registry(auth);
        }

        public Registry withAuth(String username, String password) {
            return withAuth(DUUIDockerClient.auth(username, password, null, null));
        }

        public Registry withAuth(String username, String password, String email, String serverAddress) {
            return withAuth(DUUIDockerClient.auth(username, password, email, serverAddress));
        }
    }

    public final class Container implements DUUIProxy {
        private final String id;
        private final Image image;

        private Container(String id, Image image) {
            this.id = Objects.requireNonNull(id, "id");
            this.image = Objects.requireNonNull(image, "image");
        }

        public DUUIDockerClient client() {
            return DUUIDockerClient.this;
        }

        @Override
        public DUUIAddress address() {
            return DUUIDockerClient.address("container", id);
        }

        public String id() {
            return id;
        }

        InspectContainerResponse inspect() {
            return docker.inspectContainerCmd(id).exec();
        }

        Optional<InspectContainerResponse> inspectIfExists() {
            try {
                return Optional.of(inspect());
            } catch (NotFoundException ignored) {
                return Optional.empty();
            }
        }

        public boolean exists() {
            return inspectIfExists().isPresent();
        }

        public Image image() {
            return image;
        }

        public String name() {
            String name = inspect().getName();
            return name == null ? null : name.replaceFirst("^/", "");
        }

        public Instant createdAt() {
            return parseInstant(inspect().getCreated()).orElse(null);
        }

        public boolean running() {
            InspectContainerResponse inspected = inspect();
            return inspected.getState() != null && Boolean.TRUE.equals(inspected.getState().getRunning());
        }

        public Integer exitCode() {
            return inspect().getState() == null ? null : inspect().getState().getExitCode();
        }

        Ports ports() {
            return inspect().getNetworkSettings() == null ? null : inspect().getNetworkSettings().getPorts();
        }

        Map<String, ContainerNetwork> networks() {
            Map<String, ContainerNetwork> networks = inspect().getNetworkSettings() == null
                    ? null
                    : inspect().getNetworkSettings().getNetworks();
            return networks == null ? Map.of() : Map.copyOf(networks);
        }

        Optional<Ports.Binding[]> bindings(ExposedPort port) {
            Ports ports = ports();
            if (ports == null || ports.getBindings() == null) {
                return Optional.empty();
            }
            return Optional.ofNullable(ports.getBindings().get(port));
        }

        public Container start() {
            docker.startContainerCmd(id).exec();
            return this;
        }

        public Container stop() {
            docker.stopContainerCmd(id).exec();
            return this;
        }

        public Container stop(int timeoutSeconds) {
            docker.stopContainerCmd(id).withTimeout(timeoutSeconds).exec();
            return this;
        }

        public Container restart() {
            docker.restartContainerCmd(id).exec();
            return this;
        }

        public Container restart(int timeoutSeconds) {
            docker.restartContainerCmd(id).withtTimeout(timeoutSeconds).exec();
            return this;
        }

        public Container kill() {
            docker.killContainerCmd(id).exec();
            return this;
        }

        public Container kill(String signal) {
            docker.killContainerCmd(id).withSignal(signal).exec();
            return this;
        }

        public Container pause() {
            docker.pauseContainerCmd(id).exec();
            return this;
        }

        public Container unpause() {
            docker.unpauseContainerCmd(id).exec();
            return this;
        }

        public Container rename(String name) {
            docker.renameContainerCmd(id).withName(name).exec();
            return this;
        }

        public Container remove() {
            return remove(false, false);
        }

        public Container remove(boolean force, boolean removeVolumes) {
            docker.removeContainerCmd(id).withForce(force).withRemoveVolumes(removeVolumes).exec();
            return this;
        }

        public Integer waitUntilNotRunning() throws InterruptedException {
            return docker.waitContainerCmd(id).start().awaitStatusCode();
        }

        ExecCreateCmdResponse exec(List<String> command) {
            return docker.execCreateCmd(id).withCmd(command.toArray(new String[0])).exec();
        }

        <T extends ResultCallback<Frame>> T exec(String execId, T callback) {
            return docker.execStartCmd(execId).exec(callback);
        }

        <T extends ResultCallback<Frame>> T logs(T callback) {
            return docker.logContainerCmd(id).withStdOut(true).withStdErr(true).exec(callback);
        }

        <T extends ResultCallback<Statistics>> T stats(T callback) {
            return docker.statsCmd(id).exec(callback);
        }

        public InputStream copyArchiveFrom(String path) {
            return docker.copyArchiveFromContainerCmd(id, path).exec();
        }

        public Container copyArchiveTo(String remotePath, Path hostResource) {
            docker.copyArchiveToContainerCmd(id).withRemotePath(remotePath).withHostResource(hostResource.toString()).exec();
            return this;
        }

        public Container connectToNetwork(String networkId) {
            docker.connectToNetworkCmd().withContainerId(id).withNetworkId(networkId).exec();
            return this;
        }

        public Container disconnectFromNetwork(String networkId) {
            docker.disconnectFromNetworkCmd().withContainerId(id).withNetworkId(networkId).exec();
            return this;
        }

        public Image commit(String repository, String tag) {
            String imageId = docker.commitCmd(id).withRepository(repository).withTag(tag).exec();
            return DUUIDockerClient.this.image(repository == null ? imageId : repository + (tag == null ? "" : ":" + tag));
        }

        @Override
        public String toString() {
            return id;
        }
    }

    static AuthConfig auth(String username, String password, String email, String serverAddress) {
        AuthConfig auth = new AuthConfig();
        auth.withUsername(username);
        auth.withPassword(password);
        if (email != null) {
            auth.withEmail(email);
        }
        if (serverAddress != null) {
            auth.withRegistryAddress(serverAddress);
        }
        return auth;
    }

    private static List<String> references(com.github.dockerjava.api.model.Image image) {
        List<String> references = new ArrayList<>();
        if (image.getRepoTags() != null) {
            for (String tag : image.getRepoTags()) {
                if (tag != null && !"<none>:<none>".equals(tag)) {
                    references.add(tag);
                }
            }
        }
        if (references.isEmpty() && image.getRepoDigests() != null) {
            references.addAll(List.of(image.getRepoDigests()));
        }
        if (references.isEmpty() && image.getId() != null) {
            references.add(image.getId());
        }
        return references;
    }

    private static DockerClient defaultDockerClient() {
        DefaultDockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder().build();
        if (System.getProperty("os.name", "").contains("Windows")) {
            try {
                DockerHttpClient http = new ApacheDockerHttpClient.Builder()
                        .dockerHost(URI.create("npipe:////./pipe/docker_engine"))
                        .connectionTimeout(Duration.ofSeconds(30))
                        .responseTimeout(Duration.ofMinutes(10))
                        .build();
                return DockerClientBuilder.getInstance(config).withDockerHttpClient(http).build();
            } catch (RuntimeException ignored) {
                DockerHttpClient http = new ApacheDockerHttpClient.Builder()
                        .dockerHost(URI.create("tcp://127.0.0.1:2375"))
                        .connectionTimeout(Duration.ofSeconds(30))
                        .responseTimeout(Duration.ofMinutes(10))
                        .build();
                return DockerClientBuilder.getInstance(config).withDockerHttpClient(http).build();
            }
        }
        DockerHttpClient http = new ApacheDockerHttpClient.Builder()
                .dockerHost(config.getDockerHost())
                .sslConfig(config.getSSLConfig())
                .maxConnections(100)
                .connectionTimeout(Duration.ofSeconds(30))
                .responseTimeout(Duration.ofMinutes(10))
                .build();
        return DockerClientImpl.getInstance(config, http);
    }

    private static URI defaultDockerHost() {
        return DefaultDockerClientConfig.createDefaultConfigBuilder().build().getDockerHost();
    }

    private static DUUIAddress address(String authority, String value) {
        String path = value == null || value.isBlank() ? "" : "/" + value;
        return new DUUIAddress("docker", authority, path, null, null);
    }

    private static String pathValue(DUUIAddress address) {
        String path = Objects.requireNonNull(address.path(), "address.path");
        return path.startsWith("/") ? path.substring(1) : path;
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
