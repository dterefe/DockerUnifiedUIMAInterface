package org.texttechnologylab.duui.clients.hosts.virtualization;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.core.DockerClientImpl;
import org.texttechnologylab.duui.clients.handle.DUUIAddress;
import org.texttechnologylab.duui.timelines.DUUIFlow;
import org.texttechnologylab.duui.timelines.DUUIStatus;
import org.texttechnologylab.duui.timelines.Phase;

import java.io.File;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

public class DUUIDockerClient extends DUUIVirtualizationClient<DUUIDockerClient.Container, DUUIDockerClient.Image> {
    private final DockerClient docker;

    public DUUIDockerClient() {
        this(DockerClientImpl.getInstance(), DUUIAddress.parse("docker://local"));
    }

    protected DUUIDockerClient(DockerClient docker, DUUIAddress address) {
        super(address);
        this.docker = docker;
    }

    @Override
    public Image image(String reference) {
        return new Image(imageAddress(reference), reference, 0L, null);
    }

    @Override
    public Container container(String id) throws DUUIVirtualizationException {
        try {
            InspectContainerResponse inspected = docker.inspectContainerCmd(id).exec();
            return containerFrom(id, inspected);
        } catch (RuntimeException e) {
            throw new DUUIContainerInspectException("Failed to inspect Docker container " + id, e);
        }
    }

    @Override
    public Stream<Container> containers() throws DUUIVirtualizationException {
        try {
            return docker.listContainersCmd().withShowAll(true).exec().stream()
                .map(container -> {
                    String id = container.getId();
                    return new Container(
                        containerAddress(id),
                        id,
                        image(container.getImage()),
                        Instant.ofEpochSecond(container.getCreated())
                    );
                });
        } catch (RuntimeException e) {
            throw new DUUIContainerInspectException("Failed to list Docker containers", e);
        }
    }

    protected final DockerClient docker() {
        return docker;
    }

    protected DUUIAddress imageAddress(String reference) {
        return new DUUIAddress("docker-image", null, "/" + reference, null, null);
    }

    protected DUUIAddress containerAddress(String id) {
        return new DUUIAddress("docker-container", null, "/" + id, null, null);
    }

    protected Container containerFrom(String id, InspectContainerResponse inspected) {
        String imageRef = inspected.getConfig() == null ? null : inspected.getConfig().getImage();
        return new Container(
            containerAddress(id),
            id,
            image(imageRef == null ? inspected.getImageId() : imageRef),
            inspected.getCreated() == null ? null : Instant.parse(inspected.getCreated())
        );
    }

    public final class Image extends DUUIContainerImage {
        private Image(DUUIAddress address, String reference, long size, Instant createdAt) {
            super(address, reference, size, createdAt);
        }

        @Phase(DUUIStatus.RUN)
        public DUUIFlow<Container> run(List<String> command) {
            try {
                String id = docker.createContainerCmd(reference())
                    .withCmd(command)
                    .exec()
                    .getId();
                docker.startContainerCmd(id).exec();
                return DUUIFlow.dispatch(container(id));
            } catch (RuntimeException | DUUIVirtualizationException e) {
                return DUUIFlow.fail(new DUUIContainerRunException("Failed to run Docker image " + reference(), e));
            }
        }

        @Phase(DUUIStatus.PULL)
        public DUUIFlow<DUUIContainerImage> pull() {
            try {
                docker.pullImageCmd(reference()).start().awaitCompletion();
                return DUUIFlow.dispatch(this);
            } catch (RuntimeException | InterruptedException e) {
                Thread.currentThread().interrupt();
                return DUUIFlow.fail(new DUUIContainerImageException("Failed to pull Docker image " + reference(), e));
            }
        }

        @Phase(DUUIStatus.PUSH)
        public DUUIFlow<DUUIContainerImage> push() {
            try {
                docker.pushImageCmd(reference()).start().awaitCompletion();
                return DUUIFlow.dispatch(this);
            } catch (RuntimeException | InterruptedException e) {
                Thread.currentThread().interrupt();
                return DUUIFlow.fail(new DUUIContainerImageException("Failed to push Docker image " + reference(), e));
            }
        }

        @Phase(DUUIStatus.BUILD)
        public DUUIFlow<DUUIContainerImage> build(String context) {
            try {
                String imageId = docker.buildImageCmd(new File(context)).start().awaitImageId();
                return DUUIFlow.dispatch(image(imageId));
            } catch (RuntimeException e) {
                return DUUIFlow.fail(new DUUIContainerBuildException("Failed to build Docker image from " + context, e));
            }
        }
    }

    public final class Container extends DUUIContainer {
        private Container(DUUIAddress address, String id, DUUIContainerImage image, Instant createdAt) {
            super(address, id, image, createdAt);
        }

        @Phase(DUUIStatus.PING)
        public DUUIFlow<Boolean> running() {
            try {
                Boolean running = docker.inspectContainerCmd(id()).exec().getState().getRunning();
                return DUUIFlow.dispatch(Boolean.TRUE.equals(running));
            } catch (RuntimeException e) {
                return DUUIFlow.fail(new DUUIContainerInspectException("Failed to inspect Docker container " + id(), e));
            }
        }

        @Phase(DUUIStatus.START)
        public DUUIFlow<DUUIContainer> start() {
            try {
                docker.startContainerCmd(id()).exec();
                return DUUIFlow.dispatch(this);
            } catch (RuntimeException e) {
                return DUUIFlow.fail(new DUUIContainerStartException("Failed to start Docker container " + id(), e));
            }
        }

        @Phase(DUUIStatus.STOP)
        public DUUIFlow<DUUIContainer> stop() {
            try {
                docker.stopContainerCmd(id()).exec();
                return DUUIFlow.dispatch(this);
            } catch (RuntimeException e) {
                return DUUIFlow.fail(new DUUIContainerStopException("Failed to stop Docker container " + id(), e));
            }
        }

        @Phase(DUUIStatus.RESTART)
        public DUUIFlow<DUUIContainer> restart() {
            try {
                docker.stopContainerCmd(id()).exec();
                docker.startContainerCmd(id()).exec();
                return DUUIFlow.dispatch(this);
            } catch (RuntimeException e) {
                return DUUIFlow.fail(new DUUIContainerStopException("Failed to restart Docker container " + id(), e));
            }
        }

        @Phase(DUUIStatus.DELETE)
        public DUUIFlow<Void> delete() {
            try {
                docker.removeContainerCmd(id()).withForce(true).exec();
                return DUUIFlow.dispatch();
            } catch (RuntimeException e) {
                return DUUIFlow.fail(new DUUIContainerDeleteException("Failed to delete Docker container " + id(), e));
            }
        }
    }
}
