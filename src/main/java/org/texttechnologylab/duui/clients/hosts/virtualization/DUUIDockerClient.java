package org.texttechnologylab.duui.clients.hosts.virtualization;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.core.DockerClientImpl;
import org.texttechnologylab.duui.clients.handle.DUUIAddress;

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

        @Override
        public Container run(List<String> command) throws DUUIVirtualizationException {
            try {
                String id = docker.createContainerCmd(reference())
                    .withCmd(command)
                    .exec()
                    .getId();
                docker.startContainerCmd(id).exec();
                return container(id);
            } catch (RuntimeException e) {
                throw new DUUIContainerRunException("Failed to run Docker image " + reference(), e);
            }
        }

        @Override
        public DUUIContainerImage pull() throws DUUIVirtualizationException {
            try {
                docker.pullImageCmd(reference()).start().awaitCompletion();
                return this;
            } catch (RuntimeException | InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DUUIContainerImageException("Failed to pull Docker image " + reference(), e);
            }
        }

        @Override
        public DUUIContainerImage push() throws DUUIVirtualizationException {
            try {
                docker.pushImageCmd(reference()).start().awaitCompletion();
                return this;
            } catch (RuntimeException | InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DUUIContainerImageException("Failed to push Docker image " + reference(), e);
            }
        }

        @Override
        public DUUIContainerImage build(String context) throws DUUIVirtualizationException {
            try {
                String imageId = docker.buildImageCmd(new File(context)).start().awaitImageId();
                return image(imageId);
            } catch (RuntimeException e) {
                throw new DUUIContainerBuildException("Failed to build Docker image from " + context, e);
            }
        }
    }

    public final class Container extends DUUIContainer {
        private Container(DUUIAddress address, String id, DUUIContainerImage image, Instant createdAt) {
            super(address, id, image, createdAt);
        }

        @Override
        public boolean running() throws DUUIVirtualizationException {
            try {
                Boolean running = docker.inspectContainerCmd(id()).exec().getState().getRunning();
                return Boolean.TRUE.equals(running);
            } catch (RuntimeException e) {
                throw new DUUIContainerInspectException("Failed to inspect Docker container " + id(), e);
            }
        }

        @Override
        public DUUIContainer start() throws DUUIVirtualizationException {
            try {
                docker.startContainerCmd(id()).exec();
                return this;
            } catch (RuntimeException e) {
                throw new DUUIContainerStartException("Failed to start Docker container " + id(), e);
            }
        }

        @Override
        public DUUIContainer stop() throws DUUIVirtualizationException {
            try {
                docker.stopContainerCmd(id()).exec();
                return this;
            } catch (RuntimeException e) {
                throw new DUUIContainerStopException("Failed to stop Docker container " + id(), e);
            }
        }

        @Override
        public DUUIContainer restart() throws DUUIVirtualizationException {
            stop();
            return start();
        }

        @Override
        public void delete() throws DUUIVirtualizationException {
            try {
                docker.removeContainerCmd(id()).withForce(true).exec();
            } catch (RuntimeException e) {
                throw new DUUIContainerDeleteException("Failed to delete Docker container " + id(), e);
            }
        }
    }
}
