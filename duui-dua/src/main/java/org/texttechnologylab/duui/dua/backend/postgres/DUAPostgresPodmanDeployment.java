package org.texttechnologylab.duui.dua.backend.postgres;

import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Ports;
import org.texttechnologylab.duui.clients.docker.DUUIDockerClient;
import org.texttechnologylab.duui.clients.docker.DUUIPodmanClient;

import java.util.Objects;

public final class DUAPostgresPodmanDeployment {
    private static final ExposedPort POSTGRES_PORT = ExposedPort.tcp(5432);

    private final DUUIPodmanClient podman;
    private final DUAPostgresDeployment deployment;

    public DUAPostgresPodmanDeployment(DUUIPodmanClient podman, DUAPostgresDeployment deployment) {
        this.podman = Objects.requireNonNull(podman, "podman");
        this.deployment = deployment == null ? DUAPostgresDeployment.local() : deployment;
    }

    public DUUIDockerClient.Container start() {
        DUUIDockerClient.Image image = podman.image(deployment.image());
        return image.run(command -> {
            command.withName(deployment.containerName());
            command.withEnv(deployment.environmentList());
            command.withExposedPorts(POSTGRES_PORT);

            Ports ports = new Ports();
            ports.bind(POSTGRES_PORT, Ports.Binding.bindPort(deployment.hostPort()));
            HostConfig hostConfig = HostConfig.newHostConfig().withPortBindings(ports);
            deployment.dataDirectory().ifPresent(directory ->
                    hostConfig.withBinds(Bind.parse(directory.toAbsolutePath() + ":/var/lib/postgresql/data:Z")));
            command.withHostConfig(hostConfig);
        });
    }

    public DUAPostgresDeployment deployment() {
        return deployment;
    }
}
