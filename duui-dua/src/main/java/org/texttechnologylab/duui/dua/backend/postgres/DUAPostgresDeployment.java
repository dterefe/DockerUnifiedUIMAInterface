package org.texttechnologylab.duui.dua.backend.postgres;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public record DUAPostgresDeployment(
        String image,
        String containerName,
        String database,
        String username,
        String password,
        int hostPort,
        Optional<Path> dataDirectory
) {
    public static final String DEFAULT_IMAGE = "docker.io/library/postgres:16";
    public static final String DEFAULT_CONTAINER_NAME = "duui-dua-postgres";
    public static final int DEFAULT_HOST_PORT = 55432;

    public DUAPostgresDeployment {
        image = blankDefault(image, DEFAULT_IMAGE);
        containerName = blankDefault(containerName, DEFAULT_CONTAINER_NAME);
        database = blankDefault(database, "dua");
        username = blankDefault(username, "dua");
        password = blankDefault(password, "dua");
        if (hostPort < 1 || hostPort > 65_535) {
            throw new IllegalArgumentException("hostPort must be in 1..65535");
        }
        dataDirectory = dataDirectory == null ? Optional.empty() : dataDirectory;
    }

    public static DUAPostgresDeployment local() {
        return new DUAPostgresDeployment(
                DEFAULT_IMAGE,
                DEFAULT_CONTAINER_NAME,
                "dua",
                "dua",
                "dua",
                DEFAULT_HOST_PORT,
                Optional.empty());
    }

    public DUAPostgresDeployment withDataDirectory(Path directory) {
        return new DUAPostgresDeployment(image, containerName, database, username, password, hostPort,
                Optional.of(Objects.requireNonNull(directory, "directory")));
    }

    public DUAPostgresDeployment withHostPort(int port) {
        return new DUAPostgresDeployment(image, containerName, database, username, password, port,
                dataDirectory);
    }

    public Map<String, String> environment() {
        LinkedHashMap<String, String> env = new LinkedHashMap<>();
        env.put("POSTGRES_DB", database);
        env.put("POSTGRES_USER", username);
        env.put("POSTGRES_PASSWORD", password);
        return env;
    }

    public List<String> environmentList() {
        return environment().entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .toList();
    }

    public String jdbcUrl() {
        return "jdbc:postgresql://127.0.0.1:" + hostPort + "/" + database;
    }

    private static String blankDefault(String value, String defaultValue) {
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
