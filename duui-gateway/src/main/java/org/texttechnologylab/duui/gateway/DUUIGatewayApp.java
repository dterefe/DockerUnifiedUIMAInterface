package org.texttechnologylab.duui.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

public final class DUUIGatewayApp {
    private DUUIGatewayApp() {
    }

    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        String host = System.getProperty("duui.gateway.host", env("DUUI_GATEWAY_HOST", "0.0.0.0"));
        int port = Integer.parseInt(System.getProperty("duui.gateway.port", env("DUUI_GATEWAY_PORT", "8788")));
        Path storagePath = Path.of(System.getProperty(
                "duui.gateway.storage",
                env("DUUI_GATEWAY_STORAGE", ".duui-gateway/gateway-state.json")
        ));
        DUUIGateway gateway = new DUUIGateway(mapper, storagePath);
        Path dashboardRoot = Path.of(System.getProperty(
                "duui.dashboard.dir",
                env("DUUI_DASHBOARD_DIR", "../annotator-testbench/frontend/dist")
        ));
        DUUIGatewayServer server = new DUUIGatewayServer(gateway, mapper, host, port, dashboardRoot);
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        System.out.printf("DUUI Gateway listening on http://%s:%d serving %s with storage %s%n",
                host, port, dashboardRoot.toAbsolutePath().normalize(), storagePath.toAbsolutePath().normalize());
        new CountDownLatch(1).await();
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}
