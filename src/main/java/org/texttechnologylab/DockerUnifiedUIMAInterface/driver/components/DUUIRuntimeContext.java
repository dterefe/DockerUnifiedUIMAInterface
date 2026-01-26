package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components;

import java.net.ProxySelector;
import java.net.http.HttpClient;
import java.time.Duration;

import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;

public final class DUUIRuntimeContext {

    private final HttpClient httpClient;
    private final DUUILuaContext luaContext;

    public DUUIRuntimeContext(HttpClient httpClient, DUUILuaContext luaContext) {
        this.httpClient = httpClient;
        this.luaContext = luaContext;
    }

    public HttpClient httpClient() {
        return httpClient;
    }

    public DUUILuaContext luaContext() {
        return luaContext;
    }

    public static DUUIRuntimeContext defaultContext() {
        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .followRedirects(HttpClient.Redirect.ALWAYS)
                .proxy(ProxySelector.getDefault())
                .connectTimeout(Duration.ofSeconds(1000))
                .build();

        return new DUUIRuntimeContext(client, new DUUILuaContext());
    }
}
