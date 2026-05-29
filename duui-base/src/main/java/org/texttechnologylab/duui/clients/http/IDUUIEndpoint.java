package org.texttechnologylab.duui.clients.http;

import java.net.URI;
import java.net.http.HttpClient;

public interface IDUUIEndpoint {
    URI uri();

    HttpClient client();
}
