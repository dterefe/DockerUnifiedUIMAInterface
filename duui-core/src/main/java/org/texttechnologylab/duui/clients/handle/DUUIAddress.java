package org.texttechnologylab.duui.clients.handle;

import java.net.URI;

public record DUUIAddress(
    String scheme,
    String authority,
    String path,
    String query,
    String fragment
) {
    public static DUUIAddress parse(String value) {
        URI uri = URI.create(value);
        return new DUUIAddress(
            uri.getScheme(),
            uri.getAuthority(),
            uri.getPath(),
            uri.getQuery(),
            uri.getFragment()
        );
    }

    public URI uri() {
        return new URIBuilder(scheme, authority, path, query, fragment).uri();
    }

    public String value() {
        return uri().toString();
    }

    private record URIBuilder(String scheme, String authority, String path, String query, String fragment) {
        URI uri() {
            try {
                return new URI(scheme, authority, path, query, fragment);
            } catch (java.net.URISyntaxException e) {
                throw new IllegalArgumentException("Invalid DUUI address", e);
            }
        }
    }
}
