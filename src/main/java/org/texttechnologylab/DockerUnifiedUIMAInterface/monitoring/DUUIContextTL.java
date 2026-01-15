package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIContext.*;

public final class DUUIContextTL {
    public static final ThreadLocal<DUUIComposer> COMPOSER = new ThreadLocal<>();
    public static final ThreadLocal<DocumentContext> DOCUMENT = new ThreadLocal<>();

    private DUUIContextTL() {
    }

    public static void clear() {
        COMPOSER.remove();
        DOCUMENT.remove();
    }
}
