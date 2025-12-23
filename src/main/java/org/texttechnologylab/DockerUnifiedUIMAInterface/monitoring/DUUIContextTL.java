package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIContext.*;

public final class DUUIContextTL {
    public static final ThreadLocal<ComposerContext> COMPOSER = new ThreadLocal<>();
    public static final ThreadLocal<DocumentProcessContext> DOCUMENT = new ThreadLocal<>();

    private DUUIContextTL() {
    }

    public static void clear() {
        COMPOSER.remove();
        DOCUMENT.remove();
    }
}
