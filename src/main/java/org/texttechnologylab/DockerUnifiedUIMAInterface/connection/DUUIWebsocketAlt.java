package org.texttechnologylab.DockerUnifiedUIMAInterface.connection;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

/**
 * @author Dawit Terefe, Givara Ebo
 *
 * Interface between DUUIComposer and WebsocketClient.
 */
@Deprecated
public class DUUIWebsocketAlt implements IDUUIConnectionHandler{

    public DUUIWebsocketAlt(String uri, int elements) throws InterruptedException, IOException {
        throw new IOException("DUUIWebsocketAlt is deprecated and will be removed in future versions. Please use DUUIWebsocket instead.");
    }

    /**
     * Sends serialized JCAS Object and returns result of analysis.
     *
     * @param jc serialized JCAS Object in bytes.
     * @return List of results.
     */
    public List<ByteArrayInputStream> send(byte[] jc) {
        throw new UnsupportedOperationException("DUUIWebsocketAlt is deprecated and will be removed in future versions. Please use DUUIWebsocket instead.");
    }

    /**
     * Closes connection to websocket.
     */
    public void close() {
        throw new UnsupportedOperationException("DUUIWebsocketAlt is deprecated and will be removed in future versions. Please use DUUIWebsocket instead.");
    }

}
