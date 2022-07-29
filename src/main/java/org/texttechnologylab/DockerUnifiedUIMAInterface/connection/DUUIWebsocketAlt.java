package org.texttechnologylab.DockerUnifiedUIMAInterface.connection;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class DUUIWebsocketAlt implements IDUUIConnectionHandler{

    private static List<DUUIWebsocketAlt> clients = new ArrayList<>();
    private WebsocketClient client;

    public DUUIWebsocketAlt(String uri) throws InterruptedException, IOException {
        this.client = new WebsocketClient(URI.create(uri));
        boolean connected = client.connectBlocking();

        if (!connected) {
            System.out.println("[DUUIWebsocketAlt] Client could not connect!");
            throw new IOException("Could not reach endpoint after 10 tries!");
        }

        DUUIComposer._clients.add(this);
        System.out.println("[DUUIWebsocketAlt] Remote URL %s is online and seems to understand DUUI V1 format!\n"+URI.create(uri));
    }

    public WebsocketClient getClient() {
        return client;
    }

    public byte[] get(byte[] jc) {

        client.send(jc);

        System.out.println("[DUUIWebsocketAlt]: Message sending \n"+
                StandardCharsets.UTF_8.decode(ByteBuffer.wrap(jc)));

        while (client.messageStack.isEmpty()) {
            try {
                Thread.sleep(0, 1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        byte[] result = client.messageStack.get(0);

        System.out.println("[DUUIWebsocketAlt]: Message received \n"+
                StandardCharsets.UTF_8.decode(ByteBuffer.wrap(result)));

        return result;
    }

    public void close() {
        client.close();
    }


}
