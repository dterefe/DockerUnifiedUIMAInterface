package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;

public interface IDUUIUrlAccessible {
    public String generateURL();
    public IDUUIConnectionHandler getHandler();
    public IDUUICommunicationLayer getCommunicationLayer();
}
