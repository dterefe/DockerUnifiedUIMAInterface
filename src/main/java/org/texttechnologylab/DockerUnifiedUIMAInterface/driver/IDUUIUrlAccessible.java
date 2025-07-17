package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;

/**
 * Interface for the generation of DUUI-available URLs
 * @author Alexander Leonhardt
 */
public interface IDUUIUrlAccessible {
    /**
     * Returns the URL of the DUUI-component.
     * @return
     */
    public String generateURL();

    /**
     * Returns the communication layer.
     * @see org.luaj.vm2.Lua
     * @return
     */
    public IDUUICommunicationLayer getCommunicationLayer();
}
