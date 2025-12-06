package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.InvalidXMLException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.CommunicationLayerException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.PipelineComponentException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.xml.sax.SAXException;

/**
 * Interface for all drivers
 *
 * @author Alexander Leonhardt
 */
public interface IDUUIDriverInterface {
    /**
     * Method for defining the Lua context to be used, which determines the transfer type between Composer and components.
     * @see DUUILuaContext
     * @param luaContext
     */
    public void setLuaContext(DUUILuaContext luaContext);

    /**
     * Method for checking whether the selected component can be used via the driver.
     * @param component
     * @return
     * @throws InvalidXMLException
     * @throws IOException
     * @throws SAXException
     */
    public boolean canAccept(DUUIPipelineComponent component) throws InvalidXMLException, IOException, SAXException;

    /**
     * Initialisation method
     * @param component
     * @param jc
     * @param skipVerification
     * @param shutdown
     * @return
     * @throws Exception
     */
    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification, AtomicBoolean shutdown) throws Exception;

    /**
     * Visualisation of the concurrency
     * @param uuid
     */
    public void printConcurrencyGraph(String uuid);

    //TODO: public InputOutput get_inputs_and_outputs(String uuid)
    //Example: get_typesystem(...)

    /**
     * Returns the TypeSystem used for the respective component.
     * @see TypeSystemDescription
     * @param uuid
     * @return
     * @throws InterruptedException
     * @throws IOException
     * @throws SAXException
     * @throws CompressorException
     * @throws ResourceInitializationException
     */
    public TypeSystemDescription get_typesystem(String uuid) throws InterruptedException, IOException, SAXException, CompressorException, ResourceInitializationException;

    /**
     * Initializes a Reader Component
     * @param uuid
     * @param filePath
     * @return
     * @throws Exception
     */
    public int initReaderComponent(String uuid, Path filePath) throws Exception;

    /**
     * Starting a component.
     * @param uuid
     * @param aCas
     * @param perf
     * @param composer
     * @throws CASException
     * @throws PipelineComponentException
     */
    public void run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf, DUUIComposer composer) throws CASException, PipelineComponentException, CompressorException, IOException, InterruptedException, SAXException, CommunicationLayerException;

    /**
     * Destruction of a component
     * @param uuid
     * @return
     */
    public boolean destroy(String uuid);

    /**
     * Shutting down the driver
     */
    public void shutdown();

    abstract static class ComponentBuilder<Builder extends ComponentBuilder<Builder>> {
        final protected DUUIPipelineComponent _component;

        protected ComponentBuilder(DUUIPipelineComponent component) {
            _component = component;
        }

        @SuppressWarnings("unchecked")
        public Builder withDescription(String description) {
            _component.withDescription(description);
            return (Builder) this;
        }
        
        @SuppressWarnings("unchecked")
        public Builder withName(String name) {
            _component.withName(name);
            return (Builder) this;
        }

        @SuppressWarnings("unchecked")
        public Builder withParameter(String key, String value) {
            _component.withParameter(key, value);
            return (Builder) this;
        }

        @SuppressWarnings("unchecked")
        public Builder withScale(int scale) {
            _component.withScale(scale);
            return (Builder) this;
        }
        
        @SuppressWarnings("unchecked")
        public Builder withWorkers(int workers) {
            _component.withWorkers(workers);
            return (Builder) this;
        }

        @SuppressWarnings("unchecked")
        public Builder withView(String view) {
            _component.withView(view);
            return (Builder) this;
        }

        @SuppressWarnings("unchecked")
        public Builder withSourceView(String sourceView) {
            _component.withSourceView(sourceView);
            return (Builder) this;
        }

        @SuppressWarnings("unchecked")
        public Builder withTargetView(String targetView) {
            _component.withTargetView(targetView);
            return (Builder) this;
        }

        @SuppressWarnings("unchecked")
        public Builder withSegmentationStrategy(DUUISegmentationStrategy strategy) {
            _component.withSegmentationStrategy(strategy);
            return (Builder) this;
        }

        @SuppressWarnings("unchecked")
        public <T extends DUUISegmentationStrategy> Builder withSegmentationStrategy(Class<T> strategyClass) 
            throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
            _component.withSegmentationStrategy(strategyClass.getDeclaredConstructor().newInstance());
            return (Builder) this;
        }

        @Deprecated
        @SuppressWarnings("unchecked")
        public Builder withWebsocket(boolean websocket) {
            _component.withWebsocket(websocket);
            return (Builder) this;
        }

        @Deprecated
        @SuppressWarnings("unchecked")
        public Builder withWebsocket(boolean websocket, int elements) {
            _component.withWebsocket(websocket, elements);
            return (Builder) this;
        }
    }


}
