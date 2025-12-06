package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.javatuples.Triplet;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUICompressionHelper;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.CommunicationLayerException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.PipelineComponentException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRestDriver.IDUUIInstantiatedRestComponent;

/**
 *
 * @author Alexander Leonhardt
 */
public class DUUIRemoteDriver implements IDUUIDriverInterface {
    private HashMap<String, InstantiatedComponent> _components;
    private HttpClient _client;
    private DUUICompressionHelper _helper;
    private DUUILuaContext _luaContext;


    public static class Component extends IDUUIDriverInterface.ComponentBuilder<Component>  {

        public Component(String url) throws URISyntaxException, IOException {
            super(new DUUIPipelineComponent());
            _component.withUrl(url);
        }

        public Component(String... url) throws URISyntaxException, IOException {
            super(new DUUIPipelineComponent());
            List<String> pList = new ArrayList<>();
            for (String s : url) {
                pList.add(s);
            }
            _component.withUrls(pList);
        }

        public Component(List<String> urls) throws URISyntaxException, IOException {
            super(new DUUIPipelineComponent());
            _component.withUrls(urls);
        }

        public Component(DUUIPipelineComponent pComponent) throws URISyntaxException, IOException {
            super(pComponent);
        }


        /**
         * Set the maximum concurrency-level for this component by instantiating the given number of replicas per URL.
         * @param scale Number of replicas per given URL.
         * @return {@code this}
         * @apiNote Alias for {@link #withWorkers(int)}. To achieve component-level concurrency,
         *      supply multiple (different) URL endpoints using the appropriate constructors:
         *      {@link #Component(String...)} and {@link #Component(List)}.
         */
        public Component withScale(int scale) {
            System.out.printf(
                    "[RemoteDriver] In RemoteDriver.Components, the withScale() method just aliases withWorkers(). " +
                            "To achieve component-level concurrency, supply multiple (different) URL endpoints " +
                            "to the constructor instead!%n"
            );
            _component.withWorkers(scale);
            return this;
        }

        public Component withIgnoring200Error(boolean bValue) {
            _component.withIgnoringHTTP200Error(bValue);
            return this;
        }

        public DUUIPipelineComponent build() {
            _component.withDriver(DUUIRemoteDriver.class);
            return _component;
        }
    }

    public void setLuaContext(DUUILuaContext luaContext) {
        _luaContext = luaContext;
    }

    private static class ComponentInstance implements IDUUIUrlAccessible {
        String _url;
        IDUUICommunicationLayer _communication_layer;

        ComponentInstance(String val, IDUUICommunicationLayer layer) {
            _url = val;
            _communication_layer = layer;
        }

        public IDUUICommunicationLayer getCommunicationLayer() {
            return _communication_layer;
        }

        public String generateURL() {
            return _url;
        }

    }

    private static class InstantiatedComponent extends IDUUIInstantiatedRestComponent<InstantiatedComponent> {
        
        private final List<String> _urls;

        public InstantiatedComponent(DUUIPipelineComponent component, String uniqueComponentKey) {
            super(component, uniqueComponentKey);

            _urls = component.getUrl();
            if (_urls == null || _urls.isEmpty()) {
                throw new InvalidParameterException("Missing parameter URL in the pipeline component descriptor");
            }
        }

        @Override
        public int getScale() {
            return getWorkers();
        }

        public List<String> getUrls() {
            return _urls;
        }

    }

    public DUUIRemoteDriver(int timeout) {
        _client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(timeout)).build();

        _components = new HashMap<String, InstantiatedComponent>();
        _helper = new DUUICompressionHelper(CompressorStreamFactory.ZSTANDARD);
    }

    public DUUIRemoteDriver() {
        _components = new HashMap<String, InstantiatedComponent>();
        _client = HttpClient.newHttpClient();
        _helper = new DUUICompressionHelper(CompressorStreamFactory.ZSTANDARD);
    }

    public boolean canAccept(DUUIPipelineComponent component) {
        List<String> urls = component.getUrl();
        return urls != null && urls.size() > 0;
    }

    public void shutdown() {
    }

    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification, AtomicBoolean shutdown) throws Exception {
        String uuid = UUID.randomUUID().toString();
        while (_components.containsKey(uuid)) {
            uuid = UUID.randomUUID().toString();
        }
        InstantiatedComponent comp = new InstantiatedComponent(component, uuid);

        final String uuidCopy = uuid;
        boolean added_communication_layer = false;

        for (String url : comp.getUrls()) {
            if (shutdown.get()) return null;

            IDUUICommunicationLayer layer = DUUIDockerDriver.responsiveAfterTime(url, jc, 100000, _client, (msg) -> {
                System.out.printf("[RemoteDriver][%s] %s\n", uuidCopy, msg);
            }, _luaContext, skipVerification);
            // Request to get input_output
            // {"inputs": ["de.sentence.tudarmstadt",...], "outputs": ["de.sentence.token",...]}
            // /v1/details/input_output
            if (!added_communication_layer) {
                added_communication_layer = true;
            }
            for (int i = 0; i < comp.getWorkers(); i++) {
                comp.addComponent(new ComponentInstance(url, layer.copy()));
            }
            _components.put(uuid, comp);
            System.out.printf("[RemoteDriver][%s] Remote URL %s is online and seems to understand DUUI V1 format!\n", uuid, url);

            System.out.printf("[RemoteDriver][%s] Maximum concurrency for this endpoint %d\n", uuid, comp.getWorkers());
        }

        return shutdown.get() ? null : uuid;
    }

    public void printConcurrencyGraph(String uuid) {
        InstantiatedComponent component = _components.get(uuid);
        if (component == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        System.out.printf("[RemoteDriver][%s]: Maximum concurrency %d\n", uuid, component.getWorkers());
    }

    public TypeSystemDescription get_typesystem(String uuid) throws InterruptedException, IOException, SAXException, CompressorException, ResourceInitializationException {
        DUUIRemoteDriver.InstantiatedComponent comp = _components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        return IDUUIInstantiatedPipelineComponent.getTypesystem(uuid, comp);
    }

    /**
     * init reader component
     * @param uuid
     * @param filePath
     * @return
     */
    @Override
    public int initReaderComponent(String uuid, Path filePath) {
        DUUIRemoteDriver.InstantiatedComponent comp = _components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        return IDUUIInstantiatedPipelineReaderComponent.initComponent(comp, filePath);
    }

    public void run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf, DUUIComposer composer) throws CASException, PipelineComponentException, CompressorException, IOException, InterruptedException, SAXException, CommunicationLayerException {
        long mutexStart = System.nanoTime();
        InstantiatedComponent comp = _components.get(uuid);

        if (comp == null) {
            throw new InvalidParameterException("The given instantiated component uuid was not instantiated by the remote driver");
        }
        /**
         * @edtited
         * Givara Ebo, Dawit Terefe
         *
         * Added option for websocket-process-function.
         */

        if (comp.isWebsocket()) {
            IDUUIInstantiatedPipelineComponent.process_handler(aCas, comp, perf);
        } else {
            IDUUIInstantiatedPipelineComponent.process(aCas, comp, perf);
        }
    }

    public boolean destroy(String uuid) {
        _components.remove(uuid);
        return true;
    }
}
