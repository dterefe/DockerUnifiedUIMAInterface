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
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.DUUIWebsocketAlt;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.CommunicationLayerException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.PipelineComponentException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.texttechnologylab.duui.clients.http.DUUIHttpEndpoint;
import org.texttechnologylab.duui.clients.http.IDUUIEndpoint;
import org.texttechnologylab.duui.pipeline.component.DUUIComponent;
import org.texttechnologylab.duui.pipeline.component.DUUINode;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Annotator;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Config;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 * @author Alexander Leonhardt
 */
public class DUUIRemoteDriver extends DUUIV1Driver {
    private IDUUIConnectionHandler _wsclient = null;
    private DUUICompressionHelper _helper;


    public static class Component {
        private DUUIPipelineComponent component;

        public Component(String url) throws URISyntaxException, IOException {
            component = new DUUIPipelineComponent();
            component.withUrl(url);
        }

        public Component(String... url) throws URISyntaxException, IOException {
            component = new DUUIPipelineComponent();
            List<String> pList = new ArrayList<>();
            for (String s : url) {
                pList.add(s);
            }
            component.withUrls(pList);
        }

        public Component(List<String> urls) throws URISyntaxException, IOException {
            component = new DUUIPipelineComponent();
            component.withUrls(urls);
        }

        public Component(DUUIPipelineComponent pComponent) throws URISyntaxException, IOException {
            component = pComponent;
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
            component.withWorkers(scale);
            return this;
        }

        /**
         * Set the maximum concurrency-level for this component by instantiating the given number of replicas per URL.
         * @param workers Number of replicas per given URL.
         * @return {@code this}
         */
        public Component withWorkers(int workers) {
            component.withWorkers(workers);
            return this;
        }

        public Component withIgnoring200Error(boolean bValue) {
            component.withIgnoringHTTP200Error(bValue);
            return this;
        }

        public Component withDescription(String description) {
            component.withDescription(description);
            return this;
        }

        public Component withParameter(String key, String value) {
            component.withParameter(key, value);
            return this;
        }

        public Component withView(String viewName) {
            component.withView(viewName);
            return this;
        }

        public Component withSourceView(String viewName) {
            component.withSourceView(viewName);
            return this;
        }

        public Component withTargetView(String viewName) {
            component.withTargetView(viewName);
            return this;
        }

        public Component withWebsocket(boolean b) {
            component.withWebsocket(b);
            return this;
        }

        public Component withWebsocket(boolean b, int elements) {
            component.withWebsocket(b, elements);
            return this;
        }

        public Component withSegmentationStrategy(DUUISegmentationStrategy strategy) {
            component.withSegmentationStrategy(strategy);
            return this;
        }

        public <T extends DUUISegmentationStrategy> Component withSegmentationStrategy(Class<T> strategyClass) throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
            component.withSegmentationStrategy(strategyClass.getDeclaredConstructor().newInstance());
            return this;
        }

        public DUUIPipelineComponent build() {
            component.withDriver(DUUIRemoteDriver.class);
            return component;
        }

        public Component withName(String name) {
            component.withName(name);
            return this;
        }
    }

    private static class ComponentInstance implements IDUUIUrlAccessible {
        String _url;
        IDUUIConnectionHandler _handler;
        IDUUICommunicationLayer _communication_layer;

        ComponentInstance(String val, IDUUICommunicationLayer layer) {
            _url = val;
            _communication_layer = layer;
            _handler = null;
        }

        ComponentInstance(String val, IDUUICommunicationLayer layer, IDUUIConnectionHandler handler) {
            _url = val;
            _handler = handler;
            _communication_layer = layer;
        }

        public IDUUICommunicationLayer getCommunicationLayer() {
            return _communication_layer;
        }

        public String generateURL() {
            return _url;
        }

        public IDUUIConnectionHandler getHandler() {
            return _handler;
        }
    }

    private static class InstantiatedComponent implements IDUUIInstantiatedPipelineComponent {
        private final List<String> _urls;
        private int _maximum_concurrency;
        private ConcurrentLinkedQueue<ComponentInstance> _components;
        private final String _uniqueComponentKey;
        private Map<String, String> _parameters;
        private String _sourceView;
        private String _targetView;
        private final DUUIPipelineComponent _component;
        private boolean _websocket;
        private int _ws_elements;

        public Triplet<IDUUIUrlAccessible, Long, Long> getComponent() {
            long mutexStart = System.nanoTime();
            ComponentInstance inst = _components.poll();
            while (inst == null) {
                inst = _components.poll();
            }
            long mutexEnd = System.nanoTime();
            return Triplet.with(inst, mutexStart, mutexEnd);
        }

        public void addComponent(IDUUIUrlAccessible item) {
            _components.add((ComponentInstance) item);
        }


        public InstantiatedComponent(DUUIPipelineComponent comp, String uniqueComponentKey) {
            _component = comp;
            _uniqueComponentKey = uniqueComponentKey;
            _urls = comp.getUrl();
            if (_urls == null || _urls.size() == 0) {
                throw new InvalidParameterException("Missing parameter URL in the pipeline component descriptor");
            }

            _parameters = comp.getParameters();
            _targetView = comp.getTargetView();
            _sourceView = comp.getSourceView();

            _maximum_concurrency = comp.getWorkers(1);
            _components = new ConcurrentLinkedQueue<>();
            _websocket = comp.isWebsocket();
            _ws_elements = comp.getWebsocketElements();
        }

        public DUUIPipelineComponent getPipelineComponent() {
            return _component;
        }

        public String getUniqueComponentKey() {
            return _uniqueComponentKey;
        }

        public int getScale() {
            return _maximum_concurrency;
        }

        public int getWorkers() {
            return _maximum_concurrency;
        }

        public List<String> getUrls() {
            return _urls;
        }

        public Map<String, String> getParameters() {
            return _parameters;
        }

        public String getSourceView() {return _sourceView; }

        public String getTargetView() {return _targetView; }

        public boolean isWebsocket() {
            return _websocket;
        }

        public int getWebsocketElements() {
            return _ws_elements;
        }
    }

    public DUUIRemoteDriver(int timeout) {
        super();
        _activeComponents = new HashMap<>();
        _helper = new DUUICompressionHelper(CompressorStreamFactory.ZSTANDARD);
    }

    public DUUIRemoteDriver() {
        super();
        _activeComponents = new HashMap<>();
        _helper = new DUUICompressionHelper(CompressorStreamFactory.ZSTANDARD);
    }

    @Override
    public boolean canAccept(DUUIPipelineComponent component) {
        List<String> urls = component.getUrl();
        return urls != null && urls.size() > 0;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification, AtomicBoolean shutdown) throws Exception {
        String uuid = UUID.randomUUID().toString();
        while (_activeComponents.containsKey(uuid)) {
            uuid = UUID.randomUUID().toString();
        }
        InstantiatedComponent comp = new InstantiatedComponent(component, uuid);

        final String uuidCopy = uuid;
        boolean added_communication_layer = false;

        for (String url : comp.getUrls()) {
            if (shutdown.get()) return null;

            /**
             * @see
             * @edited
             * Dawit Terefe
             *
             * Starts the websocket connection.
             */
            if (comp.isWebsocket()) {
                String websocketUrl = url.replaceFirst("http", "ws")
                    + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS_WEBSOCKET;
                _wsclient = new DUUIWebsocketAlt(websocketUrl, comp.getWebsocketElements());
            }
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
                /**
                 * @see
                 * @edited
                 * Dawit Terefe
                 *
                 * Saves websocket client in ComponentInstance for
                 * retrieval in process_handler-function.
                 */
                comp.addComponent(new ComponentInstance(url, layer.copy(), _wsclient));
            }
            _activeComponents.put(uuid, comp);
            System.out.printf("[RemoteDriver][%s] Remote URL %s is online and seems to understand DUUI V1 format!\n", uuid, url);

            System.out.printf("[RemoteDriver][%s] Maximum concurrency for this endpoint %d\n", uuid, comp.getWorkers());
        }

        return shutdown.get() ? null : uuid;
    }

    @Override
    public void run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf, DUUIComposer composer) throws CASException, PipelineComponentException, CompressorException, IOException, InterruptedException, SAXException, CommunicationLayerException {
        long mutexStart = System.nanoTime();
        InstantiatedComponent comp = (InstantiatedComponent) _activeComponents.get(uuid);

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

    @Override
    public boolean destroy(String uuid) {
        _activeComponents.remove(uuid);
        return true;
    }

    /**
     * V2 instantiation for Remote driver.
     * <p>
     * No container lifecycle — uses the URLs from the component directly.
     * Each URL becomes a {@link DUUIV1Annotator} with the configured concurrency.
     *
     * @param component        the pipeline component describing the remote endpoints and configuration
     * @param jc               a JCas for type system baseline (used only if verification is not skipped)
     * @param skipVerification if {@code true}, skip the pre-verification round-trip
     * @param shutdown         cooperative shutdown flag; checked between endpoint connections
     * @return a fully initialized {@link DUUIComponent}{@code <JCas>} ready for processing
     * @throws Exception if endpoint connection fails or annotator initialization fails
     */
    @Override
    public DUUIComponent<JCas> instantiateV2(DUUIPipelineComponent component, JCas jc, boolean skipVerification,
            AtomicBoolean shutdown) throws Exception {

        List<String> urls = component.getUrl();
        if (urls == null || urls.isEmpty()) {
            throw new InvalidParameterException(
                    "Missing parameter URL in the pipeline component descriptor");
        }

        int workers = component.getWorkers(1);
        String componentId = component.getName() != null ? component.getName() : "remote-component";

        // --- 1. Build annotators from endpoint URLs ---
        List<DUUIV1Annotator> annotators = new ArrayList<>(urls.size());
        int replicaIdx = 0;
        for (String url : urls) {
            if (shutdown.get()) {
                return null;
            }

            String replicaId = componentId + "-replica-" + replicaIdx++;
            IDUUIEndpoint endpoint = new DUUIHttpEndpoint(URI.create(url), _client);
            DUUIV1Config config = new DUUIV1Config(workers,
                    component.getSourceView(), component.getTargetView(), component.getParameters());

            // DUUIV1Annotator constructor fetches documentation, typesystem, and
            // communication layer — this also acts as a readiness check
            DUUIV1Annotator annotator = new DUUIV1Annotator(replicaId, endpoint, config);
            annotators.add(annotator);

            System.out.printf("[RemoteDriver][V2][Endpoint %d/%d] Annotator %s ready (URL %s)%n",
                    replicaIdx, urls.size(), replicaId, url);
        }

        // --- 2. Build DUUIComponent with nodes distributed round-robin ---
        List<DUUINode<JCas>> nodes = new ArrayList<>(urls.size() * workers);
        int slot = 0;
        for (DUUIV1Annotator annotator : annotators) {
            int concurrency = annotator.config().concurrency();
            for (int j = 0; j < concurrency; j++) {
                nodes.add(DUUINode.v1(componentId + "-slot-" + slot++, annotator));
            }
        }

        // No container lifecycle — closeAction is a no-op
        AutoCloseable closeAction = () -> {};

        System.out.printf("[RemoteDriver][V2] Component %s instantiated with %d nodes across %d endpoint(s)%n",
                componentId, nodes.size(), urls.size());

        return new DUUIComponent<>(componentId, nodes, closeAction);
    }
}
