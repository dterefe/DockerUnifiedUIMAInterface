package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUICompressionHelper;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;

/**
 *
 * @author Alexander Leonhardt
 */
public class DUUIRemoteDriver extends DUUIRestDriver<DUUIRemoteDriver, DUUIRemoteDriver.InstantiatedComponent> {
    private Map<String, InstantiatedComponent> _components;
    private HttpClient _client;
    private DUUICompressionHelper _helper;

    public DUUIRemoteDriver(int timeout) {
        _timeout = Duration.ofMillis(timeout);
        _client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(timeout)).build();

        _components = new HashMap<>();
        _helper = new DUUICompressionHelper(CompressorStreamFactory.ZSTANDARD);
    }

    public DUUIRemoteDriver() {
        _components = new HashMap<>();
        _timeout = Duration.ofMillis(10_000);
        _client = HttpClient.newHttpClient();
        _helper = new DUUICompressionHelper(CompressorStreamFactory.ZSTANDARD);
    }

    @Override
    protected Map<String, InstantiatedComponent> getActiveComponents() {
        return _components;
    }

    @Override
    public boolean canAccept(DUUIPipelineComponent component) {
        List<String> urls = component.getUrl();
        return urls != null && !urls.isEmpty();
    }

    @Override
    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification, AtomicBoolean shutdown) throws Exception {
        String uuid = UUID.randomUUID().toString();
        while (_components.containsKey(uuid)) {
            uuid = UUID.randomUUID().toString();
        }
        InstantiatedComponent comp = new InstantiatedComponent(component, uuid);

        final String uuidCopy = uuid;
        boolean added_communication_layer = false;
        int endpointIndex = 0;

        for (String url : comp.getUrls()) {
            if (shutdown.get()) return null;

            endpointIndex++;
            String prefix = String.format("[DUUIRemoteDriver][%s][Endpoint %d/%d]", uuidCopy.substring(0, 5) + "...", endpointIndex, comp.getUrls().size());

            DUUICommunicationLayerRequestContext requestContext = new DUUICommunicationLayerRequestContext(
                url,
                jc,
                _timeout,
                _client,
                _luaContext,
                skipVerification,
                prefix
            );

            IDUUICommunicationLayer layer = get_communication_layer(requestContext);

            // Request to get input_output
            // {"inputs": ["de.sentence.tudarmstadt",...], "outputs": ["de.sentence.token",...]}
            // /v1/details/input_output
            if (!added_communication_layer) {
                added_communication_layer = true;
            }
            for (int i = 0; i < comp.getWorkers(); i++) {
                String instanceIdentifier = "%s-%s-Endpoint-%d-Worker-%d".formatted(
                    comp.getName(),
                    uuidCopy.substring(0, 5),
                    endpointIndex,
                    i + 1 
                );
                comp.addComponent(new ComponentInstance(instanceIdentifier, url, layer.copy()));
            }
            _components.put(uuid, comp);
            System.out.printf("[RemoteDriver][%s] Remote URL %s is online and seems to understand DUUI V1 format!\n", uuid, url);

            System.out.printf("[RemoteDriver][%s] Maximum concurrency for this endpoint %d\n", uuid, comp.getWorkers());
        }

        return shutdown.get() ? null : uuid;
    }
    
    @Override
    public void shutdown() {
    }

    @Override
    public boolean destroy(String uuid) {
        _components.remove(uuid);
        return true;
    }

    public static class Component extends IDUUIDriverInterface.ComponentBuilder<Component>  {

        public Component(String url) throws URISyntaxException, IOException {
            super(new DUUIPipelineComponent());
            _component.withUrl(url);
        }

        public Component(String... url) throws URISyntaxException, IOException {
            super(new DUUIPipelineComponent());
            List<String> pList = new ArrayList<>();
            pList.addAll(Arrays.asList(url));
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
        @Override
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


    private static record ComponentInstance(
            String _identifier,
            String _url, 
            IDUUICommunicationLayer _communicationLayer
    ) implements IDUUIUrlAccessible {
        @Override
        public String getUniqueInstanceKey() {
            return _identifier;
        }

        @Override
        public String generateURL() {
            return _url;
        }

        @Override
        public IDUUICommunicationLayer getCommunicationLayer() {
            return _communicationLayer;
        }
    }

    protected static class InstantiatedComponent extends DUUIRestDriver.IDUUIInstantiatedRestComponent<InstantiatedComponent> {
        
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

}