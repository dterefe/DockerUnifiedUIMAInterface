package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.CASRuntimeException;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.metadata.ConfigurationParameter;
import org.apache.uima.resource.metadata.NameValuePair;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.InvalidXMLException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.PipelineComponentException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUILogger;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUILoggers;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.ClassScopedLogger;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.duui.ReproducibleAnnotation;
import org.xml.sax.SAXException;

/**
 *
 * @author Alexander Leonhardt
 */
public class DUUIUIMADriver implements IDUUIDriverInterface {
    private HashMap<String, InstantiatedComponent> _engines;
    private boolean _enable_debug;
    private static final DUUILogger logger = DUUILoggers.getLogger(DUUIUIMADriver.class);

    public DUUIUIMADriver() {
        _engines = new HashMap<String, InstantiatedComponent>();
        _enable_debug = false;
    }

    @Override
    public DUUILogger logger() {
        return logger;
    }

    @Override
    public void setLogger(DUUILogger delegate) {
        if (logger instanceof ClassScopedLogger scoped) {
            scoped.setDelegate(delegate);
        }
    }

    public void setLuaContext(DUUILuaContext luaContext) {
        // Not needed for the uima driver
    }

    public DUUIUIMADriver withDebug(boolean enableDebug) {
        _enable_debug = enableDebug;
        return this;
    }

    public static class InstantiatedComponent {
        private ConcurrentLinkedQueue<AnalysisEngine> _engines;
        private DUUIPipelineComponent _component;

        public InstantiatedComponent(DUUIPipelineComponent component) {
            _engines = new ConcurrentLinkedQueue<AnalysisEngine>();
            _component = component;
        }

        static boolean isCompatible(DUUIPipelineComponent component) throws InvalidXMLException, IOException, SAXException {
            return component.getEngine() != null;
        }

        public DUUIPipelineComponent getPipelineComponent() {
            return _component;
        }

        public InstantiatedComponent add(AnalysisEngine engine) {
            _engines.add(engine);
            return this;
        }

        public ConcurrentLinkedQueue<AnalysisEngine> getEngines() {
            return _engines;
        }
    }


    public static class Component extends IDUUIDriverInterface.ComponentBuilder<Component> {
        private final AnalysisEngineDescription _engine;

        public Component(AnalysisEngineDescription desc) throws IOException, SAXException, URISyntaxException {
            super(new DUUIPipelineComponent());
            _component.withEngine(desc);
            _engine = desc;
        }

        public Component(DUUIPipelineComponent pComponent) throws IOException, SAXException, URISyntaxException, InvalidXMLException {
            super(pComponent);
            _engine = pComponent.getEngine();
        }

        /**
         * Set the maximum concurrency-level for this component by instantiating the given number of replicas.
         * @param workers Number of replicas.
         * @return {@code this}
         * @apiNote Alias for {@link #withScale(int)}. Inter-component concurrency via
         * {@link org.apache.uima.analysis_engine.impl.MultiprocessingAnalysisEngine_impl MultiprocessingAnalysisEngines}
         * is not yet supported.
         */
        public Component withWorkers(int workers) {
            _component.withScale(workers);
            return this;
        }

        static private String[] extractNames(AnalysisEngineDescription engine, int recursionDepth) throws InvalidXMLException {
            List<String> lst = new ArrayList<String>();
            String offset = "";
            for (int i = 0; i < recursionDepth; i++) {
                offset += "  ";
            }
            if (engine.isPrimitive()) {
                lst.add(offset + engine.getAnnotatorImplementationName());
            } else {
                Map<String, ResourceSpecifier> spec = engine.getDelegateAnalysisEngineSpecifiers();
                for (String x : spec.keySet()) {
                    ResourceSpecifier res = spec.get(x);
                    if (res instanceof AnalysisEngineDescription) {
                        for (String inner : extractNames((AnalysisEngineDescription) res, recursionDepth + 1)) {
                            lst.add(inner);
                        }
                        lst.add("");
                    }
                }
            }
            String[] arr = new String[lst.size()];
            lst.toArray(arr);
            return arr;
        }

        public void describeAnalysisEngine() throws InvalidXMLException {
            String[] names = extractNames(_engine, 0);
            for (String i : names) {
                logger.debug("%s", i);
            }
        }

        public Component setAnalysisEngineParameter(String key, Object value) throws IOException, SAXException {
            _engine.getAnalysisEngineMetaData()
                .getConfigurationParameterSettings()
                .setParameterValue(key, value);
            return this;
        }

        public String getAnnotatorName() {
            if (_engine.isPrimitive()) {
                return _engine.getAnnotatorImplementationName();
            }
            return null;
        }

        public DUUIPipelineComponent build() throws IOException, SAXException {
            _component.withDriver(DUUIUIMADriver.class);
            return _component;
        }

        public Component withName(String name) {
            _component.withName(name);
            return this;
        }
    }

    public boolean canAccept(DUUIPipelineComponent component) throws InvalidXMLException, IOException, SAXException {
        return InstantiatedComponent.isCompatible(component);
    }

    public void printConcurrencyGraph(String uuid) {
        InstantiatedComponent component = _engines.get(uuid);
        if (component == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        logger().info("[UIMADriver][%s]: Maximum concurrency %d\n", uuid, component.getEngines().size());
    }

    String[] extractNames(AnalysisEngineDescription engine, String uuid, int recursionDepth) throws InvalidXMLException {
        List<String> lst = new ArrayList<>();

        logger.debug("[UIMADriver][%s] Dumping annotator layout and parameters:", uuid);
        String offset = "";
        for (int i = 0; i < recursionDepth; i++) {
            offset += "  ";
        }
        if (engine.isPrimitive()) {
            lst.add(offset + engine.getAnnotatorImplementationName());
            offset += " ";
            Map<String, ConfigurationParameter> val = new HashMap<>();

            for (ConfigurationParameter param : engine.getAnalysisEngineMetaData().getConfigurationParameterDeclarations().getConfigurationParameters()) {
                val.put(param.getName(), param);
            }

            for (NameValuePair valuesName : engine.getAnalysisEngineMetaData().getConfigurationParameterSettings().getParameterSettings()) {
                ConfigurationParameter param = val.get(valuesName.getName());
                if (param == null) {
                    continue;
                }
                lst.add(offset + "Name: " + param.getName());
                lst.add(offset + "Type: " + param.getType());
                Object result = valuesName.getValue();
                switch (param.getType()) {
                    case ConfigurationParameter.TYPE_FLOAT -> {
                        if (param.isMultiValued()) {
                            String serialized = "[";
                            for (float inner : (float[]) result) {
                                serialized += String.valueOf(inner);
                            }
                            serialized += "]";
                            lst.add(offset + "Value: " + serialized);
                        } else {
                            lst.add(offset + "Value: " + String.valueOf((float) result));
                        }
                    }
                    case ConfigurationParameter.TYPE_STRING -> {
                        if (param.isMultiValued()) {
                            String serialized = "[";
                            for (String inner : (String[]) result) {
                                serialized += inner;
                            }
                            serialized += "]";
                            lst.add(offset + "Value: " + serialized);
                        } else {
                            lst.add(offset + "Value: " + (String) result);
                        }
                    }
                    case ConfigurationParameter.TYPE_BOOLEAN -> {
                        if (param.isMultiValued()) {
                            String serialized = "[";
                            for (Boolean inner : (Boolean[]) result) {
                                serialized += String.valueOf(inner);
                            }
                            serialized += "]";
                            lst.add(offset + "Value: " + serialized);
                        } else {
                            lst.add(offset + "Value: " + String.valueOf((Boolean) result));
                        }
                    }
                    case ConfigurationParameter.TYPE_INTEGER -> {
                        if (param.isMultiValued()) {
                            String serialized = "[";
                            for (Integer inner : (Integer[]) result) {
                                serialized += String.valueOf(inner);
                            }
                            serialized += "]";
                            lst.add(offset + "Value: " + serialized);
                        } else {
                            lst.add(offset + "Value: " + String.valueOf((Integer) result));
                        }
                    }
                    default -> throw new InvalidXMLException();
                }
                lst.add("");
            }
        } else {
            Map<String, ResourceSpecifier> spec = engine.getDelegateAnalysisEngineSpecifiers();
            for (String x : spec.keySet()) {
                ResourceSpecifier res = spec.get(x);
                if (res instanceof AnalysisEngineDescription) {
                    for (String inner : extractNames((AnalysisEngineDescription) res, uuid, recursionDepth + 1)) {
                        lst.add(inner);
                    }
                    lst.add("");
                }
            }
        }
        String[] arr = new String[lst.size()];
        lst.toArray(arr);
        return arr;
    }

    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification, AtomicBoolean shutdown) throws InterruptedException, TimeoutException, UIMAException, SAXException, IOException {
        String uuid = UUID.randomUUID().toString();
        while ((_engines.containsKey(uuid))) {
            uuid = UUID.randomUUID().toString();
        }
        AnalysisEngineDescription analysis_engine_desc = component.getEngine();
        if (analysis_engine_desc == null) {
            throw new InvalidParameterException("The component does not contain a valid engine!");
        }

        Integer scale = component.getScale();
        if (scale == null) {
            scale = 1;
        }
        logger().info("[UIMADriver] Assigned new pipeline component unique id %s\n", uuid);

        if (_enable_debug) {
            String[] values = extractNames(analysis_engine_desc, uuid, 0);
            for (String x : values) {
                logger().debug(x);
            }
        }
        InstantiatedComponent comp = new InstantiatedComponent(component);
        for (int i = 0; i < scale; i++) {
            if (shutdown.get()) return null;
            AnalysisEngine ana = AnalysisEngineFactory.createEngine(analysis_engine_desc);
            String annotator = analysis_engine_desc.getAnnotatorImplementationName();
            if (annotator != null) {
                if (!skipVerification) {
                    ana.process(jc);
                }
                logger().info("[UIMADriver][%s][Replication %d/%d] Instantiated native UIMA Analysis Engine Annotator %s without problems\n", uuid, i + 1, scale, annotator);
            } else {
                if (!skipVerification) {
                    ana.process(jc);
                }
                logger().info("[UIMADriver][%s][Replication %d/%d] Instantiated native UIMA Analysis Engine without problems\n", uuid, i + 1, scale);
            }
            comp.add(ana);
        }
        _engines.put(uuid, comp);
        return uuid;
    }

    public void shutdown() {
    }

    public TypeSystemDescription get_typesystem(String uuid) throws InterruptedException, IOException, SAXException, CompressorException, ResourceInitializationException {
        return TypeSystemDescriptionFactory.createTypeSystemDescription();
    }

    /**
     * init reader component
     * TODO: is this needed?
     * @param uuid
     * @param filePath
     * @return
     */
    @Override
    public int initReaderComponent(String uuid, Path filePath) {
        return 0;
    }

    public void run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf, DUUIComposer composer) throws CASException, PipelineComponentException {
        long mutexStart = System.nanoTime();

        InstantiatedComponent component = _engines.get(uuid);
        if (component == null) {
            throw new InvalidParameterException("The given instantiated component uuid was not instantiated by the remote driver");
        }
        AnalysisEngine engine = component.getEngines().poll();
        while (engine == null) {
            engine = component.getEngines().poll();
        }
        long mutexEnd = System.nanoTime();
        try {
            long annotatorStart = mutexEnd;
            JCas jc;
            String viewName = component.getPipelineComponent().getViewName();
            if (viewName == null) {
                jc = aCas;
            } else {
                try {
                    jc = aCas.getView(viewName);
                } catch (CASException | CASRuntimeException e) {
                    if (component.getPipelineComponent().getCreateViewFromInitialView()) {
                        jc = aCas.createView(viewName);
                        jc.setDocumentText(aCas.getDocumentText());
                        jc.setDocumentLanguage(aCas.getDocumentLanguage());
                    } else {
                        throw e;
                    }
                }
            }

//            if (composer.shouldShutdown()) return;
            engine.process(jc);
            long annotatorEnd = System.nanoTime();
            ReproducibleAnnotation ann = new ReproducibleAnnotation(jc);
            ann.setDescription(component.getPipelineComponent().getFinalizedRepresentation());
            ann.setCompression(DUUIPipelineComponent.compressionMethod);
            ann.setTimestamp(System.nanoTime());
            ann.setPipelineName(perf.getRunKey());
            ann.addToIndexes();
            perf.addData(0, 0, annotatorEnd - annotatorStart, mutexEnd - mutexStart, annotatorEnd - mutexStart, String.valueOf(component.getPipelineComponent().getFinalizedRepresentationHash()), 0, jc, null);
        } catch (Exception e) {

            // track error docs
            long annotatorStart = mutexEnd;
            long annotatorEnd = System.nanoTime();
            if (perf.shouldTrackErrorDocs()) {
                perf.addData(0, 0, annotatorEnd - annotatorStart, mutexEnd - mutexStart, annotatorEnd - mutexStart, String.valueOf(component.getPipelineComponent().getFinalizedRepresentationHash()), 0, null, ExceptionUtils.getStackTrace(e));
            }

            throw new PipelineComponentException(e);
        } finally {
            component.add(engine);
        }
    }

    public boolean destroy(String uuid) {
        InstantiatedComponent component = _engines.remove(uuid);
        if (component == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        for (AnalysisEngine engine : component.getEngines()) {
            engine.destroy();
        }

        return true;
    }
}
