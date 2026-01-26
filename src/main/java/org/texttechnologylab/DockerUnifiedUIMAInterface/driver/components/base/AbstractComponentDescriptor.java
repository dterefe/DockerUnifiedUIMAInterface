package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.base;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.ArrayList;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.DUUIComponentDescriptors;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.IDUUIComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.DUUIRuntimeContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.instances.DUUIInstanceDescriptors;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIStatus;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;

@SuppressWarnings("unchecked")
public abstract class AbstractComponentDescriptor<C extends IDUUIComponent, SELF extends AbstractComponentDescriptor<C, SELF>>
        implements DUUIComponentDescriptors.IDUUIComponentDescriptor<C>,
        DUUIComponentDescriptors.IDUUIPipelineOptions<SELF>,
        DUUIComponentDescriptors.IDUUIRemoteOptions<SELF>,
        DUUIComponentDescriptors.IDUUIKubernetesOptions<SELF>,
        DUUIComponentDescriptors.IDUUISwarmOptions<SELF>,
        DUUIComponentDescriptors.IDUUIUimaOptions<SELF> {

    protected final SELF self() {
        return (SELF) this;
    }

    protected DUUIRuntimeContext runtimeContext = DUUIRuntimeContext.defaultContext();

    public final DUUIRuntimeContext runtimeContext() {
        return runtimeContext;
    }

    public final SELF withRuntimeContext(DUUIRuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
        return self();
    }

    protected DUUIStatus status = DUUIStatus.INSTANTIATING;
    public final DUUIStatus status() {
        return status;
    }

    protected final void requireMutable() {
        if (status.ordinal() > DUUIStatus.INSTANTIATING.ordinal()) {
            throw new IllegalStateException("component already finalized");
        }
    }

    protected final void markFinalized() {
        status = DUUIStatus.IDLE;
    }

    private String finalizedRepresentation;
    private int finalizedRepresentationHash;

    public final String finalizedRepresentation() {
        return finalizedRepresentation;
    }

    public final int finalizedRepresentationHash() {
        return finalizedRepresentationHash;
    }

    protected final void setFinalizedRepresentation(String encoded) {
        this.finalizedRepresentation = encoded;
        this.finalizedRepresentationHash = encoded != null ? encoded.hashCode() : 0;
    }

    private Class<?> driverClass;

    public final SELF withDriver(Class<?> driver) {
        requireMutable();
        this.driverClass = driver;
        return self();
    }

    public final Optional<Class<?>> driverClass() {
        return Optional.ofNullable(driverClass);
    }

    // ---------------------------
    // Core component
    // ---------------------------

    private String uuid;
    @Override
    public final String uuid() {
        return Optional.ofNullable(uuid).orElseThrow();
    }
    @Override
    public final SELF withUuid(String uuid) {
        requireMutable();
        this.uuid = uuid;
        return self();
    }

    private String name;
    @Override
    public final Optional<String> name() {
        return Optional.ofNullable(name);
    }
    @Override
    public final SELF withName(String name) {
        requireMutable();
        this.name = name;
        return self();
    }

    private String description;
    @Override
    public final Optional<String> description() {
        return Optional.ofNullable(description);
    }
    @Override
    public final SELF withDescription(String description) {
        requireMutable();
        this.description = description;
        return self();
    }

    private int replicas = 1;
    @Override
    public final int replicas() {
        return replicas;
    }
    @Override
    public final SELF withReplicas(int replicas) {
        requireMutable();
        this.replicas = replicas;
        return self();
    }

    private int concurrency = 1;
    @Override
    public final int concurrency() {
        return concurrency;
    }
    @Override
    public final SELF withConcurrency(int concurrency) {
        requireMutable();
        this.concurrency = concurrency;
        return self();
    }

    private final Map<String, String> parameters = new HashMap<>();
    @Override
    public final Map<String, String> parameters() {
        return parameters;
    }
    @Override
    public final SELF withParameter(String key, String value) {
        requireMutable();
        this.parameters.put(key, value);
        return self();
    }

    // ---------------------------
    // Pipeline
    // ---------------------------

    private String sourceView;
    @Override
    public final SELF withSourceView(String view) {
        requireMutable();
        this.sourceView = view;
        return self();
    }
    public final Optional<String> sourceView() {
        return Optional.ofNullable(sourceView);
    }

    private String targetView;
    @Override
    public final SELF withTargetView(String view) {
        requireMutable();
        this.targetView = view;
        return self();
    }
    public final Optional<String> targetView() {
        return Optional.ofNullable(targetView);
    }

    protected Optional<DUUISegmentationStrategy> segmentationStrategy = Optional.empty();
    public final Optional<DUUISegmentationStrategy> segmentationStrategy() {
        return segmentationStrategy;
    }

    @Override
    public final SELF withSegmentationStrategy(DUUISegmentationStrategy segmentationStrategy) {
        requireMutable();
        this.segmentationStrategy = Optional.ofNullable(segmentationStrategy);
        return self();
    }

    protected Optional<String> viewName = Optional.empty();
    public final Optional<String> viewName() {
        return viewName;
    }

    @Override
    public final SELF withViewName(String viewName) {
        requireMutable();
        this.viewName = Optional.ofNullable(viewName);
        return self();
    }

    protected boolean viewInitializeFromInitial;
    public final boolean viewInitializeFromInitial() {
        return viewInitializeFromInitial;
    }

    @Override
    public final SELF withViewInitializeFromInitial(boolean viewInitializeFromInitial) {
        requireMutable();
        this.viewInitializeFromInitial = viewInitializeFromInitial;
        return self();
    }

    // ---------------------------
    // UIMA
    // ---------------------------

    private AnalysisEngineDescription analysisEngineDescription;
    public final Optional<AnalysisEngineDescription> analysisEngineDescription() {
        return Optional.ofNullable(analysisEngineDescription);
    }
    @Override
    public final SELF withAnalysisEngineDescription(AnalysisEngineDescription desc) {
        requireMutable();
        this.analysisEngineDescription = desc;
        return self();
    }
    
    // ---------------------------
    // HTTP
    // ---------------------------

    private Duration timeout;
    public final Duration timeout() {
        return Optional.ofNullable(timeout).orElseThrow();
    }
    @Override
    public final SELF withTimeout(Duration timeout) {
        requireMutable();
        this.timeout = timeout;
        return self();
    }

    private boolean ignoringHttp200;
    public final boolean ignoringHttp200() {
        return ignoringHttp200;
    }
    @Override
    public final SELF withIgnoringHttp200(boolean ignoringHttp200) {
        requireMutable();
        this.ignoringHttp200 = ignoringHttp200;
        return self();
    }

    // ---------------------------
    // Remote
    // ---------------------------

    private Iterable<String> urls;
    public final Optional<Iterable<String>> urls() {
        return Optional.ofNullable(urls);
    }
    @Override
    public final SELF withUrls(Iterable<String> urls) {
        requireMutable();
        this.urls = urls;
        return self();
    }
    @Override
    public final SELF withUrl(String url) {
        requireMutable();
        throw new UnsupportedOperationException();
    }

    // ---------------------------
    // Container
    // ---------------------------

    private String dockerImageName;
    public final Optional<String> dockerImageName() {
        return Optional.ofNullable(dockerImageName);
    }
    @Override
    public final SELF withDockerImageName(String dockerImageName) {
        requireMutable();
        this.dockerImageName = dockerImageName;
        return self();
    }

    private String dockerUsername;
    public final Optional<String> dockerUsername() {
        return Optional.ofNullable(dockerUsername);
    }
    @Override
    public final SELF withDockerUsername(String dockerUsername) {
        requireMutable();
        this.dockerUsername = dockerUsername;
        return self();
    }

    private String dockerPassword;
    public final Optional<String> dockerPassword() {
        return Optional.ofNullable(dockerPassword);
    }
    @Override
    public final SELF withDockerPassword(String dockerPassword) {
        requireMutable();
        this.dockerPassword = dockerPassword;
        return self();
    }

    private boolean dockerWithGPU;
    public final boolean dockerWithGPU() {
        return dockerWithGPU;
    }
    @Override
    public final SELF withDockerWithGPU(boolean dockerWithGPU) {
        requireMutable();
        this.dockerWithGPU = dockerWithGPU;
        return self();
    }

    private boolean dockerImageFetching;
    public final boolean dockerImageFetching() {
        return dockerImageFetching;
    }
    @Override
    public final SELF withDockerImageFetching(boolean dockerImageFetching) {
        requireMutable();
        this.dockerImageFetching = dockerImageFetching;
        return self();
    }

    private boolean dockerNoShutdown;
    public final boolean dockerNoShutdown() {
        return dockerNoShutdown;
    }
    @Override
    public final SELF withDockerNoShutdown(boolean dockerNoShutdown) {
        requireMutable();
        this.dockerNoShutdown = dockerNoShutdown;
        return self();
    }

    private final List<String> env = new ArrayList<>();
    public final List<String> env() {
        return env;
    }

    public final SELF withEnv(String... env) {
        requireMutable();
        this.env.addAll(List.of(env));
        return self();
    }

    private final Map<String, String> dockerEnvironment = new HashMap<>();
    public final Map<String, String> dockerEnvironment() {
        return dockerEnvironment;
    }
    @Override
    public final SELF withDockerEnvironmentVariable(String key, String value) {
        requireMutable();
        dockerEnvironment.put(key, value);
        return self();
    }

    private final Map<String, String> dockerLabels = new HashMap<>();
    public final Map<String, String> dockerLabels() {
        return dockerLabels;
    }
    @Override
    public final SELF withDockerLabel(String key, String value) {
        requireMutable();
        dockerLabels.put(key, value);
        return self();
    }

    private final Map<String, String> kubernetesLabelSelector = new HashMap<>();
    public final Map<String, String> kubernetesLabelSelector() {
        return kubernetesLabelSelector;
    }
    @Override
    public final SELF withK8LabelSelector(String key, String value) {
        requireMutable();
        kubernetesLabelSelector.put(key, value);
        return self();
    }

    // ---------------------------
    // Swarm
    // ---------------------------
    private final List<String> constraints = new ArrayList<>();
    public final List<String> constraints() {
        return constraints;
    }

    public final SELF withConstraints(List<String> constraints) {
        requireMutable();
        this.constraints.addAll(constraints);
        return self();
    }

    public final SELF withConstraint(String constraint) {
        requireMutable();
        this.constraints.add(constraint);
        return self();
    }

    private final Map<String, String> swarmConstraints = new HashMap<>();
    public final Map<String, String> swarmConstraints() {
        return swarmConstraints;
    }
    @Override
    public final SELF withSwarmConstraint(String key, String value) {
        requireMutable();
        swarmConstraints.put(key, value);
        return self();
    }

    public DUUIInstanceDescriptors.IDUUIHttpInstanceOptions<?> createHttpInstance() {
        throw new UnsupportedOperationException();
    }

    public DUUIInstanceDescriptors.IDUUIContainerInstanceOptions<?> createContainerInstance() {
        throw new UnsupportedOperationException();
    }

    public DUUIInstanceDescriptors.IDUUIUimaInstanceOptions<?> createUimaInstance() {
        throw new UnsupportedOperationException();
    }

    
    // ---------------------------
    // Legacy-identical serialization
    // ---------------------------

    public static String compressionMethod = CompressorStreamFactory.XZ;

    private static final String ENGINE_OPTION = "engine";
    private static final String SCALE_OPTION = "scale";
    private static final String WORKERS_OPTION = "workers";

    private static final String IGNORING_200_OPTION = "ignoring200";
    private static final String URL_OPTION = "url";

    private static final String DOCKER_PASSWORD_OPTION = "dockerPassword";
    private static final String DOCKER_USERNAME_OPTION = "dockerUsername";

    private static final String DOCKER_NO_SHUTDOWN_OPTION = "dockerNoShutdown";
    private static final String DOCKER_WITH_GPU_OPTION = "dockerWithGPU";
    private static final String DOCKER_IMAGE_NAME_OPTION = "dockerImageName";
    private static final String DOCKER_IMAGE_FETCHING_OPTION = "dockerImageFetch";

    private static final String VERSION_OPTION = "version";
    private static final String VIEW_NAME_OPTION = "uimaViewName";
    private static final String VIEW_INITIALIZE_FROM_INITIAL_OPTION = "uimaViewInitializeFromInitial";

    private static final String COMPONENT_NAME_OPTION = "name";
    private static final String DRIVER_NAME_OPTION = "driver";
    private static final String DESCRIPTION_OPTION = "description";

    private static final String SOURCE_VIEW_OPTION = "sourceView";
    private static final String TARGET_VIEW_OPTION = "targetView";
    private static final String TIMEOUT_PARAM = "timeout";

    private static final String WEBSOCKET_PARAM = "websocket";

    private static String getVersion() {
        ClassLoader classLoader = AbstractComponentDescriptor.class.getClassLoader();
        try {
            return String.valueOf(classLoader.getResourceAsStream("git.properties"));
        } catch (NullPointerException e) {
            return "undefined";
        }
    }

    protected final Map<String, String> legacyOptions() {
        HashMap<String, String> options = new HashMap<>();

        options.put(VERSION_OPTION, getVersion());

        driverClass().ifPresent(driver -> {
            options.put(DRIVER_NAME_OPTION, driver.getCanonicalName());
            options.put(DRIVER_NAME_OPTION + "_simple", driver.getSimpleName());
        });

        name().ifPresent(v -> options.put(COMPONENT_NAME_OPTION, v));
        description().ifPresent(v -> options.put(DESCRIPTION_OPTION, v));

        options.put(SCALE_OPTION, String.valueOf(replicas()));
        options.put(WORKERS_OPTION, String.valueOf(concurrency()));

        options.put(IGNORING_200_OPTION, String.valueOf(ignoringHttp200()));

        if (urls().isPresent()) {
            JSONArray arr = new JSONArray();
            for (String s : urls().get()) {
                arr.put(s);
            }
            options.put(URL_OPTION, arr.toString());
        }

        if (dockerUsername().isPresent() || dockerPassword().isPresent()) {
            options.put(DOCKER_USERNAME_OPTION, dockerUsername().orElse(null));
            options.put(DOCKER_PASSWORD_OPTION, dockerPassword().orElse(null));
        }

        options.put(DOCKER_NO_SHUTDOWN_OPTION, String.valueOf(dockerNoShutdown()));
        options.put(DOCKER_WITH_GPU_OPTION, String.valueOf(dockerWithGPU()));
        options.put(DOCKER_IMAGE_FETCHING_OPTION, String.valueOf(dockerImageFetching()));
        dockerImageName().ifPresent(v -> options.put(DOCKER_IMAGE_NAME_OPTION, v));

        viewName().ifPresent(v -> {
            options.put(VIEW_NAME_OPTION, v);
            options.put(VIEW_INITIALIZE_FROM_INITIAL_OPTION, String.valueOf(viewInitializeFromInitial()));
        });

        sourceView().ifPresent(v -> options.put(SOURCE_VIEW_OPTION, v));
        targetView().ifPresent(v -> options.put(TARGET_VIEW_OPTION, v));

        analysisEngineDescription().ifPresent(desc -> {
            try {
                StringWriter writer = new StringWriter();
                desc.toXML(writer);
                options.put(ENGINE_OPTION, writer.getBuffer().toString());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        return options;
    }

    protected final Map<String, String> legacyParameters() {
        HashMap<String, String> params = new HashMap<>(parameters());

        params.putIfAbsent(WEBSOCKET_PARAM, "false");

        if (timeout != null) {
            params.put(TIMEOUT_PARAM, String.valueOf(timeout.toSeconds()));
        }

        return params;
    }

    protected final void finalizeLegacyRepresentation() throws Exception {
        JSONObject json = new JSONObject();
        json.put("options", legacyOptions());
        json.put("parameters", legacyParameters());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CompressorOutputStream cos = new CompressorStreamFactory().createCompressorOutputStream(compressionMethod, out);
        cos.write(json.toString().getBytes(StandardCharsets.UTF_8));
        cos.close();

        setFinalizedRepresentation(Base64.getEncoder().encodeToString(out.toByteArray()));
    }
}
