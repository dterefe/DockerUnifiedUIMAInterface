package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.instances.DUUIInstanceDescriptors;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;

public final class DUUIComponentDescriptors {
    
    public interface IDUUIComponentMeta {
        String uuid();

        Optional<String> name();

        Optional<String> description();

        int replicas();

        int concurrency();

        Map<String, String> parameters();
    }
    
    private DUUIComponentDescriptors() {}

    // ---------------------------
    // Core component
    // ---------------------------

    public interface IDUUIComponentDescriptor<C extends IDUUIComponent>
            extends IDUUIDescriptor<C>, IDUUIComponentMeta {
    }

    public interface IDUUIComponentOptions<O extends IDUUIComponentOptions<O>> {
        O withUuid(String uuid);

        O withName(String name);

        O withDescription(String description);

        O withParameter(String key, String value);
    }

    // ---------------------------
    // Pipeline
    // ---------------------------

    public interface IDUUIPipelineComponentDescriptor extends IDUUIComponentDescriptor<IDUUIPipelineComponent> {
        Optional<String> sourceView();

        Optional<String> targetView();

        Optional<DUUISegmentationStrategy> segmentationStrategy();

        Optional<String> viewName();

        boolean viewInitializeFromInitial();
    }

    public interface IDUUIPipelineOptions<O extends IDUUIPipelineOptions<O>> extends IDUUIComponentOptions<O> {
        O withSourceView(String view);

        O withTargetView(String view);

        O withSegmentationStrategy(DUUISegmentationStrategy segmentationStrategy);

        O withViewName(String viewName);

        O withViewInitializeFromInitial(boolean viewInitializeFromInitial);
    }

    // ---------------------------
    // HTTP
    // ---------------------------

    public interface IDUUIHttpComponentDescriptor<C extends IDUUIComponent> extends IDUUIComponentDescriptor<C> {
        Duration timeout();

        boolean ignoringHttp200();

        DUUIInstanceDescriptors.IDUUIHttpInstanceOptions<?> createHttpInstance();
    }

    public interface IDUUIHttpOptions<O extends IDUUIHttpOptions<O>> extends IDUUIComponentOptions<O> {
        O withTimeout(Duration timeout);

        O withIgnoringHttp200(boolean ignoringHttp200);
    }
    
    // ---------------------------
    // UIMA
    // ---------------------------

    public interface IDUUIUimaComponentDescriptor<C extends IDUUIComponent> extends IDUUIComponentDescriptor<C> {
        Optional<AnalysisEngineDescription> analysisEngineDescription();

        DUUIInstanceDescriptors.IDUUIUimaInstanceOptions<?> createUimaInstance();
    }

    public interface IDUUIUimaOptions<O extends IDUUIUimaOptions<O>> extends IDUUIComponentOptions<O> {
        O withReplicas(int replicas);

        O withAnalysisEngineDescription(AnalysisEngineDescription desc);
    }

    // ---------------------------
    // Remote
    // ---------------------------

    public interface IDUUIRemoteComponentDescriptor<C extends IDUUIComponent> extends IDUUIHttpComponentDescriptor<C> {
        Optional<Iterable<String>> urls();
    }

    public interface IDUUIRemoteOptions<O extends IDUUIRemoteOptions<O>> extends IDUUIHttpOptions<O> {
        O withConcurrency(int concurrency);

        O withUrl(String url);

        O withUrls(Iterable<String> urls);
    }

    // ---------------------------
    // Container
    // ---------------------------

    public interface IDUUIContainerComponentDescriptor<C extends IDUUIComponent> extends IDUUIHttpComponentDescriptor<C> {
        boolean dockerNoShutdown();

        default boolean keepAliveAfterExit() {
            return dockerNoShutdown();
        }

        boolean dockerWithGPU();

        default boolean useGPU() {
            return dockerWithGPU();
        }

        boolean dockerImageFetching();

        default boolean fetchImage() {
            return dockerImageFetching();
        }

        Optional<String> dockerImageName();

        default Optional<String> imageName() {
            return dockerImageName();
        }

        Optional<String> dockerUsername();

        default Optional<String> registryUsername() {
            return dockerUsername();
        }

        Optional<String> dockerPassword();

        default Optional<String> registryPassword() {
            return dockerPassword();
        }

        Map<String, String> dockerEnvironment();

        default Map<String, String> envs() {
            return dockerEnvironment();
        }

        Map<String, String> dockerLabels();

        DUUIInstanceDescriptors.IDUUIContainerInstanceOptions<?> createContainerInstance();
    }

    public interface IDUUIContainerOptions<O extends IDUUIContainerOptions<O>> extends IDUUIHttpOptions<O> {
        
        O withDockerNoShutdown(boolean dockerNoShutdown);

        default O withKeepAliveAfterExit(boolean keepAliveAfterExit) {
            return withDockerNoShutdown(keepAliveAfterExit);
        }

        O withDockerWithGPU(boolean dockerWithGPU);

        default O withGPU(boolean useGPU) {
            return withDockerWithGPU(useGPU);
        }

        O withDockerImageFetching(boolean dockerImageFetching);

        default O withImageFetching(boolean dockerImageFetching) {
            return withDockerImageFetching(dockerImageFetching);
        }

        O withReplicas(int replicas);

        O withConcurrency(int concurrency);

        O withDockerImageName(String dockerImageName);

        default O withImageName(String imageName) {
            return withDockerImageName(imageName);
        }

        O withDockerEnvironmentVariable(String key, String value);

        O withDockerLabel(String key, String value);

        O withDockerUsername(String dockerUsername);

        default O withRegistryUsername(String dockerUsername) {
            return withDockerUsername(dockerUsername);
        }

        O withDockerPassword(String dockerPassword);
    }

    // ---------------------------
    // Kubernetes
    // ---------------------------

    public interface IDUUIKubernetesComponentDescriptor<C extends IDUUIComponent> extends IDUUIContainerComponentDescriptor<C> {
        Map<String, String> kubernetesLabelSelector();
    }

    public interface IDUUIKubernetesOptions<O extends IDUUIKubernetesOptions<O>> extends IDUUIContainerOptions<O> {
        O withK8LabelSelector(String key, String value);
    }

    // ---------------------------
    // Swarm
    // ---------------------------

    public interface IDUUISwarmComponentDescriptor<C extends IDUUIComponent> extends IDUUIContainerComponentDescriptor<C> {
        Map<String, String> swarmConstraints();
    }

    public interface IDUUISwarmOptions<O extends IDUUISwarmOptions<O>> extends IDUUIContainerOptions<O> {
        O withSwarmConstraint(String key, String value);
    }
}
