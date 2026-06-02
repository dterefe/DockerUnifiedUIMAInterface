package org.texttechnologylab.duui.clients.kubernetes;

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;
import io.fabric8.kubernetes.api.model.NodeSelectorTerm;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.texttechnologylab.duui.clients.DUUIClient;
import org.texttechnologylab.duui.clients.handle.DUUIAddress;
import org.texttechnologylab.duui.clients.handle.DUUIProxy;
import org.texttechnologylab.duui.timelines.DUUIFlow;
import org.texttechnologylab.duui.timelines.DUUIStatus;
import org.texttechnologylab.duui.timelines.Phase;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Kubernetes client following the same proxy pattern as {@link org.texttechnologylab.duui.clients.docker.DUUIDockerClient}
 * and {@link org.texttechnologylab.duui.clients.docker.DUUIPodmanClient}.
 *
 * <p>Exposes {@link Deployment}, {@link Service}, {@link Pod}, and {@link Namespace} as inner
 * {@link DUUIProxy} classes with full lifecycle methods (create, inspect, delete, list, etc.).
 * Every proxy event is scoped to a {@link DUUIAddress}.</p>
 */
public final class DUUIKubernetesClient implements DUUIClient<DUUIProxy> {

    private static final String DEFAULT_NAMESPACE = "default";
    private static final DateTimeFormatter K8S_TIMESTAMP =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);

    private final KubernetesClient k8s;
    private final boolean ownsClient;

    /**
     * Creates a new client using the default Kubernetes configuration (kubeconfig, service account, etc.).
     */
    public DUUIKubernetesClient() {
        this(new KubernetesClientBuilder().build(), true);
    }

    /**
     * Creates a client that wraps an existing {@link KubernetesClient}.
     * The caller is responsible for closing the provided client unless {@code ownsClient} is {@code true}.
     */
    public DUUIKubernetesClient(KubernetesClient k8s, boolean ownsClient) {
        this.k8s = Objects.requireNonNull(k8s, "k8s");
        this.ownsClient = ownsClient;
    }

    /**
     * Returns the underlying fabric8 {@link KubernetesClient}.
     */
    public KubernetesClient k8s() {
        return k8s;
    }

    // ── Factory helpers ────────────────────────────────────────────────

    /**
     * Returns a {@link Deployment} proxy for the named deployment in the given namespace.
     */
    public Deployment deployment(String namespace, String name) {
        return new Deployment(Objects.requireNonNull(namespace, "namespace"), Objects.requireNonNull(name, "name"));
    }

    /**
     * Returns a {@link Deployment} proxy in the default namespace.
     */
    public Deployment deployment(String name) {
        return deployment(DEFAULT_NAMESPACE, name);
    }

    /**
     * Returns a {@link Service} proxy for the named service in the given namespace.
     */
    public Service service(String namespace, String name) {
        return new Service(Objects.requireNonNull(namespace, "namespace"), Objects.requireNonNull(name, "name"));
    }

    /**
     * Returns a {@link Service} proxy in the default namespace.
     */
    public Service service(String name) {
        return service(DEFAULT_NAMESPACE, name);
    }

    /**
     * Returns a {@link Pod} proxy for the named pod in the given namespace.
     */
    public Pod pod(String namespace, String name) {
        return new Pod(Objects.requireNonNull(namespace, "namespace"), Objects.requireNonNull(name, "name"));
    }

    /**
     * Returns a {@link Pod} proxy in the default namespace.
     */
    public Pod pod(String name) {
        return pod(DEFAULT_NAMESPACE, name);
    }

    /**
     * Returns a {@link Namespace} proxy for the named namespace.
     */
    public Namespace namespace(String name) {
        return new Namespace(Objects.requireNonNull(name, "name"));
    }

    // ── DUUIClient contract ────────────────────────────────────────────

    @Override
    public DUUIProxy proxy(DUUIAddress address) {
        Objects.requireNonNull(address, "address");
        return switch (Objects.requireNonNull(address.authority(), "address.authority")) {
            case "deployment" -> {
                String[] parts = splitPath(address);
                yield deployment(parts[0], parts[1]);
            }
            case "service" -> {
                String[] parts = splitPath(address);
                yield service(parts[0], parts[1]);
            }
            case "pod" -> {
                String[] parts = splitPath(address);
                yield pod(parts[0], parts[1]);
            }
            case "namespace" -> namespace(pathValue(address));
            default -> throw new IllegalArgumentException(
                    "Unsupported Kubernetes proxy address: " + address.value());
        };
    }

    @Override
    public void shutdown() {
        if (ownsClient) {
            k8s.close();
        }
    }

    // ── Inner proxy classes ────────────────────────────────────────────

    /**
     * Kubernetes Deployment proxy with full lifecycle: create, scale, inspect, delete, list pods.
     */
    public final class Deployment implements DUUIProxy {
        private final String namespace;
        private final String name;

        private Deployment(String namespace, String name) {
            this.namespace = namespace;
            this.name = name;
        }

        public DUUIKubernetesClient client() {
            return DUUIKubernetesClient.this;
        }

        @Override
        public DUUIAddress address() {
            return DUUIKubernetesClient.address("deployment", namespace + "/" + name);
        }

        public String namespace() {
            return namespace;
        }

        public String name() {
            return name;
        }

        /**
         * Inspects (fetches) the current state of the deployment from the cluster.
         */
        public io.fabric8.kubernetes.api.model.apps.Deployment inspect() {
            return k8s.apps().deployments().inNamespace(namespace).withName(name).get();
        }

        /**
         * Returns {@code true} if the deployment exists in the cluster.
         */
        public boolean exists() {
            return inspect() != null;
        }

        /**
         * Creates a deployment with the given image, replicas, container port, and node selector labels.
         * Labels should be in {@code "key=value"} format.
         */
        @Phase(DUUIStatus.CREATE)
        public DUUIFlow<Deployment> create(String image, int replicas, int containerPort, List<String> nodeLabels) {
            return create(image, replicas, containerPort, nodeLabels, Map.of());
        }

        /**
         * Creates a deployment with additional pod labels.
         */
        @Phase(DUUIStatus.CREATE)
        public DUUIFlow<Deployment> create(
                String image,
                int replicas,
                int containerPort,
                List<String> nodeLabels,
                Map<String, String> podLabels
        ) {
            try {
                Objects.requireNonNull(image, "image");

                List<NodeSelectorTerm> terms = nodeSelectorTerms(nodeLabels);

                Map<String, String> allPodLabels = new java.util.LinkedHashMap<>();
                allPodLabels.put("pipeline-uid", name);
                if (podLabels != null) {
                    allPodLabels.putAll(podLabels);
                }

                io.fabric8.kubernetes.api.model.apps.Deployment deployment = new DeploymentBuilder()
                        .withNewMetadata()
                        .withName(name)
                        .endMetadata()
                        .withNewSpec()
                        .withReplicas(replicas)
                        .withNewTemplate()
                        .withNewMetadata()
                        .withLabels(allPodLabels)
                        .endMetadata()
                        .withNewSpec()
                        .addNewContainer()
                        .withName(name)
                        .withImage(image)
                        .addNewPort()
                        .withContainerPort(containerPort)
                        .endPort()
                        .endContainer()
                        .withNewAffinity()
                        .withNewNodeAffinity()
                        .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .addAllToNodeSelectorTerms(terms)
                        .endRequiredDuringSchedulingIgnoredDuringExecution()
                        .endNodeAffinity()
                        .endAffinity()
                        .endSpec()
                        .endTemplate()
                        .withNewSelector()
                        .addToMatchLabels("pipeline-uid", name)
                        .endSelector()
                        .endSpec()
                        .build();

                k8s.apps().deployments().inNamespace(namespace).resource(deployment).create();
                return DUUIFlow.dispatch(this);
            } catch (RuntimeException e) {
                return DUUIFlow.fail(e);
            }
        }

        /**
         * Scales the deployment to the given number of replicas.
         */
        @Phase(DUUIStatus.SCALE)
        public DUUIFlow<Deployment> scale(int replicas) {
            try {
                k8s.apps().deployments().inNamespace(namespace).withName(name).scale(replicas);
                return DUUIFlow.dispatch(this);
            } catch (RuntimeException e) {
                return DUUIFlow.fail(e);
            }
        }

        /**
         * Returns the current number of replicas.
         */
        public int replicas() {
            io.fabric8.kubernetes.api.model.apps.Deployment d = inspect();
            return d == null || d.getSpec() == null || d.getSpec().getReplicas() == null
                    ? 0
                    : d.getSpec().getReplicas();
        }

        /**
         * Lists the pods belonging to this deployment using the {@code pipeline-uid} label selector.
         */
        public List<Pod> listPods() {
            PodList list = k8s.pods().inNamespace(namespace)
                    .withLabel("pipeline-uid", name)
                    .list();
            if (list == null || list.getItems() == null) {
                return List.of();
            }
            return list.getItems().stream()
                    .map(p -> new Pod(namespace, p.getMetadata().getName()))
                    .toList();
        }

        /**
         * Deletes the deployment from the cluster.
         */
        @Phase(DUUIStatus.DELETE)
        public DUUIFlow<Deployment> delete() {
            try {
                k8s.apps().deployments().inNamespace(namespace).withName(name).delete();
                return DUUIFlow.dispatch(this);
            } catch (RuntimeException e) {
                return DUUIFlow.fail(e);
            }
        }

        @Override
        public String toString() {
            return "Deployment[" + namespace + "/" + name + "]";
        }
    }

    /**
     * Kubernetes Service proxy with full lifecycle: create (ClusterIP / NodePort / LoadBalancer),
     * inspect, get endpoint URL, delete.
     */
    public final class Service implements DUUIProxy {
        private final String namespace;
        private final String name;

        private Service(String namespace, String name) {
            this.namespace = namespace;
            this.name = name;
        }

        public DUUIKubernetesClient client() {
            return DUUIKubernetesClient.this;
        }

        @Override
        public DUUIAddress address() {
            return DUUIKubernetesClient.address("service", namespace + "/" + name);
        }

        public String namespace() {
            return namespace;
        }

        public String name() {
            return name;
        }

        /**
         * Inspects (fetches) the current state of the service from the cluster.
         */
        public io.fabric8.kubernetes.api.model.Service inspect() {
            return k8s.services().inNamespace(namespace).withName(name).get();
        }

        /**
         * Returns {@code true} if the service exists.
         */
        public boolean exists() {
            return inspect() != null;
        }

        /**
         * Creates a ClusterIP service (default type).
         *
         * @param selector      label selector matching the target pods
         * @param portName      port identifier
         * @param port          service port
         * @param targetPort    container target port
         */
        @Phase(DUUIStatus.CREATE)
        public DUUIFlow<Service> createClusterIP(
                Map<String, String> selector,
                String portName,
                int port,
                int targetPort
        ) {
            return create(selector, portName, port, targetPort, "ClusterIP");
        }

        /**
         * Creates a NodePort service.
         *
         * @param selector      label selector matching the target pods
         * @param portName      port identifier
         * @param port          service port
         * @param targetPort    container target port
         */
        @Phase(DUUIStatus.CREATE)
        public DUUIFlow<Service> createNodePort(
                Map<String, String> selector,
                String portName,
                int port,
                int targetPort
        ) {
            return create(selector, portName, port, targetPort, "NodePort");
        }

        /**
         * Creates a LoadBalancer service.
         *
         * @param selector      label selector matching the target pods
         * @param portName      port identifier
         * @param port          service port
         * @param targetPort    container target port
         */
        @Phase(DUUIStatus.CREATE)
        public DUUIFlow<Service> createLoadBalancer(
                Map<String, String> selector,
                String portName,
                int port,
                int targetPort
        ) {
            return create(selector, portName, port, targetPort, "LoadBalancer");
        }

        private DUUIFlow<Service> create(
                Map<String, String> selector,
                String portName,
                int port,
                int targetPort,
                String type
        ) {
            try {
                io.fabric8.kubernetes.api.model.Service service = new ServiceBuilder()
                        .withNewMetadata()
                        .withName(name)
                        .endMetadata()
                        .withNewSpec()
                        .withSelector(selector == null ? Collections.singletonMap("pipeline-uid", name) : selector)
                        .addNewPort()
                        .withName(portName == null ? "k-port" : portName)
                        .withProtocol("TCP")
                        .withPort(port)
                        .withTargetPort(new IntOrString(targetPort))
                        .endPort()
                        .withType(type)
                        .endSpec()
                        .build();

                k8s.services().inNamespace(namespace).resource(service).create();
                return DUUIFlow.dispatch(this);
            } catch (RuntimeException e) {
                return DUUIFlow.fail(e);
            }
        }

        /**
         * Returns the service endpoint URL for the given port name.
         */
        public String getEndpointUrl(String portName) {
            return k8s.services().inNamespace(namespace).withName(name)
                    .getURL(Objects.requireNonNull(portName, "portName"));
        }

        /**
         * Returns the NodePort if this is a NodePort service, or {@code -1} if not available.
         */
        public int nodePort() {
            io.fabric8.kubernetes.api.model.Service s = inspect();
            if (s == null || s.getSpec() == null || s.getSpec().getPorts() == null
                    || s.getSpec().getPorts().isEmpty()) {
                return -1;
            }
            ServicePort firstPort = s.getSpec().getPorts().get(0);
            return firstPort.getNodePort() != null ? firstPort.getNodePort() : -1;
        }

        /**
         * Deletes the service from the cluster.
         */
        @Phase(DUUIStatus.DELETE)
        public DUUIFlow<Service> delete() {
            try {
                k8s.services().inNamespace(namespace).withName(name).delete();
                return DUUIFlow.dispatch(this);
            } catch (RuntimeException e) {
                return DUUIFlow.fail(e);
            }
        }

        @Override
        public String toString() {
            return "Service[" + namespace + "/" + name + "]";
        }
    }

    /**
     * Kubernetes Pod proxy: inspect, logs, status, delete.
     */
    public final class Pod implements DUUIProxy {
        private final String namespace;
        private final String name;

        private Pod(String namespace, String name) {
            this.namespace = namespace;
            this.name = name;
        }

        public DUUIKubernetesClient client() {
            return DUUIKubernetesClient.this;
        }

        @Override
        public DUUIAddress address() {
            return DUUIKubernetesClient.address("pod", namespace + "/" + name);
        }

        public String namespace() {
            return namespace;
        }

        public String name() {
            return name;
        }

        /**
         * Inspects (fetches) the current state of the pod.
         */
        public io.fabric8.kubernetes.api.model.Pod inspect() {
            return k8s.pods().inNamespace(namespace).withName(name).get();
        }

        /**
         * Returns {@code true} if the pod exists.
         */
        public boolean exists() {
            return inspect() != null;
        }

        /**
         * Returns the pod status phase (e.g., Running, Pending, Succeeded, Failed).
         */
        @Phase(DUUIStatus.PING)
        public DUUIFlow<String> status() {
            try {
                io.fabric8.kubernetes.api.model.Pod p = inspect();
                return DUUIFlow.dispatch(p == null || p.getStatus() == null || p.getStatus().getPhase() == null
                        ? "Unknown"
                        : p.getStatus().getPhase());
            } catch (RuntimeException e) {
                return DUUIFlow.fail(e);
            }
        }

        /**
         * Returns {@code true} if the pod is in the Running phase.
         */
        public boolean running() {
            return "Running".equalsIgnoreCase(status().join());
        }

        /**
         * Returns the pod's IP address, or {@code null} if not available.
         */
        public String podIP() {
            io.fabric8.kubernetes.api.model.Pod p = inspect();
            return p == null || p.getStatus() == null ? null : p.getStatus().getPodIP();
        }

        /**
         * Returns the logs for this pod.
         */
        public String logs() {
            return k8s.pods().inNamespace(namespace).withName(name).getLog();
        }

        /**
         * Tail the last {@code lines} lines of the pod logs.
         */
        public String logs(int lines) {
            return k8s.pods().inNamespace(namespace).withName(name).tailingLines(lines).getLog();
        }

        /**
         * Returns the creation timestamp.
         */
        public Instant createdAt() {
            io.fabric8.kubernetes.api.model.Pod p = inspect();
            if (p == null || p.getMetadata() == null || p.getMetadata().getCreationTimestamp() == null) {
                return null;
            }
            try {
                return Instant.from(K8S_TIMESTAMP.parse(p.getMetadata().getCreationTimestamp()));
            } catch (RuntimeException ignored) {
                return null;
            }
        }

        /**
         * Deletes the pod from the cluster.
         */
        @Phase(DUUIStatus.DELETE)
        public DUUIFlow<Pod> delete() {
            try {
                k8s.pods().inNamespace(namespace).withName(name).delete();
                return DUUIFlow.dispatch(this);
            } catch (RuntimeException e) {
                return DUUIFlow.fail(e);
            }
        }

        @Override
        public String toString() {
            return "Pod[" + namespace + "/" + name + "]";
        }
    }

    /**
     * Kubernetes Namespace proxy: create, delete, list.
     */
    public final class Namespace implements DUUIProxy {
        private final String name;

        private Namespace(String name) {
            this.name = name;
        }

        public DUUIKubernetesClient client() {
            return DUUIKubernetesClient.this;
        }

        @Override
        public DUUIAddress address() {
            return DUUIKubernetesClient.address("namespace", name);
        }

        public String name() {
            return name;
        }

        /**
         * Inspects (fetches) the namespace.
         */
        public io.fabric8.kubernetes.api.model.Namespace inspect() {
            return k8s.namespaces().withName(name).get();
        }

        /**
         * Returns {@code true} if the namespace exists.
         */
        public boolean exists() {
            return inspect() != null;
        }

        /**
         * Creates the namespace.
         */
        @Phase(DUUIStatus.CREATE)
        public DUUIFlow<Namespace> create() {
            try {
                k8s.namespaces().resource(
                        new NamespaceBuilder().withNewMetadata().withName(name).endMetadata().build()
                ).create();
                return DUUIFlow.dispatch(this);
            } catch (RuntimeException e) {
                return DUUIFlow.fail(e);
            }
        }

        /**
         * Deletes the namespace (and all resources within it).
         */
        @Phase(DUUIStatus.DELETE)
        public DUUIFlow<Namespace> delete() {
            try {
                k8s.namespaces().withName(name).delete();
                return DUUIFlow.dispatch(this);
            } catch (RuntimeException e) {
                return DUUIFlow.fail(e);
            }
        }

        /**
         * Lists all pods in this namespace.
         */
        public List<Pod> listPods() {
            PodList list = k8s.pods().inNamespace(name).list();
            if (list == null || list.getItems() == null) {
                return List.of();
            }
            return list.getItems().stream()
                    .map(p -> new Pod(name, p.getMetadata().getName()))
                    .toList();
        }

        @Override
        public String toString() {
            return "Namespace[" + name + "]";
        }
    }

    // ── Static helpers ─────────────────────────────────────────────────

    /**
     * Converts a list of {@code "key=value"} strings into {@link NodeSelectorTerm} objects.
     */
    public static List<NodeSelectorTerm> nodeSelectorTerms(List<String> rawLabels) {
        if (rawLabels == null || rawLabels.isEmpty()) {
            return List.of(defaultNodeSelectorTerm());
        }
        List<NodeSelectorTerm> terms = new ArrayList<>();
        for (String raw : rawLabels) {
            String[] parts = raw.split("=", 2);
            if (parts.length == 2) {
                NodeSelectorTerm term = new NodeSelectorTerm();
                NodeSelectorRequirement req = new NodeSelectorRequirement(parts[0], "In", List.of(parts[1]));
                term.setMatchExpressions(List.of(req));
                terms.add(term);
            }
        }
        return terms.isEmpty() ? List.of(defaultNodeSelectorTerm()) : terms;
    }

    private static NodeSelectorTerm defaultNodeSelectorTerm() {
        NodeSelectorTerm term = new NodeSelectorTerm();
        NodeSelectorRequirement req = new NodeSelectorRequirement("disktype", "In", List.of("all"));
        term.setMatchExpressions(List.of(req));
        return term;
    }

    private static DUUIAddress address(String authority, String value) {
        String path = value == null || value.isBlank() ? "" : "/" + value;
        return new DUUIAddress("kubernetes", authority, path, null, null);
    }

    private static String pathValue(DUUIAddress address) {
        String path = Objects.requireNonNull(address.path(), "address.path");
        return path.startsWith("/") ? path.substring(1) : path;
    }

    /**
     * Splits the path into {@code [namespace, name]}. If the path only contains one segment,
     * the default namespace is used as the first element.
     */
    private static String[] splitPath(DUUIAddress address) {
        String value = pathValue(address);
        int slash = value.indexOf('/');
        if (slash < 0) {
            return new String[]{DEFAULT_NAMESPACE, value};
        }
        return new String[]{value.substring(0, slash), value.substring(slash + 1)};
    }
}
