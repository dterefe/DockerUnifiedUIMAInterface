import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUILocalDrivesDocumentHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIDocumentReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;

public class GatewayEvaluation {

    private static final String ENV_ROOT_PATH = "GATEWAY_EVAL_ROOT_PATH";
    private static final String ENV_INPUT_PATH = "GATEWAY_EVAL_INPUT_PATH";
    private static final String ENV_INPUT_EXT = "GATEWAY_EVAL_INPUT_EXT";
    private static final String ENV_OUTPUT_PATH = "GATEWAY_EVAL_OUTPUT_PATH";
    private static final String ENV_OUTPUT_EXT = "GATEWAY_EVAL_OUTPUT_EXT";
    private static final String ENV_DOCKER_IMAGE = "GATEWAY_EVAL_DOCKER_IMAGE";
    private static final String ENV_REMOTE_URLS = "GATEWAY_EVAL_REMOTE_URLS";
    private static final String ENV_PROM_PATH = "GATEWAY_EVAL_PROM_PATH";
    private static final String ENV_RUN_KEY = "GATEWAY_EVAL_RUN_KEY";
    private static final String ENV_WORKERS = "GATEWAY_EVAL_WORKERS";
    private static final String ENV_MODES = "GATEWAY_EVAL_MODES";

    private enum ComponentMode {
        LOCAL,
        REMOTE
    }

    private static Stream<Arguments> composerConfigurations() {
        String workersEnv = System.getenv(ENV_WORKERS);
        String modesEnv = System.getenv(ENV_MODES);

        List<Integer> workers;
        if (workersEnv == null || workersEnv.isBlank()) {
            workers = List.of(1, 4, 8);
        } else {
            workers = Arrays.stream(workersEnv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(Integer::parseInt)
                .toList();
        }

        List<ComponentMode> modes;
        if (modesEnv == null || modesEnv.isBlank()) {
            modes = List.of(ComponentMode.LOCAL, ComponentMode.REMOTE);
        } else {
            modes = Arrays.stream(modesEnv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(s -> ComponentMode.valueOf(s.toUpperCase()))
                .toList();
        }

        return workers.stream()
            .flatMap(w -> modes.stream().map(m -> arguments(w, m)));
    }

    @ParameterizedTest(name = "gateway-eval workers={0}, mode={1}")
    @MethodSource("composerConfigurations")
    @DisplayName("Evaluate DUUIComposer run(DUUIDocumentReader, ...) for different worker counts and component modes")
    public void evaluateGatewayRunWithDocumentReader(int workers, ComponentMode mode) throws Exception {
        String rootPath = System.getenv(ENV_ROOT_PATH);
        String inputPath = System.getenv(ENV_INPUT_PATH);
        String inputExt = System.getenv(ENV_INPUT_EXT);
        String outputPath = System.getenv(ENV_OUTPUT_PATH);
        String outputExt = System.getenv(ENV_OUTPUT_EXT);
        String promPath = System.getenv(ENV_PROM_PATH);
        String runKey = System.getenv(ENV_RUN_KEY);

        Assumptions.assumeTrue(rootPath != null && !rootPath.isBlank(),
            () -> ENV_ROOT_PATH + " must be set for GatewayEvaluation");

        Assumptions.assumeTrue(inputPath != null && !inputPath.isBlank(),
            () -> ENV_INPUT_PATH + " must be set for GatewayEvaluation");
        Assumptions.assumeTrue(inputExt != null && !inputExt.isBlank(),
            () -> ENV_INPUT_EXT + " must be set for GatewayEvaluation");
        Assumptions.assumeTrue(outputPath != null && !outputPath.isBlank(),
            () -> ENV_OUTPUT_PATH + " must be set for GatewayEvaluation");
        Assumptions.assumeTrue(outputExt != null && !outputExt.isBlank(),
            () -> ENV_OUTPUT_EXT + " must be set for GatewayEvaluation");
        Assumptions.assumeTrue(promPath != null && !promPath.isBlank(),
            () -> ENV_PROM_PATH + " must be set for GatewayEvaluation");
        Assumptions.assumeTrue(runKey != null && !runKey.isBlank(),
            () -> ENV_RUN_KEY + " must be set for GatewayEvaluation");

        DUUILuaContext luaContext = new DUUILuaContext().withJsonLibrary();

        DUUIComposer composer = new DUUIComposer()
            .withLuaContext(luaContext)
            .withSkipVerification(true)
            .withWorkers(workers)
            .withDebugLevel(DUUIComposer.DebugLevel.INFO)
            .withPrometheusProfiler(Path.of(promPath));

        configureComponent(composer, workers, mode);

        DUUILocalDrivesDocumentHandler handler = new DUUILocalDrivesDocumentHandler(rootPath);

        String resolvedInputPath = Path.of(rootPath, inputPath).toString();
        String resolvedOutputPath = Path.of(rootPath, outputPath).toString();

        DUUIDocumentReader documentReader = DUUIDocumentReader
            .builder(composer)
            .withInputHandler(handler)
            .withInputPath(resolvedInputPath)
            .withInputFileExtension(inputExt)
            .withOutputHandler(handler)
            .withOutputPath(resolvedOutputPath)
            .withOutputFileExtension(outputExt)
            .withSortBySize(true)
            .withRecursive(true)
            .withAddMetadata(true)
            .withCheckTarget(true)
            .build();

        try {
            composer.run(documentReader, runKey);
        } finally {
            composer.shutdown();
        }
    }

    private static void configureComponent(DUUIComposer composer, int workers, ComponentMode mode) throws URISyntaxException, IOException {
        switch (mode) {
            case LOCAL -> {
                String image = System.getenv(ENV_DOCKER_IMAGE);
                Assumptions.assumeTrue(image != null && !image.isBlank(),
                    () -> ENV_DOCKER_IMAGE + " must be set for LOCAL component mode");

                DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
                composer.addDriver(dockerDriver);

                composer.add(
                    new DUUIDockerDriver.Component(image)
                        .withScale(workers)
                        .withName("spacy-local")
                        .build()
                );
            }
            case REMOTE -> {
                String urlsEnv = System.getenv(ENV_REMOTE_URLS);
                Assumptions.assumeTrue(urlsEnv != null && !urlsEnv.isBlank(),
                    () -> ENV_REMOTE_URLS + " must be set for REMOTE component mode");

                List<String> urls = Arrays.stream(urlsEnv.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toList();

                Assumptions.assumeTrue(!urls.isEmpty(),
                    () -> ENV_REMOTE_URLS + " must contain at least one URL");

                DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
                composer.addDriver(remoteDriver);

                composer.add(
                    new DUUIRemoteDriver.Component(urls)
                        .withName("spacy-remote")
                        .build()
                );
            }
        }
    }
}
