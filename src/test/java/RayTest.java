import org.apache.uima.UIMAException;
import org.junit.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.*;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUIAsynchronousProcessor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUICollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIMultimodalCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class RayTest {

    private static final int iWorkers = 1;
    private static DUUIComposer pComposer = null;
    private static DUUIRayDriver rayDriver = null;

    @BeforeAll
    public static void init() throws IOException, URISyntaxException, UIMAException, SAXException {

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();

        pComposer = new DUUIComposer()
                .withSkipVerification(true)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);

        DUUIUIMADriver uima_driver = new DUUIUIMADriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        rayDriver = new DUUIRayDriver();

        pComposer.addDriver(uima_driver, remoteDriver, dockerDriver, rayDriver);


    }

    @Test
    @DisplayName("RayTest-LetterCounter")
    /**
     * Creates a new ray cluster
     * Counts the frequency of letters across multiple text documents
     */
    public void runRayLetterCounter() throws Exception {

        //init();
        DUUIMultimodalCollectionReader DMCR = new DUUIMultimodalCollectionReader("test_corpora", "txt");
        Set<DUUICollectionReader> readers = new HashSet<>();
        readers.add(DMCR);
        DUUIAsynchronousProcessor processor = new DUUIAsynchronousProcessor(readers);


        pComposer.add(new DUUIRayDriver.Component("test_containers/ray/ray_letter_counter/", "letter_counter_stream.py")
                .withTargetView("frequencyView")
                .withNumWorkers(iWorkers)
                .withRayExecutable("/home/bundan/.local/bin/ray")  // If not set: using global ray installation
                .withKeepJobAlive(false)  // Kill job
                .withDeleteJob(true)  // Delete Job
                .withTaskUrl("http://127.0.0.1:25591")  // Python code port
                .withStreamMode(true)  // N files to 1 output
                .withCreateOwnCas(true, "result")  // Create new cas just for output (otherwise written to last cas)
                .withKeepAlive(false)  // Shut down ray cluster after processing
                .build());

        pComposer.run(processor, "ray-test");

        // Print letter frequency
        System.out.println(rayDriver.getResultCases().getFirst().getView("frequencyView").getSofaDataString());
    }

    @Test
    @DisplayName("RayTest-LetterCounter-ExistingCluster")
    /**
     * Connects to an existing ray cluster. Ray versions of cluster and own ray executable should match!
     * Counts the frequency of letters across multiple text documents
     */
    public void runRayLetterCounterWithExistingCluster() throws Exception {

        //init();
        DUUIMultimodalCollectionReader DMCR = new DUUIMultimodalCollectionReader("test_corpora", "txt");
        Set<DUUICollectionReader> readers = new HashSet<>();
        readers.add(DMCR);
        DUUIAsynchronousProcessor processor = new DUUIAsynchronousProcessor(readers);

        pComposer.add(new DUUIRayDriver.Component("test_containers/ray/ray_letter_counter/", "letter_counter_stream.py")
                .withTargetView("frequencyView")
                .withNumWorkers(iWorkers)
                .withRayExecutable("/home/bundan/.local/bin/ray")  // If not set: using global ray installation
                .withClusterUrl("http://localhost:8265")  // Connect with this already running cluster.
                .withKeepJobAlive(false)  // Kill job
                .withDeleteJob(true)  // Delete Job
                .withTaskUrl("http://127.0.0.1:25591")  // Python code port
                .withStreamMode(true)  // N files to 1 output
                .withCreateOwnCas(true, "result")  // Create new cas just for output (otherwise written to last cas)
                .build());

        pComposer.run(processor, "ray-test");

        // Print letter frequency
        System.out.println(rayDriver.getResultCases().getFirst().getView("frequencyView").getSofaDataString());
    }
}

