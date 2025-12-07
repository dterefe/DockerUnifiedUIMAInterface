package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.javatuples.Triplet;
import org.jetbrains.annotations.NotNull;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.PipelineComponentException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.duui.ReproducibleAnnotation;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;

/**
 * The interface for the instance of each component that is executed in a pipeline.
 * @author Alexander Leonhardt
 */
public interface IDUUIInstantiatedPipelineComponent {
    public static HttpClient _client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.ALWAYS)
            .proxy(ProxySelector.getDefault())
            .connectTimeout(Duration.ofSeconds(1000)).build();

    /**
     * @return the non-null {@link DUUIPipelineComponent} configuration backing this instantiated component.
     */
    @NotNull
    public DUUIPipelineComponent getPipelineComponent();
    public Triplet<IDUUIUrlAccessible,Long,Long> getComponent();
    public void addComponent(IDUUIUrlAccessible item);
    public String getUniqueComponentKey();

    default String getName() {
        return getPipelineComponent().getName();
    }

    default String getDescription() {
        return getPipelineComponent().getDescription();
    }

    default int getScale() {
        return getPipelineComponent().getScale(1);
    }

    default int getWorkers() {
        return getPipelineComponent().getWorkers(1);
    }

    default Map<String, String> getParameters() {
        return getPipelineComponent().getParameters();
    }

    default String getSourceView() {
        return getPipelineComponent().getSourceView();
    }

    default String getTargetView() {
        return getPipelineComponent().getTargetView();
    }

    default long getTimeout() {
        return getPipelineComponent().getTimeout();
    }

    default int getFinalizedRepresentationHash() {
        return getPipelineComponent().getFinalizedRepresentationHash();
    }

    default String getFinalizedRepresentation() {
        return getPipelineComponent().getFinalizedRepresentation();
    }
    
    public static int REQUEST_TRIES = 50;

    /**
     * Returns the TypeSystem used for the DUUI component used.
     * @param uuid
     * @param comp
     * @return
     * @throws ResourceInitializationException
     */
    public static TypeSystemDescription getTypesystem(String uuid, IDUUIInstantiatedPipelineComponent comp) throws ResourceInitializationException {
        Triplet<IDUUIUrlAccessible,Long,Long> queue = comp.getComponent();
        DUUIPipelineComponent pipelineComponent = comp.getPipelineComponent();
        String driverName = pipelineComponent.getDriverSimpleName() != null
                ? pipelineComponent.getDriverSimpleName()
                : "unknown-driver";
        String componentKey = comp.getUniqueComponentKey() != null
                ? comp.getUniqueComponentKey()
                : "unknown-component";
        String logPrefix = String.format("[%s][%s]", driverName, componentKey);

        System.out.printf(
                "%s Requesting typesystem from %s%s%n",
                logPrefix,
                queue.getValue0().generateURL(),
                DUUIComposer.V1_COMPONENT_ENDPOINT_TYPESYSTEM
        );
        
        try {
    
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(queue.getValue0().generateURL() + DUUIComposer.V1_COMPONENT_ENDPOINT_TYPESYSTEM))
                    .timeout(Duration.ofSeconds(comp.getTimeout()))
                    .version(HttpClient.Version.HTTP_1_1)
                    .GET()
                    .build();
    
            Duration componentTimeout = Duration.ofSeconds(comp.getTimeout());
            Instant deadline = Instant.now().plus(componentTimeout);
            
            HttpResponse<byte[]> resp = DUUIRestDriver.sendWithRetries(
                    _client,
                    request,
                    deadline,
                    componentTimeout,
                    logPrefix
            );
            

            if (resp == null) {
                throw new IOException("No response from typesystem endpoint.");
            }

            if (resp.statusCode() == 200) {
                String body = new String(resp.body(), Charset.defaultCharset());
                File tmp = File.createTempFile("duui.composer", "_type");
                tmp.deleteOnExit();
                try (FileWriter writer = new FileWriter(tmp)) {
                    writer.write(body);
                    writer.flush();

                    return TypeSystemDescriptionFactory.createTypeSystemDescriptionFromPath(tmp.toURI().toString());
                }
            } else {
                System.err.printf(
                        "%s Typesystem endpoint %s%s returned HTTP %d, using default typesystem instead.%n",
                        logPrefix,
                        queue.getValue0().generateURL(),
                        DUUIComposer.V1_COMPONENT_ENDPOINT_TYPESYSTEM,
                        resp.statusCode()
                );
                return TypeSystemDescriptionFactory.createTypeSystemDescription();
            }
        } catch (Exception e) {
            System.err.printf(
                "%s Typesystem endpoint %s%s unreachable after %d tries: %s %n %s %n",
                logPrefix,
                queue.getValue0().generateURL(),
                DUUIComposer.V1_COMPONENT_ENDPOINT_TYPESYSTEM,
                REQUEST_TRIES,
                e.getClass().getSimpleName(),
                e.getMessage()
            
            );

            throw new ResourceInitializationException(new Exception(String.format(
                "%s Typesystem endpoint %s%s unreachable after %d tries: %s %n %s %n",
                logPrefix,
                queue.getValue0().generateURL(),
                DUUIComposer.V1_COMPONENT_ENDPOINT_TYPESYSTEM,
                REQUEST_TRIES,
                e.getClass().getSimpleName(),
                e.getMessage()
            )));
        } finally {
            comp.addComponent(queue.getValue0());
        }
    }


    /**
     * Calling the DUUI component
     * @param jc
     * @param comp
     * @param perf
     * @throws CASException
     * @throws PipelineComponentException
     */
    public static void process(JCas jc, IDUUIInstantiatedPipelineComponent comp, DUUIPipelineDocumentPerformance perf) throws CASException, PipelineComponentException {
        Triplet<IDUUIUrlAccessible,Long,Long> queue = comp.getComponent();

        IDUUICommunicationLayer layer = queue.getValue0().getCommunicationLayer();
        long serializeStart = System.nanoTime();
        
        DUUIPipelineComponent pipelineComponent = comp.getPipelineComponent();
        String driverName = pipelineComponent.getDriverSimpleName() != null
                ? pipelineComponent.getDriverSimpleName()
                : "unknown-driver";
        String componentKey = comp.getUniqueComponentKey() != null
                ? comp.getUniqueComponentKey()
                : "unknown-component";

        String logPrefix = String.format("[%s][%s]", driverName, componentKey);

        try {

            String viewName = pipelineComponent.getViewName();
            JCas viewJc;
            if(viewName == null) {
                viewJc = jc;
            }
            else {
                try {
                    viewJc = jc.getView(viewName);
                }
                catch(CASException e) {
                    if(pipelineComponent.getCreateViewFromInitialView()) {
                        viewJc = jc.createView(viewName);
                        viewJc.setDocumentText(jc.getDocumentText());
                        viewJc.setDocumentLanguage(jc.getDocumentLanguage());
                    }
                    else {
                        throw e;
                    }
                }
            }

            if (layer.supportsProcess()) {
                JCas sourceCas = viewJc.getView(comp.getSourceView());
                JCas targetCas;
                try {
                    targetCas = viewJc.getView(comp.getTargetView());
                } catch (CASException e) {
                    targetCas = viewJc.createView(comp.getTargetView());
                }

                layer.process(
                        sourceCas,
                        new DUUIHttpRequestHandler(_client, queue.getValue0().generateURL(), pipelineComponent.getTimeout()),
                        comp.getParameters(),
                        targetCas
                );

                ReproducibleAnnotation ann = new ReproducibleAnnotation(jc);
                ann.setDescription(comp.getFinalizedRepresentation());
                ann.setCompression(DUUIPipelineComponent.compressionMethod);
                ann.setTimestamp(System.nanoTime());
                ann.setPipelineName(perf.getRunKey());
                ann.addToIndexes();

                return;
            }

            ByteArrayOutputStream out = new ByteArrayOutputStream(1024*1024);

            // Invoke Lua serialize()
            layer.serialize(viewJc,out,comp.getParameters(), comp.getSourceView());

            byte[] ok = out.toByteArray();
            long sizeArray = ok.length;
            long serializeEnd = System.nanoTime();

            long annotatorStart = serializeEnd;

            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(queue.getValue0().generateURL() + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS))
                .timeout(Duration.ofSeconds(comp.getTimeout()))
                .POST(HttpRequest.BodyPublishers.ofByteArray(ok))
                .version(HttpClient.Version.HTTP_1_1)
                .build();

            Duration componentTimeout = Duration.ofSeconds(comp.getTimeout());
            Instant deadline = Instant.now().plus(componentTimeout);
            HttpResponse<byte[]> resp = null;

            try {
                resp = DUUIRestDriver.sendWithRetries(
                    _client, 
                    request, 
                    deadline, 
                    componentTimeout, 
                    componentTimeout, 
                    REQUEST_TRIES, 
                    logPrefix
                );
            } catch (IOException | TimeoutException e) {
                System.err.printf(
                        "%s Could not reach endpoint %s%s after %d tries, aborting. %n",
                        logPrefix,
                        queue.getValue0().generateURL(),
                        DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS,
                        REQUEST_TRIES
                );

                throw e;
            } catch (Exception e) {
                System.err.printf(
                        "%s Fatal error during process request, aborting: %s %s %n",
                        logPrefix,
                        e.getCause().getClass().getSimpleName(),
                        e.getCause().getMessage()
                );
                throw e;
            }

            if(resp==null) {
                System.err.printf(
                        "%s Could not reach endpoint %s%s after %d tries, aborting. %n",
                        logPrefix,
                        queue.getValue0().generateURL(),
                        DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS,
                        REQUEST_TRIES
                );
                throw new IOException("Could not reach endpoint after " + REQUEST_TRIES + " tries!");
            }


            if (resp.statusCode() == 200) {
                ByteArrayInputStream st = new ByteArrayInputStream(resp.body());
                long annotatorEnd = System.nanoTime();
                long deserializeStart = annotatorEnd;

                    layer.deserialize(viewJc, st, comp.getTargetView());

                long deserializeEnd = System.nanoTime();

                ReproducibleAnnotation ann = new ReproducibleAnnotation(jc);
                ann.setDescription(comp.getFinalizedRepresentation());
                ann.setCompression(DUUIPipelineComponent.compressionMethod);
                ann.setTimestamp(System.nanoTime());
                ann.setPipelineName(perf.getRunKey());
                ann.addToIndexes();
                perf.addData(serializeEnd-serializeStart,deserializeEnd-deserializeStart,annotatorEnd-annotatorStart,queue.getValue2()-queue.getValue1(),deserializeEnd-queue.getValue1(), String.valueOf(comp.getFinalizedRepresentationHash()), sizeArray, jc, null);

            } else {
                ByteArrayInputStream st = new ByteArrayInputStream(resp.body());
                String responseBody = new String(st.readAllBytes(), StandardCharsets.UTF_8);
                st.close();

                // track "performance" of error documents if not explicitly disabled
                if (perf.shouldTrackErrorDocs()) {
                    long annotatorEnd = System.nanoTime();
                    long deserializeStart = annotatorEnd;
                    long deserializeEnd = System.nanoTime();

                    String error = "Expected response 200, got " + resp.statusCode() + ": " + responseBody;


                    perf.addData(serializeEnd - serializeStart, deserializeEnd - deserializeStart, annotatorEnd - annotatorStart, queue.getValue2() - queue.getValue1(), deserializeEnd - queue.getValue1(), String.valueOf(comp.getFinalizedRepresentationHash()), sizeArray, jc, error);
                }

                if (!pipelineComponent.getIgnoringHTTP200Error()) {
                    throw new InvalidObjectException(String.format("Expected response 200, got %d: %s", resp.statusCode(), responseBody));
                } else {
                    System.err.println(String.format("%s Expected response 200, got %d: %s", logPrefix, resp.statusCode(), responseBody));
                }
            }
        } catch (CASException e) {
            throw e;
        } catch (IOException | TimeoutException e ) {
            throw new PipelineComponentException(pipelineComponent, e);
        } catch (Exception e) {
            try {
                System.err.printf(
                        "%s Unhandled exception while processing component: %s%n%s%n",
                        logPrefix,
                        e.toString(),
                        ExceptionUtils.getStackTrace(e)
                );
                DocumentMetaData documentMetaData = DocumentMetaData.get(jc);
                throw new PipelineComponentException(comp.getPipelineComponent(), documentMetaData, e);
            } catch (IllegalArgumentException ignored) {
                System.err.printf(
                        "%s Could not retrieve DocumentMetaData from CAS while wrapping exception: %s%n",
                        logPrefix,
                        ignored.toString()
                );
                throw new PipelineComponentException(comp.getPipelineComponent(), e);
            }
        } finally {
            comp.addComponent(queue.getValue0());
        }
    }

    /**
     * The process merchant describes the use of the component as a web socket
     * @param jc
     * @param comp
     * @param perf
     * @throws CASException
     * @throws PipelineComponentException
     */
    @Deprecated
    public static void process_handler(JCas jc,
                                       IDUUIInstantiatedPipelineComponent comp,
                                       DUUIPipelineDocumentPerformance perf) throws CASException, PipelineComponentException {

        throw new UnsupportedOperationException("WebSocket processing is no longer supported in DUUI.");
    }
}
