package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;


import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.javatuples.Triplet;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.DUUIWebsocketAlt;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.PipelineComponentException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.duui.ReproducibleAnnotation;

import java.io.*;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

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

    public DUUIPipelineComponent getPipelineComponent();
    public Triplet<IDUUIUrlAccessible,Long,Long> getComponent();
    public void addComponent(IDUUIUrlAccessible item);

    public Map<String,String> getParameters();
    public String getSourceView();
    public String getTargetView();
    public String getUniqueComponentKey();

    /**
     * Returns the TypeSystem used for the DUUI component used.
     * @param uuid
     * @param comp
     * @return
     * @throws ResourceInitializationException
     */
    public static TypeSystemDescription getTypesystem(String uuid, IDUUIInstantiatedPipelineComponent comp) throws ResourceInitializationException {
        Triplet<IDUUIUrlAccessible, Long, Long> queue = comp.getComponent();
        int tries = 0;

        String url = queue.getValue0().generateURL() + DUUIComposer.V1_COMPONENT_ENDPOINT_TYPESYSTEM;
        System.out.printf("[DEBUG][%s] getTypesystem: URL=%s%n", uuid, url);

        while (tries < 100) {
            tries++;
            try {
                System.out.printf("[DEBUG][%s] Attempt #%d: Creating HTTP GET request to %s%n", uuid, tries, url);

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .version(HttpClient.Version.HTTP_1_1)
                        .GET()
                        .build();

                System.out.printf("[DEBUG][%s] Sending request...%n", uuid);
                HttpResponse<byte[]> resp = _client.send(request, HttpResponse.BodyHandlers.ofByteArray());
                System.out.printf("[DEBUG][%s] Got response: Status=%d, BodySize=%d%n", uuid, resp.statusCode(), resp.body() != null ? resp.body().length : -1);

                if (resp.statusCode() == 200) {
                    String body = new String(resp.body(), Charset.defaultCharset());
                    File tmp = File.createTempFile("duui.composer", "_type");
                    tmp.deleteOnExit();
                    System.out.printf("[DEBUG][%s] Writing typesystem to temp file: %s, Size=%d%n", uuid, tmp.getAbsolutePath(), body.length());
                    FileWriter writer = new FileWriter(tmp);
                    writer.write(body);
                    writer.flush();
                    writer.close();

                    comp.addComponent(queue.getValue0());
                    System.out.printf("[DEBUG][%s] TypeSystemDescriptionFactory.createTypeSystemDescriptionFromPath(%s)%n", uuid, tmp.toURI().toString());
                    return TypeSystemDescriptionFactory.createTypeSystemDescriptionFromPath(tmp.toURI().toString());
                } else {
                    comp.addComponent(queue.getValue0());
                    System.out.printf("[%s][DEBUG] Endpoint did not provide typesystem (Status %d), using default one...\n", uuid, resp.statusCode());
                    return TypeSystemDescriptionFactory.createTypeSystemDescription();
                }
            } catch (Exception e) {
                System.out.printf("[DEBUG][%s] Exception on try %d/%d: %s (%s)%n", uuid, tries, 100, e.getClass().getSimpleName(), e.getMessage());
                e.printStackTrace(System.out);
                System.out.printf("Cannot reach endpoint, trying again %d/%d...\n", tries + 1, 100);
                try { Thread.sleep(250); } catch (InterruptedException ignored) {}
            } finally {
                System.out.printf("[DEBUG][%s] Adding component back to pool.%n", uuid);
                comp.addComponent(queue.getValue0());
            }
        }
        System.out.printf("[DEBUG][%s] All tries failed, endpoint unreachable!%n", uuid);
        throw new ResourceInitializationException(new Exception("Endpoint is unreachable!"));
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
        
        System.out.printf("[DEBUG] process(): Starting processing for component %s%n", comp.getPipelineComponent().getName());
        System.out.printf("[DEBUG] process(): Retrieving component instance... %n");
        Triplet<IDUUIUrlAccessible, Long, Long> queue = comp.getComponent();
        String uuid = java.util.UUID.randomUUID().toString(); // Für Tracing, falls kein besseres vorhanden

        System.out.printf("[DEBUG][%s] Retrieved component instance. %n", uuid);

        IDUUICommunicationLayer layer = queue.getValue0().getCommunicationLayer();
        long serializeStart = System.nanoTime();

        try {
            DUUIPipelineComponent pipelineComponent = comp.getPipelineComponent();
            String viewName = pipelineComponent.getViewName();
            JCas viewJc;
            if(viewName == null) {
                viewJc = jc;
                System.out.printf("[DEBUG][%s] No specific view requested, using main JCas%n", uuid);
            } else {
                try {
                    viewJc = jc.getView(viewName);
                    System.out.printf("[DEBUG][%s] Using view: %s%n", uuid, viewName);
                }
                catch(CASException e) {
                    if(pipelineComponent.getCreateViewFromInitialView()) {
                        viewJc = jc.createView(viewName);
                        viewJc.setDocumentText(jc.getDocumentText());
                        viewJc.setDocumentLanguage(jc.getDocumentLanguage());
                        System.out.printf("[DEBUG][%s] View %s not found, created new with copied text/lang.%n", uuid, viewName);
                    } else {
                        System.out.printf("[DEBUG][%s] View %s not found, and not allowed to create.%n", uuid, viewName);
                        throw e;
                    }
                }
            }

            if (layer.supportsProcess()) {
                System.out.printf("[DEBUG][%s] Using optimized communication layer for process()%n", uuid);
                JCas sourceCas = viewJc.getView(comp.getSourceView());
                JCas targetCas;
                try {
                    targetCas = viewJc.getView(comp.getTargetView());
                } catch (CASException e) {
                    targetCas = viewJc.createView(comp.getTargetView());
                    System.out.printf("[DEBUG][%s] Target view %s not found, created new.%n", uuid, comp.getTargetView());
                }

                layer.process(
                    sourceCas,
                    new DUUIHttpRequestHandler(_client, queue.getValue0().generateURL(), pipelineComponent.getTimeout()),
                    comp.getParameters(),
                    targetCas
                );

                System.out.printf("[DEBUG][%s] Layer.process() done, writing annotation...%n", uuid);

                ReproducibleAnnotation ann = new ReproducibleAnnotation(jc);
                ann.setDescription(comp.getPipelineComponent().getFinalizedRepresentation());
                ann.setCompression(DUUIPipelineComponent.compressionMethod);
                ann.setTimestamp(System.nanoTime());
                ann.setPipelineName(perf.getRunKey());
                ann.addToIndexes();

                return;
            }

            ByteArrayOutputStream out = new ByteArrayOutputStream(1024*1024);

            // Invoke Lua serialize()
            System.out.printf("[DEBUG][%s] Calling layer.serialize()...%n", uuid);
            layer.serialize(viewJc, out, comp.getParameters(), comp.getSourceView());

            byte[] ok = out.toByteArray();
            long sizeArray = ok.length;
            long serializeEnd = System.nanoTime();
            System.out.printf("[DEBUG][%s] Serialize finished. Size: %d bytes, Time: %d ms%n", uuid, sizeArray, (serializeEnd-serializeStart)/1_000_000);

            long annotatorStart = serializeEnd;
            int tries = 0;
            HttpResponse<byte[]> resp = null;
            String processUrl = queue.getValue0().generateURL() + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS;
            System.out.printf("[DEBUG][%s] Target endpoint: %s%n", uuid, processUrl);
            while (tries < 3) {
                tries++;
                try {
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create(processUrl))
                            .timeout(Duration.ofSeconds(pipelineComponent.getTimeout()))
                            .POST(HttpRequest.BodyPublishers.ofByteArray(ok))
                            .version(HttpClient.Version.HTTP_1_1)
                            .build();
                    System.out.printf("[DEBUG][%s] Attempt #%d: Sending POST (size: %d bytes)%n", uuid, tries, ok.length);
                    resp = _client.send(request, HttpResponse.BodyHandlers.ofByteArray());
                    System.out.printf("[DEBUG][%s] Response: Status=%d, Size=%d bytes%n", uuid, resp.statusCode(), resp.body()!=null?resp.body().length:-1);
                    break;
                } catch (Exception e) {
                    System.out.printf("[DEBUG][%s] Exception on try %d/%d: %s (%s)%n", uuid, tries, 3, e.getClass().getSimpleName(), e.getMessage());
                    e.printStackTrace(System.out);
                    try { Thread.sleep(500); } catch (InterruptedException ignored) {}
                }
            }
            if (resp == null) {
                System.out.printf("[DEBUG][%s] Could not reach endpoint after 3 tries!%n", uuid);
                throw new IOException("Could not reach endpoint after 3 tries!");
            }

            if (resp.statusCode() == 200) {
                ByteArrayInputStream st = new ByteArrayInputStream(resp.body());
                long annotatorEnd = System.nanoTime();
                long deserializeStart = annotatorEnd;

                System.out.printf("[DEBUG][%s] Deserializing response into layer...%n", uuid);
                layer.deserialize(viewJc, st, comp.getTargetView());
                long deserializeEnd = System.nanoTime();

                ReproducibleAnnotation ann = new ReproducibleAnnotation(jc);
                ann.setDescription(comp.getPipelineComponent().getFinalizedRepresentation());
                ann.setCompression(DUUIPipelineComponent.compressionMethod);
                ann.setTimestamp(System.nanoTime());
                ann.setPipelineName(perf.getRunKey());
                ann.addToIndexes();
                perf.addData(serializeEnd - serializeStart, deserializeEnd - deserializeStart, annotatorEnd - annotatorStart, queue.getValue2() - queue.getValue1(), deserializeEnd - queue.getValue1(), String.valueOf(comp.getPipelineComponent().getFinalizedRepresentationHash()), sizeArray, jc, null);

                System.out.printf("[DEBUG][%s] Deserialization done, annotation added. Timing (ms): serialize=%d, deserialize=%d, annotator=%d%n", uuid, (serializeEnd-serializeStart)/1_000_000, (deserializeEnd-deserializeStart)/1_000_000, (annotatorEnd-annotatorStart)/1_000_000);

            } else {
                ByteArrayInputStream st = new ByteArrayInputStream(resp.body());
                String responseBody = new String(st.readAllBytes(), StandardCharsets.UTF_8);
                st.close();

                System.out.printf("[DEBUG][%s] Non-200 response: %d %s%n", uuid, resp.statusCode(), responseBody);

                // track "performance" of error documents if not explicitly disabled
                if (perf.shouldTrackErrorDocs()) {
                    long annotatorEnd = System.nanoTime();
                    long deserializeStart = annotatorEnd;
                    long deserializeEnd = System.nanoTime();

                    String error = "Expected response 200, got " + resp.statusCode() + ": " + responseBody;

                    perf.addData(serializeEnd - serializeStart, deserializeEnd - deserializeStart, annotatorEnd - annotatorStart, queue.getValue2() - queue.getValue1(), deserializeEnd - queue.getValue1(), String.valueOf(comp.getPipelineComponent().getFinalizedRepresentationHash()), sizeArray, jc, error);
                }

                if (!pipelineComponent.getIgnoringHTTP200Error()) {
                    throw new InvalidObjectException(String.format("Expected response 200, got %d: %s", resp.statusCode(), responseBody));
                } else {
                    System.err.println(String.format("Expected response 200, got %d: %s", resp.statusCode(), responseBody));
                }
            }
        } catch (CASException e) {
            System.out.printf("[DEBUG][%s] CASException: %s%n", uuid, e.getMessage());
            throw e;
        } catch (Exception e) {
            System.out.printf("[DEBUG][%s] Exception: %s%n", uuid, e.getMessage());
            try {
                DocumentMetaData documentMetaData = DocumentMetaData.get(jc);
                throw new PipelineComponentException(comp.getPipelineComponent(), documentMetaData, e);
            } catch (IllegalArgumentException ignored) {
                throw new PipelineComponentException(comp.getPipelineComponent(), e);
            }
        } finally {
            System.out.printf("[DEBUG][%s] Adding component back to pool.%n", uuid);
            comp.addComponent(queue.getValue0());
        }
    }


    /**
     * @deprecated
     *
     * The process merchant describes the use of the component as a web socket
     * @param jc
     * @param comp
     * @param perf
     * @throws CASException
     * @throws PipelineComponentException
     */
    public static void process_handler(JCas jc,
                                       IDUUIInstantiatedPipelineComponent comp,
                                       DUUIPipelineDocumentPerformance perf) throws CASException, PipelineComponentException {

    }
}
