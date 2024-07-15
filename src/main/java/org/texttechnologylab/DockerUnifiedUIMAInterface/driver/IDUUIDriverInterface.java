package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.InvalidXMLException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Interface for all drivers
 *
 * @author Alexander Leonhardt
 */
public interface IDUUIDriverInterface {
    public void setLuaContext(DUUILuaContext luaContext);
    public boolean canAccept(DUUIPipelineComponent component) throws InvalidXMLException, IOException, SAXException;
    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification, AtomicBoolean shutdown) throws Exception;
    public void printConcurrencyGraph(String uuid);

    //TODO: public InputOutput get_inputs_and_outputs(String uuid)
    //Example: get_typesystem(...)
    public TypeSystemDescription get_typesystem(String uuid) throws InterruptedException, IOException, SAXException, CompressorException, ResourceInitializationException;
    public void run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf, DUUIComposer composer) throws InterruptedException, IOException, SAXException, AnalysisEngineProcessException, CompressorException, CASException;

    public boolean destroy(String uuid);
    public void shutdown();

}
