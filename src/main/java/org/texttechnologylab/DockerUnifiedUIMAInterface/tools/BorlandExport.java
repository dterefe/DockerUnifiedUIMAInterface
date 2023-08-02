package org.texttechnologylab.DockerUnifiedUIMAInterface.tools;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.SerialFormat;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.json.JsonCasSerializer;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.CasIOUtils;
import org.dkpro.core.api.io.JCasFileWriter_ImplBase;
import org.texttechnologylab.annotation.AnnotationComment;
import org.texttechnologylab.utilities.helper.BorlandUtils;
import org.texttechnologylab.utilities.helper.FileUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class BorlandExport extends JCasFileWriter_ImplBase {

    public static final String PARM_OUTPUTFILE = "output";
    @ConfigurationParameter(name = PARM_OUTPUTFILE, mandatory = false, defaultValue = "/tmp/export.bf.txt")
    protected String output;

    private Map<String, BorlandUtils.DATATYPE> nodes = new HashMap<>();
    private Map<String, BorlandUtils.DATATYPE> edges = new HashMap<>();

    public static StringBuilder outputString = new StringBuilder();


    @Override
    public void initialize(UimaContext context) throws ResourceInitializationException {
        super.initialize(context);

        nodes.put("verb", BorlandUtils.DATATYPE.String);
        nodes.put("chatgpt", BorlandUtils.DATATYPE.String);
        nodes.put("xmi", BorlandUtils.DATATYPE.String);
        nodes.put("json", BorlandUtils.DATATYPE.String);
//        nodes.put("ner", BorlandUtils.DATATYPE.StringList);

    }

    @Override
    public void process(JCas jCas) throws AnalysisEngineProcessException {

        Map<String, Object> objectMap = new HashMap<>();

        List<AnnotationComment> acList = JCasUtil.select(jCas, AnnotationComment.class).stream().filter(ac->{
            return ac.getKey().equalsIgnoreCase("content");
        }).collect(Collectors.toList());

        String sID = acList.get(0).getValue();
        if(acList.size()==1){
            objectMap.put("verb", acList.get(0).getValue());
        }
        objectMap.put("chatgpt", jCas.getDocumentText());

        ByteArrayOutputStream xmiOut = new ByteArrayOutputStream();
        ByteArrayOutputStream jsonOut = new ByteArrayOutputStream();
        try {
            CasIOUtils.save(jCas.getCas(), xmiOut, SerialFormat.XMI);
            objectMap.put("xmi", xmiOut.toString().replaceAll("\n", " "));
            JsonCasSerializer.jsonSerialize(jCas.getCas(), jsonOut);
            objectMap.put("json", jsonOut.toString().replaceAll("\n", " "));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        outputString.append(BorlandUtils.addVertex(sID.replaceAll("/","_"), objectMap));

    }

    @Override
    public void destroy() {

        String bOutput = BorlandUtils.createBorland(BorlandUtils.createHeader(nodes, edges), outputString.toString(), "");
        try {
            FileUtils.writeContent(bOutput, new File(output));
        } catch (IOException e) {
            e.printStackTrace();
        }

        super.destroy();
    }

    public static String getEntities(Set<Annotation> pSet){

        StringBuilder sb = new StringBuilder();


        pSet.stream().forEach(a->{

            if(sb.length()>0){
                sb.append(BorlandUtils.delemiter);
            }

        });

        return sb.toString();


    }
}
