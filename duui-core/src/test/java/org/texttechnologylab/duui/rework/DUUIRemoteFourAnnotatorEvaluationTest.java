package org.texttechnologylab.duui.rework;

import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.UIMAFramework;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.XMLInputSource;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.duui.exception.DUUIExecutionResult;
import org.texttechnologylab.duui.exception.DUUIExecutionStatus;
import org.texttechnologylab.duui.orchestration.DUUIOrchestrationResult;
import org.texttechnologylab.duui.pipeline.io.DUUIXmiCollectionReader;
import org.texttechnologylab.duui.pipeline.io.DUUIXmiTarget;
import org.texttechnologylab.duui.runtime.*;

import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.jar.JarFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DUUIRemoteFourAnnotatorEvaluationTest {

    @Test void evaluateAllFourAnnotatorsRemoteV2() throws Exception { evaluate("v2",19714,19715,19716,19717); }
    @Test void evaluateAllFourAnnotatorsRemoteLegacy() throws Exception { evaluate("legacy",19814,19815,19816,19817); }

    private void evaluate(String engine, int... ports) throws Exception {
        String mp=System.getProperty("duui.eval.manifest","/storage/projects/BIOfid/code/dterefe/duui-py-eval-2026-06-01/manifests/top30-ascii-safe.tsv");
        int max=Integer.getInteger("duui.eval.max.docs",30);
        String out=System.getProperty("duui.eval.output.dir","/storage/projects/BIOfid/code/dterefe/duui-py-eval-2026-06-01/remote-eval-duui-30");
        List<Path> paths=new ArrayList<>();
        try(java.io.BufferedReader r=Files.newBufferedReader(Path.of(mp),java.nio.charset.StandardCharsets.UTF_8)){
            String l;while((l=r.readLine())!=null&&paths.size()<max){String[]p=l.split("\t");if(p.length>=2){try{paths.add(Path.of(p[1]));}catch(java.nio.file.InvalidPathException e){System.err.println("SKIP: "+p[1]);}}}
        }
        assertTrue(!paths.isEmpty(), "No manifest documents scheduled from " + mp + " with duui.eval.max.docs=" + max);
        TypeSystemDescription typeSystem = autoDetectedUimaTypeSystem();
        System.out.println("=== REMOTE "+engine.toUpperCase()+": "+ANNOTATORS.length+" x "+paths.size()+" docs ===");
        for(int i=0;i<ANNOTATORS.length;i++){
            String id=ANNOTATORS[i],ep="http://127.0.0.1:"+ports[i];
            Path xd=Path.of(out,id+"-"+engine,"xmi");Files.createDirectories(xd);
            System.out.println("\n--- "+id+" "+engine+" @ "+ep+" ---");
            long start=System.nanoTime();int ok=0,fail=0;
            DUUIOrchestrationResult r;
            try(DUUISystemScope s=DUUI.system(engine+"-"+id)){
                try(DUUIPipelineScope p=s.pipeline("p-"+engine+"-"+id)){
                    DUUIXmiCollectionReader.Builder reader=DUUIXmiCollectionReader.builder()
                            .typeSystem(typeSystem)
                            .casSupplier(() -> {
                                try {
                                    return JCasFactory.createJCas(typeSystem);
                                } catch (Exception e) {
                                    throw new IllegalStateException("Failed to create evaluation CAS from classpath type system", e);
                                }
                            });
                    for(Path path:paths){reader.source(path);}
                    try(var g=reader.build().open(p)){
                        try(var stage=g.linear(id)){
                            stage.v1(id).remote().endpoint(ep).sourceView("_InitialView").targetView("_InitialView").parameters(pm(id));
                        }
                        try(var target=DUUIXmiTarget.builder().output(xd).open(g)){
                            // target scope is registered by construction
                        }
                    }
                }
                r=s.run("p-"+engine+"-"+id);
                long wall=Duration.ofNanos(System.nanoTime()-start).toMillis();
                for(DUUIExecutionResult<?> e:r.results()){if(e.status()==DUUIExecutionStatus.SUCCESS)ok++;else fail++;}
                System.out.printf("  wall=%dms success=%d fail=%d%n",wall,ok,fail);
                assertTrue(!r.results().isEmpty(), id + " " + engine + " processed zero documents from " + paths.size() + " scheduled manifest docs");
                assertEquals(paths.size(), r.results().size(), id + " " + engine + " result count mismatch");
                assertFalse(r.hasFailures(), id + " " + engine + " had failed executions");
                assertEquals(0, r.unroutableArtifacts().size(), id + " " + engine + " had unroutable artifacts");
            }
        }
    }
    private static TypeSystemDescription autoDetectedUimaTypeSystem() throws Exception {
        List<TypeSystemDescription> descriptions = new ArrayList<>();
        for (String entry : System.getProperty("java.class.path", "").split(java.io.File.pathSeparator)) {
            Path path = Path.of(entry);
            if (Files.isDirectory(path)) {
                addDirectoryTypeSystems(descriptions, path, "desc/type");
                addDirectoryTypeSystems(descriptions, path, "org/texttechnologylab/types");
            } else if (Files.isRegularFile(path) && entry.endsWith(".jar")) {
                try (JarFile jar = new JarFile(path.toFile())) {
                    jar.stream()
                            .map(java.util.jar.JarEntry::getName)
                            .filter(name -> (name.startsWith("desc/type/") || name.startsWith("org/texttechnologylab/types/")) && name.endsWith(".xml"))
                            .sorted()
                            .forEach(name -> {
                                try {
                                    descriptions.add(UIMAFramework.getXMLParser().parseTypeSystemDescription(
                                            new XMLInputSource("jar:" + path.toUri() + "!/" + name)));
                                } catch (Exception e) {
                                    throw new IllegalStateException("Failed to parse classpath type system " + name + " from " + path, e);
                                }
                            });
                }
            }
        }
        if (descriptions.isEmpty()) {
            throw new IllegalStateException("No DUUI/UIMATypeSystem descriptors found on the test classpath");
        }
        return CasCreationUtils.mergeTypeSystems(descriptions);
    }

    private static void addDirectoryTypeSystems(List<TypeSystemDescription> descriptions, Path root, String relative) throws Exception {
        Path dir = root.resolve(relative);
        if (!Files.isDirectory(dir)) {
            return;
        }
        try (var stream = Files.walk(dir)) {
            for (Path file : stream.filter(path -> Files.isRegularFile(path) && path.toString().endsWith(".xml")).sorted().toList()) {
                descriptions.add(UIMAFramework.getXMLParser().parseTypeSystemDescription(new XMLInputSource(file.toFile())));
            }
        }
    }
    private static final String[] ANNOTATORS={"gnfinder","gazetteer","spacy","taxonerd"};
    private static Map<String,String> pm(String a){return switch(a){
        case"spacy"->Map.of("spacy_language","de","spacy_model_size","sm","spacy_batch_size","32","use_existing_sentences","false");
        case"taxonerd"->Map.of("model","en_ner_eco_md","linking","none","allow_unlinked","true","input_strategy","whole-document","prefer_gpu","false");
        default->Map.of();};}
}
