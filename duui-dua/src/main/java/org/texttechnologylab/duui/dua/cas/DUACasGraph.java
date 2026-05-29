package org.texttechnologylab.duui.dua.cas;

import org.apache.uima.cas.CAS;
import org.apache.uima.cas.Feature;
import org.apache.uima.cas.FeatureStructure;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.text.AnnotationFS;
import org.texttechnologylab.duui.dua.graph.DUAGraphEdge;
import org.texttechnologylab.duui.dua.graph.DUAGraphNode;
import org.texttechnologylab.duui.dua.graph.DUAGraphPartition;

import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public final class DUACasGraph {
    public DUAGraphPartition fromCas(String corpusId, String documentId, CAS cas) {
        DUAGraphPartition partition = new DUAGraphPartition("cas-" + documentId, "document");
        String corpusNode = "corpus:" + corpusId;
        String documentNode = "document:" + documentId;
        partition.node(DUAGraphNode.of(corpusNode, "corpus").with("id", corpusId));
        partition.node(DUAGraphNode.of(documentNode, "document").with("id", documentId));
        partition.edge(new DUAGraphEdge(DUACasIds.create("edge"), "contains", corpusNode, documentNode, Map.of()));

        IdentityHashMap<FeatureStructure, String> fsIds = new IdentityHashMap<>();
        Iterator<CAS> views = cas.getViewIterator();
        while (views.hasNext()) {
            CAS view = views.next();
            String viewName = view.getViewName();
            String viewNode = documentNode + "/view:" + viewName;
            partition.node(DUAGraphNode.of(viewNode, "view")
                    .with("name", viewName)
                    .with("documentTextLength", view.getDocumentText() == null ? 0 : view.getDocumentText().length()));
            partition.edge(new DUAGraphEdge(DUACasIds.create("edge"), "hasView", documentNode, viewNode, Map.of()));
            indexView(partition, view, viewNode, fsIds);
        }
        return partition;
    }

    private void indexView(
            DUAGraphPartition partition,
            CAS view,
            String viewNode,
            IdentityHashMap<FeatureStructure, String> fsIds
    ) {
        Type top = view.getTypeSystem().getTopType();
        var iterator = view.getIndexRepository().getAllIndexedFS(top);
        while (iterator.hasNext()) {
            FeatureStructure fs = iterator.next();
            String fsNode = fsIds.computeIfAbsent(fs, ignored -> DUACasIds.create("fs"));
            Map<String, Object> properties = new LinkedHashMap<>();
            properties.put("type", fs.getType().getName());
            if (fs instanceof AnnotationFS annotation) {
                properties.put("begin", annotation.getBegin());
                properties.put("end", annotation.getEnd());
                properties.put("coveredText", annotation.getCoveredText());
            }
            partition.node(new DUAGraphNode(fsNode, "featureStructure", properties));
            partition.edge(new DUAGraphEdge(DUACasIds.create("edge"), "indexedIn", viewNode, fsNode, Map.of()));
            for (Feature feature : fs.getType().getFeatures()) {
                addFeature(partition, fs, fsNode, feature, fsIds);
            }
        }
    }

    private void addFeature(
            DUAGraphPartition partition,
            FeatureStructure fs,
            String fsNode,
            Feature feature,
            IdentityHashMap<FeatureStructure, String> fsIds
    ) {
        if (feature.getRange().isPrimitive()) {
            String value = fs.getFeatureValueAsString(feature);
            if (value != null) {
                String valueNode = DUACasIds.create("value");
                partition.node(DUAGraphNode.of(valueNode, "featureValue")
                        .with("feature", feature.getShortName())
                        .with("value", value));
                partition.edge(new DUAGraphEdge(DUACasIds.create("edge"), "hasFeature", fsNode, valueNode,
                        Map.of("feature", feature.getShortName())));
            }
            return;
        }
        FeatureStructure target = fs.getFeatureValue(feature);
        if (target != null) {
            String targetNode = fsIds.computeIfAbsent(target, ignored -> DUACasIds.create("fs"));
            partition.edge(new DUAGraphEdge(DUACasIds.create("edge"), "references", fsNode, targetNode,
                    Map.of("feature", feature.getShortName())));
        }
    }
}
