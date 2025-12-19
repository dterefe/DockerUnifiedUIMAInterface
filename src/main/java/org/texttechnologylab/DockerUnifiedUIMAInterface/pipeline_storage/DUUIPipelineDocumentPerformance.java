package org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage;

import com.arangodb.entity.BaseDocument;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.AppMetrics;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;


public class DUUIPipelineDocumentPerformance {
    private Vector<DUUIPipelinePerformancePoint> _points;
    private String _runKey;
    private Long _durationTotalSerialize;
    private Long _durationTotalDeserialize;
    private Long _durationTotalAnnotator;
    private Long _durationTotalMutexWait;
    private Long _durationTotal;
    private Integer _documentSize;
    private Long _documentWaitTime;
    private String document;

    /**
     * Stores the types of annotations and how many were made.
     */
    private Map<String, Integer> annotationTypesCount;

    /**
     * Whether to track error documents in the database or not
     */
    private final boolean trackErrorDocs;

    /**
     * Optional metrics handle for Prometheus-based profiling.
     */
    private final AppMetrics appMetrics;

    public DUUIPipelineDocumentPerformance(String runKey, long waitDocumentTime, JCas jc, boolean trackErrorDocs) {
        this(runKey, waitDocumentTime, jc, trackErrorDocs, null);
    }

    public DUUIPipelineDocumentPerformance(String runKey, long waitDocumentTime, JCas jc, boolean trackErrorDocs, AppMetrics appMetrics) {
        this.trackErrorDocs = trackErrorDocs;
        this.appMetrics = appMetrics;

        _points = new Vector<>();
        _runKey = runKey;

        _documentWaitTime = waitDocumentTime;
        _durationTotalDeserialize = 0L;
        _durationTotalSerialize = 0L;
        _durationTotalAnnotator = 0L;
        _durationTotalMutexWait = 0L;
        _durationTotal = 0L;
        if(jc.getDocumentText()!=null) {
            _documentSize = jc.getDocumentText().length();
        }
        else{
            _documentSize = -1;
        }

        try {
            DocumentMetaData meta = DocumentMetaData.get(jc);
            document = meta.getDocumentUri();
            if (document == null) {
                document = meta.getDocumentId();
            }
            if (document == null) {
                document = meta.getDocumentTitle();
            }
        }
        catch (Exception e){
            document = null;
        }
        annotationTypesCount = new HashMap<>();
    }

    /**
     * Whether to track error documents in the database or not
     * @return true if error documents should be tracked, false otherwise
     */
    public boolean shouldTrackErrorDocs() {
        return trackErrorDocs;
    }

    public String getRunKey() {
        return _runKey;
    }

    public Vector<DUUIPipelinePerformancePoint> getPerformancePoints() {
        return _points;
    }

    public void addData(long durationSerialize, long durationDeserialize, long durationAnnotator, long durationMutexWait, long durationComponentTotal, String componentKey, long serializeSize, JCas jc, String error) {
        _durationTotalDeserialize += durationDeserialize;
        _durationTotalSerialize += durationSerialize;
        _durationTotalAnnotator += durationAnnotator;
        _durationTotalMutexWait += durationMutexWait;
        _durationTotal += durationComponentTotal;

//        for (Annotation annotation : jc.getAnnotationIndex()) {
//            annotationTypesCount.put(
//                    annotation.getClass().getCanonicalName(),
//                    JCasUtil.select(jc, annotation.getClass()).size()
//            );
//        }

        _points.add(new DUUIPipelinePerformancePoint(durationSerialize,durationDeserialize,durationAnnotator,durationMutexWait,durationComponentTotal,componentKey,serializeSize, jc, error, document));
    }

    public long getDocumentWaitTime() {
        return _documentWaitTime;
    }

    public long getTotalTime() {
        return _durationTotal+_documentWaitTime;
    }

    public long getDocumentSize() {
        return _documentSize;
    }

    public Vector<BaseDocument> generateComponentPerformance(String docKey) {
        Vector<BaseDocument> docs = new Vector<>();
        for(DUUIPipelinePerformancePoint point : _points) {
            Map<String, Object> props = new HashMap<>();
            props.put("run", _runKey);
            props.put("compkey", point.getKey());
            props.put("performance",point.getProperties());
            props.put("docsize",_documentSize);
            BaseDocument doc = new BaseDocument();
            doc.setProperties(props);

            docs.add(doc);
        }
        return docs;
    }

    public BaseDocument toArangoDocument() {
        BaseDocument doc = new BaseDocument();
        Map<String,Object> props = new HashMap<>();

        props.put("pipelineKey",_runKey);
        props.put("total",_durationTotal);
        props.put("mutexsync",_durationTotalMutexWait);
        props.put("annotator",_durationTotalAnnotator);
        props.put("serialize",_durationTotalSerialize);
        props.put("deserialize",_durationTotalDeserialize);
        props.put("docsize",_documentSize);
        doc.setProperties(props);
        return doc;
    }

    public String getDocument() {
        return document;
    }

    public Map<String, Integer> getAnnotationTypesCount() {
        return annotationTypesCount;
    }

    /**
     * @return the {@link AppMetrics} instance associated with this document, if any.
     */
    public AppMetrics getAppMetrics() {
        return appMetrics;
    }

    /**
     * Convenience wrapper exposing {@link AppMetrics#timeStep(String)} via the
     * document performance object.
     *
     * @param step logical step name
     * @return a new {@link AppMetrics.Timer} for the given step
     */
    public AppMetrics.Timer timeStep(String step) {
        return AppMetrics.timeStep(step);
    }

    /**
     * Convenience wrapper exposing {@link AppMetrics#timeStep(String, String)} via the
     * document performance object.
     *
     * @param step      logical step name
     * @param component component identifier
     * @return a new {@link AppMetrics.Timer} for the given step/component pair
     */
    public AppMetrics.Timer timeStep(String step, String component) {
        return AppMetrics.timeStep(step, component);
    }

    /**
     * Convenience wrapper exposing {@link AppMetrics#docRun()} via the
     * document performance object.
     *
     * @return a new {@link AppMetrics.DocRun} instance
     */
    public AppMetrics.DocRun docRun() {
        return AppMetrics.docRun(sizeBucket());
    }

    private String sizeBucket() {
        long bytes = _documentSize == null ? -1L : _documentSize.longValue();
        if (bytes < 0) {
            return "unknown";
        }

        long mb = 1024L * 1024L;
        if (bytes < 1L * mb) {
            return "<1MB";
        }
        if (bytes < 10L * mb) {
            return "1MB-10MB";
        }
        if (bytes < 50L * mb) {
            return "10MB-50MB";
        }
        if (bytes < 100L * mb) {
            return "50MB-100MB";
        }
        if (bytes < 1024L * mb) {
            return "100MB-1GB";
        }
        return ">=1GB";
    }
}
