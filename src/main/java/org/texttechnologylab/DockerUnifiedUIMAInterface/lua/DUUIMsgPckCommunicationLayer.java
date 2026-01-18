package org.texttechnologylab.DockerUnifiedUIMAInterface.lua;

import org.apache.uima.cas.*;
import org.apache.uima.cas.impl.LowLevelCAS;
import org.apache.uima.cas.text.AnnotationFS;
import org.apache.uima.jcas.JCas;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ValueType;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIHttpRequestHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.CommunicationLayerException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.model.AnnotatorDescriptor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.SerDeUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

public final class DUUIMsgPckCommunicationLayer implements IDUUICommunicationLayer {
    private final AnnotatorDescriptor descriptor;
    private final String selectionTypeOrNull;

    public DUUIMsgPckCommunicationLayer(AnnotatorDescriptor descriptor, String selectionTypeOrNull) {
        this.descriptor = Objects.requireNonNull(descriptor, "descriptor");
        this.selectionTypeOrNull = selectionTypeOrNull;
    }

    @Override
    public void serialize(JCas jc, ByteArrayOutputStream out, Map<String, String> parameters, String sourceView)
            throws CommunicationLayerException, CASException {
        try {
            CAS selected = selectViewBySofa(
                    jc.getCas(),
                    descriptor.input().domain().sofa(),
                    sourceView,
                    false
            );
            byte[] bytes = serializeRequest(
                    jc.getCas(),
                    descriptor,
                    parameters,
                    selected.getViewName(),
                    selectionTypeOrNull
            );
            out.write(bytes);
        } catch (Exception e) {
            throw new CommunicationLayerException("Failed to serialize DUUI-BIN msgpack request", e);
        }
    }

    @Override
    public void deserialize(JCas jc, ByteArrayInputStream input, String targetView)
            throws CommunicationLayerException, CASException {
        try {
            String preferredView = (targetView != null && !targetView.isBlank())
                    ? targetView
                    : descriptor.targetView();
            CAS selected = selectViewBySofa(
                    jc.getCas(),
                    descriptor.output().sofa(),
                    preferredView,
                    true
            );
            byte[] bytes = input.readAllBytes();
            deserializeResponse(jc.getCas(), descriptor, bytes, selected.getViewName());
        } catch (Exception e) {
            throw new CommunicationLayerException("Failed to deserialize DUUI-BIN msgpack response", e);
        }
    }

    @Override
    public void process(JCas jCas, DUUIHttpRequestHandler handler, Map<String, String> parameters, JCas targetCas)
            throws CommunicationLayerException, CASException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsProcess() {
        return false;
    }

    @Override
    public boolean supportsSerialize() {
        return true;
    }

    @Override
    public IDUUICommunicationLayer copy() {
        return new DUUIMsgPckCommunicationLayer(descriptor, selectionTypeOrNull);
    }

    public AnnotatorDescriptor descriptor() {
        return descriptor;
    }

    private static String normLang(String lang) {
        if (lang == null || lang.isBlank()) return "x-unspecified";
        return lang.trim();
    }

    private static String effectiveMimeType(CAS view) {
        String mt = view.getSofaMimeType();
        if (mt != null && !mt.isBlank()) return mt;
        return view.getDocumentText() != null ? "text/plain" : null;
    }

    private static CAS selectViewBySofa(
            CAS base,
            AnnotatorDescriptor.SofaDesc desc,
            String preferredViewOrNull,
            boolean createIfMissing) {
        String lang = normLang(desc.language());

        List<CAS> candidates = new ArrayList<>();
        CAS preferredCandidate = null;
        StringBuilder viewsDebug = new StringBuilder();

        Iterator<CAS> it = base.getViewIterator();
        while (it.hasNext()) {
            CAS v = it.next();
            String name = v.getViewName();
            String mt = effectiveMimeType(v);
            String lg = normLang(v.getDocumentLanguage());

            viewsDebug
                    .append(name)
                    .append(" {mimeType=").append(mt)
                    .append(", language=").append(lg)
                    .append("}\n");

            if (mt != null && SerDeUtils.mimeMatches(desc.mimeType(), mt) && lang.equals(lg)) {
                candidates.add(v);
                if (preferredCandidate == null && preferredViewOrNull != null && preferredViewOrNull.equals(name)) {
                    preferredCandidate = v;
                }
            }
        }

        String debug = viewsDebug.toString();

        if (preferredCandidate != null) return preferredCandidate;
        if (candidates.size() == 1) return candidates.get(0);
        if (candidates.size() > 1) {
            throw new IllegalArgumentException(
                    "Ambiguous SofA match (" + candidates.size() + ") for Annotator Descriptor: {mimeType=" + desc.mimeType() +
                            ", language=" + lang + "}\nCAS-Views:\n" + debug
            );
        }

        if (!createIfMissing) {
            throw new IllegalArgumentException(
                    "No SofA view matches Annotator Descriptor: {mimeType=" + desc.mimeType() + ", language=" + lang + "}\nCAS-Views:\n" + debug
            );
        }

        String name = (preferredViewOrNull != null && !preferredViewOrNull.isBlank())
                ? preferredViewOrNull
                : ("_duui_" + desc.mimeType().replaceAll("[^a-zA-Z0-9]+", "_") + "_" + lang);

        CAS v;
        try {
            v = base.getView(name);
        } catch (CASRuntimeException e) {
            v = base.createView(name);
        }
        v.setDocumentLanguage(lang);
        return v;
    }

    public static byte[] serializeRequest(
            CAS cas,
            AnnotatorDescriptor desc,
            Map<String, String> parameters,
            String sourceView,
            String selectionTypeOrNull) throws Exception {
        String sv = (sourceView == null) ? "" : sourceView;
        CAS view = sv.isEmpty() ? cas : cas.getView(sv);

        // selectionType reserved (currently unused)
        String selectionType = null;

        IdentityHashMap<FeatureStructure, Integer> ids = new IdentityHashMap<>();
        ArrayDeque<FeatureStructure> queue = new ArrayDeque<>();
        ArrayList<FeatureStructure> ordered = new ArrayList<>();

        for (AnnotatorDescriptor.InputTypeSpec spec : desc.input().optional_inputs()) {
            Type t = view.getTypeSystem().getType(spec.type());
            if (t == null) continue;
            FSIterator<FeatureStructure> it = view.getIndexRepository().getAllIndexedFS(t);
            while (it.hasNext()) enqueue(it.next(), ids, queue, ordered);
        }

        while (!queue.isEmpty()) {
            FeatureStructure fs = queue.removeFirst();

            Type t = fs.getType();
            for (Feature f : t.getFeatures()) {
                if (isExcludedFeature(view, desc, selectionType, fs, f)) continue;

                Type range = f.getRange();
                if (range.isPrimitive()) continue;

                FeatureStructure val = fs.getFeatureValue(f);
                if (val == null) continue;

                if (val instanceof ArrayFS arr) {
                    for (int i = 0; i < arr.size(); i++) enqueue(arr.get(i), ids, queue, ordered);
                    continue;
                }
                if (isFsListType(val.getType())) {
                    Feature headF = val.getType().getFeatureByBaseName(CAS.FEATURE_BASE_NAME_HEAD);
                    Feature tailF = val.getType().getFeatureByBaseName(CAS.FEATURE_BASE_NAME_TAIL);
                    FeatureStructure cur = val;
                    while (cur != null && isNonEmptyListType(cur.getType())) {
                        FeatureStructure head = cur.getFeatureValue(headF);
                        enqueue(head, ids, queue, ordered);
                        cur = cur.getFeatureValue(tailF);
                    }
                    continue;
                }

                enqueue(val, ids, queue, ordered);
            }
        }

        try (MessageBufferPacker p = MessagePack.newDefaultBufferPacker()) {
            p.packMapHeader(5);
            packStr(p, "descriptor");
            packDescriptor(p, desc);
            packStr(p, "parameters");
            packStringMap(p, parameters == null ? Map.of() : parameters);
            packStr(p, "view");
            packStr(p, sv);
            packStr(p, "sofa");
            packSofa(p, view, desc.input().domain().sofa());
            packStr(p, "fs");
            packFsArray(p, view, desc, selectionType, ordered, ids);
            p.flush();
            return p.toByteArray();
        }
    }

    public static void deserializeResponse(CAS cas, AnnotatorDescriptor desc, byte[] msgpackBytes, String targetView) throws Exception {
        String tv = targetView == null ? "" : targetView;
        CAS view = tv.isEmpty() ? cas : cas.getView(tv);
        TypeSystem ts = view.getTypeSystem();
        LowLevelCAS ll = view.getLowLevelCAS();

        Envelope env = unpackEnvelope(msgpackBytes);

        if (env.sofa() != null) {
            applySofa(view, env.sofa().mimeType(), env.sofa().language(), env.sofa().data());
        }

        Set<FeatureStructure> created = Collections.newSetFromMap(new IdentityHashMap<>());
        Map<Integer, FeatureStructure> byId = new HashMap<>();
        for (FsRec r : env.fs()) {
            Type t = ts.getType(r.typeName());
            if (t == null) continue;
            FeatureStructure fs = null;
            if (r.ref() != null && r.ref() > 0) {
                fs = ll.ll_getFSForRef(r.ref());
            }
            if (fs == null) {
                fs = view.createFS(t);
                created.add(fs);
            } else if (!fs.getType().getName().equals(r.typeName())) {
                throw new IllegalArgumentException(
                        "ref type mismatch: ref=" + r.ref() + " is " + fs.getType().getName() +
                                " but response says " + r.typeName()
                );
            }
            byId.put(r.id(), fs);
        }

        for (FsRec r : env.fs()) {
            FeatureStructure fs = byId.get(r.id());
            if (fs == null) continue;
            Type t = fs.getType();

            if (fs instanceof AnnotationFS a && r.begin() != null && r.end() != null) {
                boolean wasIndexed = false;
                try {
                    view.removeFsFromIndexes(fs);
                    wasIndexed = true;
                } catch (Exception ignored) {
                    wasIndexed = false;
                }
                a.setBegin(r.begin());
                a.setEnd(r.end());
                if (wasIndexed) view.addFsToIndexes(fs);
            }

            for (Map.Entry<String, Object> e : r.features().entrySet()) {
                Feature f = t.getFeatureByBaseName(e.getKey());
                if (f == null) continue;
                setFeatureValue(view, fs, f, e.getValue(), byId);
            }

            if (created.contains(fs)) {
                Type annoBase = ts.getType(CAS.TYPE_NAME_ANNOTATION_BASE);
                if (annoBase != null && ts.subsumes(annoBase, t)) {
                    view.addFsToIndexes(fs);
                }
            }
        }
    }

    private static void enqueue(
            FeatureStructure fs,
            IdentityHashMap<FeatureStructure, Integer> ids,
            ArrayDeque<FeatureStructure> queue,
            ArrayList<FeatureStructure> ordered) {
        if (fs == null) return;
        if (ids.containsKey(fs)) return;
        int id = ids.size() + 1;
        ids.put(fs, id);
        ordered.add(fs);
        queue.add(fs);
    }

    private static void packFsArray(
            MessageBufferPacker p,
            CAS view,
            AnnotatorDescriptor desc,
            String selectionType,
            List<FeatureStructure> ordered,
            IdentityHashMap<FeatureStructure, Integer> ids) throws Exception {
        p.packArrayHeader(ordered.size());
        for (FeatureStructure fs : ordered) packOneFs(p, view, desc, selectionType, fs, ids);
    }

    private static void packOneFs(
            MessageBufferPacker p,
            CAS view,
            AnnotatorDescriptor desc,
            String selectionType,
            FeatureStructure fs,
            IdentityHashMap<FeatureStructure, Integer> ids) throws Exception {
        Type t = fs.getType();
        int id = ids.get(fs);
        int ref = view.getLowLevelCAS().ll_getFSRef(fs);

        boolean isAnno = fs instanceof AnnotationFS;
        int fields = isAnno ? 5 : 4;

        p.packMapHeader(fields);
        packStr(p, "id");
        p.packInt(id);
        packStr(p, "ref");
        p.packInt(ref);
        packStr(p, "type");
        packStr(p, t.getName());

        if (isAnno) {
            AnnotationFS a = (AnnotationFS) fs;
            packStr(p, "begin");
            p.packInt(a.getBegin());
            packStr(p, "end");
            p.packInt(a.getEnd());
        }

        packStr(p, "features");
        List<Feature> feats = t.getFeatures();
        int count = 0;
        for (Feature f : feats) {
            if (isExcludedFeature(view, desc, selectionType, fs, f)) continue;
            count++;
        }
        p.packMapHeader(count);
        for (Feature f : feats) {
            if (isExcludedFeature(view, desc, selectionType, fs, f)) continue;
            packStr(p, f.getShortName());
            packFeatureValue(p, view, fs, f, ids);
        }
    }

    private static boolean isExcludedFeature(CAS view, AnnotatorDescriptor desc, String selectionType, FeatureStructure owner, Feature f) {
        List<AnnotatorDescriptor.InputTypeSpec> specs = desc.input().optional_inputs();
        for (AnnotatorDescriptor.InputTypeSpec s : specs) {
            Type seedT = view.getTypeSystem().getType(s.type());
            if (seedT == null) continue;
            if (!view.getTypeSystem().subsumes(seedT, owner.getType())) continue;

            AnnotatorDescriptor.ExcludeSpec ex = s.exclude();
            if (ex == null) return false;

            if (ex.features() != null && ex.features().contains(f.getShortName())) return true;

            Type r = f.getRange();
            if (ex.ranges() != null) {
                for (String rn : ex.ranges()) {
                    Type rt = view.getTypeSystem().getType(rn);
                    if (rt != null && view.getTypeSystem().subsumes(rt, r)) return true;
                }
            }

            if (ex.types() != null) {
                FeatureStructure v = owner.getFeatureValue(f);
                if (v != null && containsExcludedType(view, v, ex.types())) return true;
            }
        }
        return false;
    }

    private static boolean containsExcludedType(CAS view, FeatureStructure v, List<String> excludeTypes) {
        if (excludeTypes == null || excludeTypes.isEmpty() || v == null) return false;
        TypeSystem ts = view.getTypeSystem();

        for (String tn : excludeTypes) {
            Type tt = ts.getType(tn);
            if (tt != null && ts.subsumes(tt, v.getType())) return true;
        }

        if (v instanceof ArrayFS arr) {
            for (int i = 0; i < arr.size(); i++) {
                FeatureStructure it = arr.get(i);
                if (it != null && containsExcludedType(view, it, excludeTypes)) return true;
            }
        } else if (isFsListType(v.getType())) {
            for (FeatureStructure it : readFsListItems(v)) {
                if (it != null && containsExcludedType(view, it, excludeTypes)) return true;
            }
        }

        return false;
    }

    private static void packFeatureValue(
            MessageBufferPacker p,
            CAS view,
            FeatureStructure fs,
            Feature f,
            IdentityHashMap<FeatureStructure, Integer> ids) throws Exception {
        Type r = f.getRange();
        if (r.isPrimitive()) {
            switch (r.getName()) {
                case CAS.TYPE_NAME_BOOLEAN -> p.packBoolean(fs.getBooleanValue(f));
                case CAS.TYPE_NAME_BYTE, CAS.TYPE_NAME_SHORT, CAS.TYPE_NAME_INTEGER -> p.packInt(fs.getIntValue(f));
                case CAS.TYPE_NAME_LONG -> p.packLong(fs.getLongValue(f));
                case CAS.TYPE_NAME_FLOAT -> p.packFloat(fs.getFloatValue(f));
                case CAS.TYPE_NAME_DOUBLE -> p.packDouble(fs.getDoubleValue(f));
                case CAS.TYPE_NAME_STRING -> {
                    String s = fs.getStringValue(f);
                    if (s == null) p.packNil();
                    else packStr(p, s);
                }
                default -> p.packNil();
            }
            return;
        }

        FeatureStructure val = fs.getFeatureValue(f);
        if (val == null) {
            p.packNil();
            return;
        }

        if (val instanceof FloatArrayFS fa) {
            packF32(p, fa);
            return;
        }
        if (val instanceof DoubleArrayFS da) {
            packF64(p, da);
            return;
        }
        if (val instanceof IntArrayFS ia) {
            packI32(p, ia);
            return;
        }
        if (val instanceof LongArrayFS la) {
            packI64(p, la);
            return;
        }

        if (val instanceof ByteArrayFS ba) {
            byte[] data = new byte[ba.size()];
            ba.copyToArray(0, data, 0, ba.size());
            packBin(p, data);
            return;
        }

        if (val instanceof StringArrayFS sa) {
            p.packArrayHeader(sa.size());
            for (int i = 0; i < sa.size(); i++) {
                String s = sa.get(i);
                if (s == null) p.packNil();
                else packStr(p, s);
            }
            return;
        }

        if (val instanceof ArrayFS arr) {
            p.packArrayHeader(arr.size());
            for (int i = 0; i < arr.size(); i++) packRefOrNil(p, arr.get(i), ids);
            return;
        }

        if (isFsListType(val.getType())) {
            List<FeatureStructure> items = readFsListItems(val);
            p.packArrayHeader(items.size());
            for (FeatureStructure it : items) packRefOrNil(p, it, ids);
            return;
        }
        if (isStringListType(val.getType())) {
            List<String> items = readStringListItems(val);
            p.packArrayHeader(items.size());
            for (String it : items) {
                if (it == null) p.packNil();
                else packStr(p, it);
            }
            return;
        }

        packRefOrNil(p, val, ids);
    }

    private static void packRefOrNil(MessageBufferPacker p, FeatureStructure fs, IdentityHashMap<FeatureStructure, Integer> ids) throws Exception {
        if (fs == null) {
            p.packNil();
            return;
        }
        Integer id = ids.get(fs);
        if (id == null) {
            p.packNil();
            return;
        }
        p.packMapHeader(1);
        packStr(p, "$ref");
        p.packInt(id);
    }

    private static void packF32(MessageBufferPacker p, FloatArrayFS fa) throws Exception {
        ByteBuffer bb = ByteBuffer.allocate(fa.size() * 4).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < fa.size(); i++) bb.putFloat(fa.get(i));
        p.packMapHeader(1);
        packStr(p, "$f32");
        packBin(p, bb.array());
    }

    private static void packF64(MessageBufferPacker p, DoubleArrayFS da) throws Exception {
        ByteBuffer bb = ByteBuffer.allocate(da.size() * 8).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < da.size(); i++) bb.putDouble(da.get(i));
        p.packMapHeader(1);
        packStr(p, "$f64");
        packBin(p, bb.array());
    }

    private static void packI32(MessageBufferPacker p, IntArrayFS ia) throws Exception {
        ByteBuffer bb = ByteBuffer.allocate(ia.size() * 4).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < ia.size(); i++) bb.putInt(ia.get(i));
        p.packMapHeader(1);
        packStr(p, "$i32");
        packBin(p, bb.array());
    }

    private static void packI64(MessageBufferPacker p, LongArrayFS la) throws Exception {
        ByteBuffer bb = ByteBuffer.allocate(la.size() * 8).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < la.size(); i++) bb.putLong(la.get(i));
        p.packMapHeader(1);
        packStr(p, "$i64");
        packBin(p, bb.array());
    }

    private static void packDescriptor(MessageBufferPacker p, AnnotatorDescriptor d) throws Exception {
        p.packMapHeader(6);
        packStr(p, "name");
        packStr(p, d.name());
        packStr(p, "version");
        packStr(p, d.version());
        packStr(p, "input");
        packInputDesc(p, d.input());
        packStr(p, "output");
        packOutputDesc(p, d.output());
        packStr(p, "sourceView");
        packStr(p, d.sourceView());
        packStr(p, "targetView");
        packStr(p, d.targetView());
    }

    private static void packInputDesc(MessageBufferPacker p, AnnotatorDescriptor.InputDesc in) throws Exception {
        p.packMapHeader(2);
        packStr(p, "domain");
        packDomain(p, in.domain());
        packStr(p, "optional_inputs");
        packInputTypes(p, in.optional_inputs());
    }

    private static void packInputTypes(MessageBufferPacker p, List<AnnotatorDescriptor.InputTypeSpec> xs) throws Exception {
        p.packArrayHeader(xs.size());
        for (AnnotatorDescriptor.InputTypeSpec x : xs) {
            p.packMapHeader(2);
            packStr(p, "type");
            packStr(p, x.type());
            packStr(p, "exclude");
            packExclude(p, x.exclude());
        }
    }

    private static void packExclude(MessageBufferPacker p, AnnotatorDescriptor.ExcludeSpec ex) throws Exception {
        if (ex == null) {
            p.packNil();
            return;
        }
        p.packMapHeader(3);
        packStr(p, "features");
        packStringList(p, ex.features() == null ? List.of() : ex.features());
        packStr(p, "ranges");
        packStringList(p, ex.ranges() == null ? List.of() : ex.ranges());
        packStr(p, "types");
        packStringList(p, ex.types() == null ? List.of() : ex.types());
    }

    private static void packOutputDesc(MessageBufferPacker p, AnnotatorDescriptor.OutputDesc out) throws Exception {
        p.packMapHeader(2);
        packStr(p, "sofa");
        packSofaDesc(p, out.sofa());
        packStr(p, "types");
        packStringList(p, out.types() == null ? List.of() : out.types());
    }

    private static void packDomain(MessageBufferPacker p, AnnotatorDescriptor.Domain d) throws Exception {
        p.packMapHeader(2);
        packStr(p, "sofa");
        packSofaDesc(p, d.sofa());
        packStr(p, "optional_types");
        packStringList(p, d.optional_types() == null ? List.of() : d.optional_types());
    }

    private static void packSofaDesc(MessageBufferPacker p, AnnotatorDescriptor.SofaDesc s) throws Exception {
        p.packMapHeader(2);
        packStr(p, "mimeType");
        packStr(p, s.mimeType());
        packStr(p, "language");
        packStr(p, s.language());
    }

    private static void packStringMap(MessageBufferPacker p, Map<String, String> m) throws Exception {
        p.packMapHeader(m.size());
        for (var e : m.entrySet()) {
            packStr(p, e.getKey());
            packStr(p, e.getValue());
        }
    }

    private static void packStringList(MessageBufferPacker p, List<String> xs) throws Exception {
        p.packArrayHeader(xs.size());
        for (String x : xs) packStr(p, x);
    }

    private static void packSofa(MessageBufferPacker p, CAS view, AnnotatorDescriptor.SofaDesc sofaDesc) throws Exception {
        String actualMimeType = view.getSofaMimeType();
        if (actualMimeType == null || actualMimeType.isBlank()) {
            actualMimeType = view.getDocumentText() != null ? "text/plain" : "application/octet-stream";
        }
        String actualLanguage = view.getDocumentLanguage();
        if (actualLanguage == null || actualLanguage.isBlank()) {
            actualLanguage = sofaDesc.language();
        }
        boolean isText = SerDeUtils.mimeMatches(SerDeUtils.MimePrimitive.TEXT, actualMimeType);

        p.packMapHeader(3);
        packStr(p, "mimeType");
        packStr(p, actualMimeType);
        packStr(p, "language");
        packStr(p, actualLanguage);
        packStr(p, "data");
        if (isText) {
            String s = view.getSofaDataString();
            if (s == null) s = "";
            packStr(p, s);
        } else {
            byte[] bytes = readAllBytes(view.getSofaDataStream());
            packBin(p, bytes);
        }
    }

    private static byte[] readAllBytes(InputStream in) throws Exception {
        try (in) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buf = new byte[8192];
            int r;
            while ((r = in.read(buf)) != -1) out.write(buf, 0, r);
            return out.toByteArray();
        }
    }

    private static void packBin(MessageBufferPacker p, byte[] data) throws Exception {
        p.packBinaryHeader(data.length);
        p.writePayload(data);
    }

    private static void packStr(MessageBufferPacker p, String s) throws Exception {
        p.packString(s);
    }

    private static boolean isFsListType(Type t) {
        return CAS.TYPE_NAME_FS_LIST.equals(t.getName()) || t.getName().endsWith("FSList");
    }

    private static boolean isStringListType(Type t) {
        return CAS.TYPE_NAME_STRING_LIST.equals(t.getName()) || t.getName().endsWith("StringList");
    }

    private static boolean isNonEmptyListType(Type t) {
        return t.getName().contains("NonEmpty") ||
                t.getName().endsWith("NonEmptyFSList") ||
                t.getName().endsWith("NonEmptyStringList");
    }

    private static List<FeatureStructure> readFsListItems(FeatureStructure listFs) {
        List<FeatureStructure> out = new ArrayList<>();
        Type t = listFs.getType();
        Feature headF = t.getFeatureByBaseName(CAS.FEATURE_BASE_NAME_HEAD);
        Feature tailF = t.getFeatureByBaseName(CAS.FEATURE_BASE_NAME_TAIL);
        FeatureStructure cur = listFs;
        while (cur != null && isNonEmptyListType(cur.getType())) {
            out.add(cur.getFeatureValue(headF));
            cur = cur.getFeatureValue(tailF);
        }
        return out;
    }

    private static List<String> readStringListItems(FeatureStructure listFs) {
        List<String> out = new ArrayList<>();
        Type t = listFs.getType();
        Feature headF = t.getFeatureByBaseName(CAS.FEATURE_BASE_NAME_HEAD);
        Feature tailF = t.getFeatureByBaseName(CAS.FEATURE_BASE_NAME_TAIL);
        FeatureStructure cur = listFs;
        while (cur != null && isNonEmptyListType(cur.getType())) {
            out.add(cur.getStringValue(headF));
            cur = cur.getFeatureValue(tailF);
        }
        return out;
    }

    private record SofaPayload(String mimeType, String language, Object data) {}

    private record FsRec(int id, Integer ref, String typeName, Integer begin, Integer end, Map<String, Object> features) {}

    private record Envelope(SofaPayload sofa, List<FsRec> fs) {}

    private static Envelope unpackEnvelope(byte[] bytes) throws Exception {
        try (MessageUnpacker u = MessagePack.newDefaultUnpacker(bytes)) {
            int n = u.unpackMapHeader();
            SofaPayload sofa = null;
            List<FsRec> fs = List.of();
            for (int i = 0; i < n; i++) {
                String k = u.unpackString();
                switch (k) {
                    case "sofa" -> sofa = unpackSofa(u);
                    case "fs" -> fs = unpackFsArray(u);
                    default -> u.skipValue();
                }
            }
            return new Envelope(sofa, fs);
        }
    }

    private static SofaPayload unpackSofa(MessageUnpacker u) throws Exception {
        int n = u.unpackMapHeader();
        String mime = null;
        String lang = null;
        Object data = null;
        for (int i = 0; i < n; i++) {
            String k = u.unpackString();
            switch (k) {
                case "mimeType" -> mime = u.unpackString();
                case "language" -> lang = u.unpackString();
                case "data" -> data = unpackAny(u);
                default -> u.skipValue();
            }
        }
        return new SofaPayload(mime, lang, data);
    }

    private static List<FsRec> unpackFsArray(MessageUnpacker u) throws Exception {
        int n = u.unpackArrayHeader();
        ArrayList<FsRec> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            if (u.getNextFormat().getValueType() != ValueType.MAP) {
                u.skipValue();
                continue;
            }
            int m = u.unpackMapHeader();
            Integer id = null;
            Integer ref = null;
            Integer begin = null;
            Integer end = null;
            String typeName = null;
            Map<String, Object> feats = new HashMap<>();
            for (int j = 0; j < m; j++) {
                String k = u.unpackString();
                switch (k) {
                    case "id" -> id = u.unpackInt();
                    case "ref" -> ref = u.unpackInt();
                    case "type" -> typeName = u.unpackString();
                    case "begin" -> begin = u.unpackInt();
                    case "end" -> end = u.unpackInt();
                    case "features" -> feats = unpackStringKeyMap(u);
                    default -> u.skipValue();
                }
            }
            if (id == null || typeName == null) continue;
            out.add(new FsRec(id, ref, typeName, begin, end, feats));
        }
        return out;
    }

    private static Map<String, Object> unpackStringKeyMap(MessageUnpacker u) throws Exception {
        int n = u.unpackMapHeader();
        Map<String, Object> out = HashMap.newHashMap(n);
        for (int i = 0; i < n; i++) {
            String k = u.unpackString();
            out.put(k, unpackAny(u));
        }
        return out;
    }

    private static Object unpackAny(MessageUnpacker u) throws Exception {
        return switch (u.getNextFormat().getValueType()) {
            case NIL -> {
                u.unpackNil();
                yield null;
            }
            case BOOLEAN -> u.unpackBoolean();
            case INTEGER -> {
                long v = u.unpackLong();
                yield (v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE) ? (int) v : v;
            }
            case FLOAT -> u.unpackDouble();
            case STRING -> u.unpackString();
            case BINARY -> u.readPayload(u.unpackBinaryHeader());
            case ARRAY -> {
                int n = u.unpackArrayHeader();
                ArrayList<Object> xs = new ArrayList<>(n);
                for (int i = 0; i < n; i++) xs.add(unpackAny(u));
                yield xs;
            }
            case MAP -> {
                int n = u.unpackMapHeader();
                Map<String, Object> m = HashMap.newHashMap(n);
                for (int i = 0; i < n; i++) {
                    String k = u.unpackString();
                    m.put(k, unpackAny(u));
                }
                yield m;
            }
            case EXTENSION -> {
                u.skipValue();
                yield null;
            }
        };
    }

    private static void applySofa(CAS view, String mimeType, String language, Object data) {
        if (mimeType == null || mimeType.isBlank()) return;
        if (language == null || language.isBlank()) return;

        boolean isText = SerDeUtils.mimeMatches(SerDeUtils.MimePrimitive.TEXT, mimeType);
        if (isText) {
            if (!(data instanceof String)) throw new IllegalArgumentException("text SofA requires string data");
            view.setSofaDataString((String) data, mimeType);
            view.setDocumentLanguage(language);
            return;
        }

        if (!(data instanceof byte[])) throw new IllegalArgumentException("non-text SofA requires bytes data");
        byte[] b = (byte[]) data;
        ByteArrayFS arr = view.createByteArrayFS(b.length);
        arr.copyFromArray(b, 0, 0, b.length);
        view.setSofaDataArray(arr, mimeType);
        view.setDocumentLanguage(language);
    }

    private static void setFeatureValue(CAS view, FeatureStructure owner, Feature f, Object v, Map<Integer, FeatureStructure> byId) {
        if (v == null) return;
        Type r = f.getRange();
        if (r.isPrimitive()) {
            String rn = r.getName();
            switch (rn) {
                case CAS.TYPE_NAME_BOOLEAN -> owner.setBooleanValue(f, (boolean) v);
                case CAS.TYPE_NAME_BYTE, CAS.TYPE_NAME_SHORT, CAS.TYPE_NAME_INTEGER ->
                        owner.setIntValue(f, ((Number) v).intValue());
                case CAS.TYPE_NAME_LONG -> owner.setLongValue(f, ((Number) v).longValue());
                case CAS.TYPE_NAME_FLOAT -> owner.setFloatValue(f, ((Number) v).floatValue());
                case CAS.TYPE_NAME_DOUBLE -> owner.setDoubleValue(f, ((Number) v).doubleValue());
                case CAS.TYPE_NAME_STRING -> owner.setStringValue(f, (String) v);
            }
            return;
        }

        if (v instanceof Map<?, ?> m) {
            if (m.containsKey("$ref")) {
                int id = ((Number) m.get("$ref")).intValue();
                FeatureStructure ref = byId.get(id);
                if (ref != null) owner.setFeatureValue(f, ref);
                return;
            }
            if (m.containsKey("$f32")) {
                owner.setFeatureValue(f, unpackF32Array(view, (byte[]) m.get("$f32")));
                return;
            }
            if (m.containsKey("$f64")) {
                owner.setFeatureValue(f, unpackF64Array(view, (byte[]) m.get("$f64")));
                return;
            }
            if (m.containsKey("$i32")) {
                owner.setFeatureValue(f, unpackI32Array(view, (byte[]) m.get("$i32")));
                return;
            }
            if (m.containsKey("$i64")) {
                owner.setFeatureValue(f, unpackI64Array(view, (byte[]) m.get("$i64")));
                return;
            }
        }

        if (v instanceof List<?> xs) {
            String rn = r.getName();

            if (rn.equals(CAS.TYPE_NAME_STRING_ARRAY)) {
                StringArrayFS a = view.createStringArrayFS(xs.size());
                for (int i = 0; i < xs.size(); i++) a.set(i, (String) xs.get(i));
                owner.setFeatureValue(f, a);
                return;
            }
            if (rn.equals(CAS.TYPE_NAME_FS_ARRAY)) {
                ArrayFS a = view.createArrayFS(xs.size());
                for (int i = 0; i < xs.size(); i++) {
                    Object it = xs.get(i);
                    if (it instanceof Map<?, ?> mm && mm.containsKey("$ref")) {
                        int id = ((Number) mm.get("$ref")).intValue();
                        a.set(i, byId.get(id));
                    }
                }
                owner.setFeatureValue(f, a);
                return;
            }
            if (rn.equals(CAS.TYPE_NAME_FS_LIST)) {
                owner.setFeatureValue(f, buildFsList(view, xs, byId));
                return;
            }
            if (rn.equals(CAS.TYPE_NAME_STRING_LIST)) {
                owner.setFeatureValue(f, buildStringList(view, xs));
                return;
            }
        }
    }

    private static FeatureStructure unpackF32Array(CAS view, byte[] bin) {
        int n = bin.length / 4;
        FloatArrayFS a = view.createFloatArrayFS(n);
        ByteBuffer bb = ByteBuffer.wrap(bin).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < n; i++) a.set(i, bb.getFloat());
        return a;
    }

    private static FeatureStructure unpackF64Array(CAS view, byte[] bin) {
        int n = bin.length / 8;
        DoubleArrayFS a = view.createDoubleArrayFS(n);
        ByteBuffer bb = ByteBuffer.wrap(bin).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < n; i++) a.set(i, bb.getDouble());
        return a;
    }

    private static FeatureStructure unpackI32Array(CAS view, byte[] bin) {
        int n = bin.length / 4;
        IntArrayFS a = view.createIntArrayFS(n);
        ByteBuffer bb = ByteBuffer.wrap(bin).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < n; i++) a.set(i, bb.getInt());
        return a;
    }

    private static FeatureStructure unpackI64Array(CAS view, byte[] bin) {
        int n = bin.length / 8;
        LongArrayFS a = view.createLongArrayFS(n);
        ByteBuffer bb = ByteBuffer.wrap(bin).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < n; i++) a.set(i, bb.getLong());
        return a;
    }

    private static FeatureStructure buildFsList(CAS view, List<?> xs, Map<Integer, FeatureStructure> byId) {
        TypeSystem ts = view.getTypeSystem();
        Type emptyT = ts.getType(CAS.TYPE_NAME_EMPTY_FS_LIST);
        Type nonEmptyT = ts.getType(CAS.TYPE_NAME_NON_EMPTY_FS_LIST);
        Feature headF = nonEmptyT.getFeatureByBaseName(CAS.FEATURE_BASE_NAME_HEAD);
        Feature tailF = nonEmptyT.getFeatureByBaseName(CAS.FEATURE_BASE_NAME_TAIL);

        FeatureStructure tail = view.createFS(emptyT);
        for (int i = xs.size() - 1; i >= 0; i--) {
            FeatureStructure cell = view.createFS(nonEmptyT);
            Object it = xs.get(i);
            FeatureStructure head = null;
            if (it instanceof Map<?, ?> m && m.containsKey("$ref")) head = byId.get(((Number) m.get("$ref")).intValue());
            cell.setFeatureValue(headF, head);
            cell.setFeatureValue(tailF, tail);
            tail = cell;
        }
        return tail;
    }

    private static FeatureStructure buildStringList(CAS view, List<?> xs) {
        TypeSystem ts = view.getTypeSystem();
        Type emptyT = ts.getType(CAS.TYPE_NAME_EMPTY_STRING_LIST);
        Type nonEmptyT = ts.getType(CAS.TYPE_NAME_NON_EMPTY_STRING_LIST);
        Feature headF = nonEmptyT.getFeatureByBaseName(CAS.FEATURE_BASE_NAME_HEAD);
        Feature tailF = nonEmptyT.getFeatureByBaseName(CAS.FEATURE_BASE_NAME_TAIL);

        FeatureStructure tail = view.createFS(emptyT);
        for (int i = xs.size() - 1; i >= 0; i--) {
            FeatureStructure cell = view.createFS(nonEmptyT);
            cell.setStringValue(headF, (String) xs.get(i));
            cell.setFeatureValue(tailF, tail);
            tail = cell;
        }
        return tail;
    }
}
