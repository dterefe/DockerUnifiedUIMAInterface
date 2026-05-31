package org.texttechnologylab.duui.dua.cas;

import org.apache.uima.cas.Feature;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.TypeSystem;
import org.apache.uima.jcas.JCas;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.IntSupplier;

final class DUAXmiFsIdRemapper {
    private static final String XMI_NS = "http://www.omg.org/XMI";

    byte[] remap(byte[] xmi, JCas sourceView, IntSupplier fsIds) throws Exception {
        Objects.requireNonNull(xmi, "xmi");
        Objects.requireNonNull(sourceView, "sourceView");
        Objects.requireNonNull(fsIds, "fsIds");

        Document document = parse(xmi);
        Map<Integer, Integer> remappedIds = collectIds(document, fsIds);
        if (remappedIds.isEmpty()) {
            return xmi;
        }
        ReferenceFeatures referenceFeatures = ReferenceFeatures.from(sourceView.getTypeSystem());
        rewrite(document.getDocumentElement(), remappedIds, referenceFeatures);
        return serialize(document);
    }

    private static Document parse(byte[] xmi) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        return factory.newDocumentBuilder().parse(new ByteArrayInputStream(xmi));
    }

    private static Map<Integer, Integer> collectIds(Document document, IntSupplier fsIds) {
        Map<Integer, Integer> remappedIds = new HashMap<>();
        visit(document.getDocumentElement(), element -> {
            String id = xmiId(element);
            if (id != null) {
                parseInt(id, -1);
                remappedIds.put(Integer.parseInt(id), fsIds.getAsInt());
            }
        });
        return remappedIds;
    }

    private static void rewrite(Element root, Map<Integer, Integer> remappedIds, ReferenceFeatures referenceFeatures) {
        visit(root, element -> {
            NamedNodeMap attributes = element.getAttributes();
            for (int i = 0; i < attributes.getLength(); i++) {
                Node attribute = attributes.item(i);
                if (isXmiId(attribute)) {
                    attribute.setNodeValue(remapValue(attribute.getNodeValue(), remappedIds));
                } else if (isReferenceAttribute(element, attribute, referenceFeatures)) {
                    attribute.setNodeValue(remapValues(attribute.getNodeValue(), remappedIds));
                }
            }
        });
    }

    private static boolean isReferenceAttribute(Element element, Node attribute, ReferenceFeatures referenceFeatures) {
        String attributeName = localName(attribute);
        String elementName = localName(element);
        if (attributeName.startsWith("_ref_")) {
            return true;
        }
        if ("View".equals(elementName) && ("sofa".equals(attributeName) || "members".equals(attributeName))) {
            return true;
        }
        if ("FSArray".equals(elementName) && "elements".equals(attributeName)) {
            return true;
        }
        if ("NonEmptyFSList".equals(elementName) && ("head".equals(attributeName) || "tail".equals(attributeName))) {
            return true;
        }
        return referenceFeatures.isReferenceFeature(elementName, attributeName);
    }

    private static byte[] serialize(Document document) throws Exception {
        TransformerFactory factory = TransformerFactory.newInstance();
        factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        var transformer = factory.newTransformer();
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
        transformer.setOutputProperty(OutputKeys.ENCODING, StandardCharsets.UTF_8.name());
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            transformer.transform(new DOMSource(document), new StreamResult(output));
            return output.toByteArray();
        }
    }

    private static String remapValues(String value, Map<Integer, Integer> remappedIds) {
        String stripped = value.strip();
        if (stripped.isEmpty()) {
            return value;
        }
        String[] tokens = stripped.split("\\s+");
        StringBuilder result = new StringBuilder();
        for (String token : tokens) {
            if (!result.isEmpty()) {
                result.append(' ');
            }
            result.append(remapValue(token, remappedIds));
        }
        return result.toString();
    }

    private static String remapValue(String value, Map<Integer, Integer> remappedIds) {
        int id = parseInt(value, -1);
        Integer remapped = remappedIds.get(id);
        return remapped == null ? value : Integer.toString(remapped);
    }

    private static String xmiId(Element element) {
        if (element.hasAttributeNS(XMI_NS, "id")) {
            return element.getAttributeNS(XMI_NS, "id");
        }
        return element.hasAttribute("xmi:id") ? element.getAttribute("xmi:id") : null;
    }

    private static boolean isXmiId(Node attribute) {
        return "id".equals(attribute.getLocalName()) && XMI_NS.equals(attribute.getNamespaceURI())
                || "xmi:id".equals(attribute.getNodeName());
    }

    private static String localName(Node node) {
        String localName = node.getLocalName();
        return localName == null ? node.getNodeName() : localName;
    }

    private static int parseInt(String value, int fallback) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    private static void visit(Element root, ElementVisitor visitor) {
        visitor.accept(root);
        Node child = root.getFirstChild();
        while (child != null) {
            if (child instanceof Element element) {
                visit(element, visitor);
            }
            child = child.getNextSibling();
        }
    }

    private interface ElementVisitor {
        void accept(Element element);
    }

    private record ReferenceFeatures(Map<String, Set<String>> featuresByType) {
        private static ReferenceFeatures from(TypeSystem typeSystem) {
            Map<String, Set<String>> featuresByType = new HashMap<>();
            for (Iterator<Type> types = typeSystem.getTypeIterator(); types.hasNext(); ) {
                Type type = types.next();
                Set<String> referenceFeatures = new HashSet<>();
                for (Feature feature : type.getFeatures()) {
                    Type range = feature.getRange();
                    if (range != null && !range.isPrimitive()) {
                        referenceFeatures.add(feature.getShortName());
                    }
                }
                if (!referenceFeatures.isEmpty()) {
                    featuresByType.put(type.getShortName(), referenceFeatures);
                }
            }
            return new ReferenceFeatures(featuresByType);
        }

        private boolean isReferenceFeature(String typeShortName, String featureShortName) {
            return featuresByType.getOrDefault(typeShortName, Set.of()).contains(featureShortName);
        }
    }
}
