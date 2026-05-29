package org.texttechnologylab.duui.dua.store;

import java.util.stream.Stream;
import org.texttechnologylab.duui.dua.graph.DUAGraphEdge;
import org.texttechnologylab.duui.dua.graph.DUAGraphNode;
import org.texttechnologylab.duui.dua.model.DUAAssociation;
import org.texttechnologylab.duui.dua.model.DUAEntity;
import org.texttechnologylab.duui.dua.model.DUAEntityRef;

public interface DUAGraphNavigationStore {
    DUAWriteResult indexEntity(DUAEntity entity);

    DUAWriteResult indexAssociation(DUAAssociation association);

    Stream<DUAGraphNode> nodes(String label);

    Stream<DUAGraphEdge> edges( DUAEntityRef<? extends DUAEntity> source, String label);
}
