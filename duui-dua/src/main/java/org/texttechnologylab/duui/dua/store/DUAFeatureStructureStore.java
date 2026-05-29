package org.texttechnologylab.duui.dua.store;

import java.util.Optional;
import java.util.stream.Stream;
import org.texttechnologylab.duui.dua.DUAId;
import org.texttechnologylab.duui.dua.model.DUAFeatureKey;
import org.texttechnologylab.duui.dua.model.DUAFeatureStructure;
import org.texttechnologylab.duui.dua.model.DUAValue;
import org.texttechnologylab.duui.dua.query.DUAQuery;

public interface DUAFeatureStructureStore {
    Optional<DUAFeatureStructure> read(DUAId id);

    DUAWriteResult write(DUAFeatureStructure featureStructure);

    DUAWriteResult writeFeature(DUAId featureStructureId, DUAFeatureKey feature, DUAValue value);

    Stream<DUAFeatureStructure> find(DUAQuery query);
}
