package org.texttechnologylab.duui.dua.inspect;

import java.util.Map;
import java.util.Objects;
import org.texttechnologylab.duui.dua.model.DUAFeatureKey;
import org.texttechnologylab.duui.dua.query.DUAQuery;

public record DUADataBinding(DUAQuery query, Map<String, DUAFeatureKey> featureMappings) {
    public DUADataBinding {
        Objects.requireNonNull(query, "query");
        featureMappings = featureMappings == null ? Map.of() : Map.copyOf(featureMappings);
    }
}
