package org.texttechnologylab.duui.dua.inspect;

import java.util.Objects;
import java.util.Optional;
import org.texttechnologylab.duui.dua.model.DUAFeatureKey;

public record DUATimelineBinding(DUAFeatureKey start, Optional<DUAFeatureKey> end,
                                 DUAFeatureKey label, Optional<DUAFeatureKey> tooltip) {
    public DUATimelineBinding {
        Objects.requireNonNull(start, "start");
        end = end == null ? Optional.empty() : end;
        Objects.requireNonNull(label, "label");
        tooltip = tooltip == null ? Optional.empty() : tooltip;
    }
}
