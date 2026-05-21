package org.texttechnologylab.duui.refactor.timelines;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

public enum DUUILifecycle {
    CREATION,
    INACTIVE,
    ACTIVE,
    TERMINAL;

    private static final Map<DUUILifecycle, Set<DUUILifecycle>> TRANSITIONS = new EnumMap<>(DUUILifecycle.class);

    static {
        TRANSITIONS.put(CREATION, EnumSet.of(INACTIVE, ACTIVE));
        TRANSITIONS.put(INACTIVE, EnumSet.of(CREATION, ACTIVE));
        TRANSITIONS.put(ACTIVE, EnumSet.of(INACTIVE, TERMINAL));
        TRANSITIONS.put(TERMINAL, EnumSet.noneOf(DUUILifecycle.class));
    }

    public boolean canTransitionTo(DUUILifecycle target) {
        return TRANSITIONS.get(this).contains(target);
    }

    public DUUILifecycle transitionTo(DUUILifecycle target) {
        if (!canTransitionTo(target)) {
            throw new IllegalStateException("Illegal DUUI lifecycle transition: " + this + " -> " + target);
        }
        return target;
    }
}
