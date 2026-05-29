package org.texttechnologylab.duui.ems;

import org.texttechnologylab.duui.ems.traits.DUUITrait;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

public final class DUUITraits {
    private final Set<Class<? extends DUUITrait>> traits = new LinkedHashSet<>();

    public static DUUITraits empty() {
        return new DUUITraits();
    }

    @SafeVarargs
    public static DUUITraits of(Class<? extends DUUITrait>... traits) {
        DUUITraits result = new DUUITraits();
        if (traits != null) {
            for (Class<? extends DUUITrait> trait : traits) {
                result.add(trait);
            }
        }
        return result;
    }

    public DUUITraits add(Class<? extends DUUITrait> trait) {
        traits.add(Objects.requireNonNull(trait, "trait"));
        return this;
    }

    public DUUITraits remove(Class<? extends DUUITrait> trait) {
        traits.remove(trait);
        return this;
    }

    public boolean has(Class<? extends DUUITrait> trait) {
        return traits.contains(trait);
    }

    public Set<Class<? extends DUUITrait>> values() {
        return Collections.unmodifiableSet(traits);
    }
}
