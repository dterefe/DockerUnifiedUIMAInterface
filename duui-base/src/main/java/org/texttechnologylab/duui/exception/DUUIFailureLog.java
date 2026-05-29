package org.texttechnologylab.duui.exception;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class DUUIFailureLog {
    private final List<DUUIFailure> failures = new ArrayList<>();

    public void add(DUUIFailure failure) {
        if (failure != null) failures.add(failure);
    }

    public boolean hasFailures() { return !failures.isEmpty(); }
    public List<DUUIFailure> all() { return Collections.unmodifiableList(failures); }
}
