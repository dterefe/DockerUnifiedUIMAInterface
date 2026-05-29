package org.texttechnologylab.duui.dua.store;

public record DUARevision(long value) implements Comparable<DUARevision> {
    public static final DUARevision INITIAL = new DUARevision(0);

    public DUARevision next() {
        return new DUARevision(value + 1);
    }

    @Override
    public int compareTo(DUARevision other) {
        return Long.compare(value, other.value);
    }
}
