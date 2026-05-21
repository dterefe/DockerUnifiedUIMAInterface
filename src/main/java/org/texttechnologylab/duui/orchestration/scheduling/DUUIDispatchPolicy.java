package org.texttechnologylab.duui.orchestration.scheduling;

public final class DUUIDispatchPolicy {
    public static final DUUIDispatchPolicy INHERIT = new DUUIDispatchPolicy(null, null, false);
    public static final DUUIDispatchPolicy CALLER = new DUUIDispatchPolicy(null, null, true);

    private final DUUIDispatchMode mode;
    private final Integer parallelism;
    private final boolean caller;

    public DUUIDispatchPolicy(DUUIDispatchMode mode, Integer parallelism, boolean caller) {
        this.mode = mode;
        this.parallelism = parallelism;
        this.caller = caller;
    }

    public static DUUIDispatchPolicy io() { return new DUUIDispatchPolicy(DUUIDispatchMode.IO, null, false); }
    public static DUUIDispatchPolicy cpu() { return new DUUIDispatchPolicy(DUUIDispatchMode.CPU, null, false); }
    public static DUUIDispatchPolicy mixed() { return new DUUIDispatchPolicy(DUUIDispatchMode.MIXED, null, false); }
    public static DUUIDispatchPolicy of(DUUIDispatchMode mode, Integer parallelism) { return new DUUIDispatchPolicy(mode, parallelism, false); }

    public DUUIDispatchPolicy merge(DUUIDispatchPolicy override) {
        if (override == null) return this;
        return new DUUIDispatchPolicy(
                override.mode != null ? override.mode : mode,
                override.parallelism != null ? override.parallelism : parallelism,
                override.caller || caller
        );
    }

    public DUUIDispatchMode mode() { return mode; }
    public Integer parallelism() { return parallelism; }
    public boolean caller() { return caller; }
}
