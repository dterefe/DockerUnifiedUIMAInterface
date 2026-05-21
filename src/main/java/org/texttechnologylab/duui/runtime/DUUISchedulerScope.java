package org.texttechnologylab.duui.runtime;

import org.texttechnologylab.duui.orchestration.DUUIDirector;
import org.texttechnologylab.duui.orchestration.DUUIScheduler;
import org.texttechnologylab.duui.orchestration.DUUITypeDirector;

public final class DUUISchedulerScope implements AutoCloseable {
    private final DUUISystemScope system;
    private DUUIScheduler scheduler = new DUUIScheduler();
    private DUUIDirector director = new DUUITypeDirector();
    private boolean closed;

    DUUISchedulerScope(DUUISystemScope system) {
        this.system = system;
    }

    public DUUISchedulerScope sequential() {
        this.scheduler = new DUUIScheduler();
        return this;
    }

    public DUUISchedulerScope scheduler(DUUIScheduler scheduler) {
        this.scheduler = scheduler == null ? new DUUIScheduler() : scheduler;
        return this;
    }

    public DUUISchedulerScope director(DUUIDirector director) {
        this.director = director == null ? new DUUITypeDirector() : director;
        return this;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        system.scheduler(scheduler);
        system.director(director);
    }
}
