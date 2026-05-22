package org.texttechnologylab.duui.artifact;

import org.texttechnologylab.duui.ems.DUUIActor;
import org.texttechnologylab.duui.ems.DUUISubject;
import org.texttechnologylab.duui.ems.DUUITraits;
import org.texttechnologylab.duui.ems.GID;

import java.util.Objects;

public final class DUUIArtifact<T> implements DUUISubject, DUUIActor {
    private final GID gid;
    private final T subject;
    private final DUUITraits traits;

    private DUUIArtifact(GID gid, T subject, DUUITraits traits) {
        this.gid = Objects.requireNonNull(gid, "gid");
        this.subject = Objects.requireNonNull(subject, "subject");
        this.traits = traits == null ? DUUITraits.empty() : traits;
    }

    public static <T> DUUIArtifact<T> of(T subject) {
        return new DUUIArtifact<>(GID.create(), subject, DUUITraits.empty());
    }

    public static <T> DUUIArtifact<T> of(T subject, DUUITraits traits) {
        return new DUUIArtifact<>(GID.create(), subject, traits);
    }

    public DUUIArtifact<T> withTraits(DUUITraits traits) {
        return new DUUIArtifact<>(gid, subject, traits);
    }

    @Override
    public GID gid() {
        return gid;
    }

    @Override
    public DUUITraits traits() {
        return traits;
    }

    public T subject() {
        return subject;
    }

    public T payload() {
        return subject;
    }
}
