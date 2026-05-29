package org.texttechnologylab.duui.dua.graph;

import org.texttechnologylab.duui.dua.graph.jsonl.DUAJsonlGraphCodec;
import org.texttechnologylab.duui.dua.graph.sqlite.DUASqliteGraphCodec;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public final class DUAGraphCodecs {
    private final Map<String, DUAGraphCodec> codecs = new LinkedHashMap<>();

    public static DUAGraphCodecs defaults() {
        return new DUAGraphCodecs()
                .register(new DUAJsonlGraphCodec())
                .register(new DUASqliteGraphCodec());
    }

    public DUAGraphCodecs register(DUAGraphCodec codec) {
        codecs.put(codec.id(), codec);
        return this;
    }

    public Optional<DUAGraphCodec> find(String id) {
        return Optional.ofNullable(codecs.get(id));
    }

    public DUAGraphCodec require(String id) {
        return find(id).orElseThrow(() -> new IllegalArgumentException("Unknown DUA graph codec: " + id));
    }
}
