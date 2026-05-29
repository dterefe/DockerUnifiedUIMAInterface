package org.texttechnologylab.duui.dua.query;

public interface DUAQueryCompiler<T> {
    T compile(DUAQuery query);
}
