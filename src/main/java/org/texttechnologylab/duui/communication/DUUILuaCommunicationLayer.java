package org.texttechnologylab.duui.communication;

import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LoadState;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.compiler.LuaC;
import org.luaj.vm2.lib.Bit32Lib;
import org.luaj.vm2.lib.CoroutineLib;
import org.luaj.vm2.lib.PackageLib;
import org.luaj.vm2.lib.StringLib;
import org.luaj.vm2.lib.TableLib;
import org.luaj.vm2.lib.jse.CoerceJavaToLua;
import org.luaj.vm2.lib.jse.JseBaseLib;
import org.luaj.vm2.lib.jse.JseIoLib;
import org.luaj.vm2.lib.jse.JseMathLib;
import org.luaj.vm2.lib.jse.JseOsLib;
import org.luaj.vm2.lib.jse.LuajavaLib;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.msgpack.core.MessagePack;
import org.apache.uima.fit.util.JCasUtil;

public final class DUUILuaCommunicationLayer implements DUUICommunicationLayer {
    private final String script;
    private final Globals globals;

    public DUUILuaCommunicationLayer(String script) throws IOException {
        this.script = script;
        this.globals = new Globals();
        globals.load(new JseBaseLib());
        globals.load(new PackageLib());
        globals.load(new Bit32Lib());
        globals.load(new TableLib());
        globals.load(new StringLib());
        globals.load(new JseMathLib());
        globals.load(new CoroutineLib());
        globals.load(new JseIoLib());
        globals.load(new JseOsLib());
        globals.load(new LuajavaLib());
        LoadState.install(globals);
        LuaC.install(globals);

        String jsonModule;
        InputStream jsonStream = DUUILuaCommunicationLayer.class.getResourceAsStream("/lua_stdlib/json.lua");
        if (jsonStream == null) {
            jsonStream = DUUILuaCommunicationLayer.class.getResourceAsStream(
                    "/org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua");
        }
        try (InputStream in = jsonStream) {
            if (in == null) {
                throw new IOException("missing resource /lua_stdlib/json.lua");
            }
            jsonModule = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }

        LuaValue jsonChunk = globals.load(jsonModule, "json", globals);
        globals.set("json", jsonChunk.call());
        globals.set("MessagePack", CoerceJavaToLua.coerce(MessagePack.class));
        globals.set("JCasUtil", CoerceJavaToLua.coerce(JCasUtil.class));
        globals.set("DUUIBytes", CoerceJavaToLua.coerce(DUUIBytes.class));

        LuaValue mainChunk = globals.load(script, "duui_py_comm_layer", globals);
        mainChunk.call();
    }

    @Override
    public void serialize(JCas sourceCas, OutputStream output, Map<String, String> parameters, String sourceView)
        throws CASException {
        JCas view = sourceCas.getView(sourceView);
        LuaTable params = new LuaTable();
        if (parameters != null) {
            for (Map.Entry<String, String> e : parameters.entrySet()) {
                params.set(e.getKey(), e.getValue());
            }
        }
        globals.get("serialize").invoke(
            LuaValue.varargsOf(
                new LuaValue[] {
                    CoerceJavaToLua.coerce(view),
                    CoerceJavaToLua.coerce(output),
                    CoerceJavaToLua.coerce(params),
                    CoerceJavaToLua.coerce(sourceView)
                }
            )
        );
    }

    @Override
    public void deserialize(JCas targetCas, InputStream input, String targetView) throws CASException {
        JCas view;
        try {
            view = targetCas.getView(targetView);
        } catch (Exception e) {
            view = targetCas.createView(targetView);
        }
        globals.get("deserialize").invoke(
            LuaValue.varargsOf(
                new LuaValue[] {
                    CoerceJavaToLua.coerce(view),
                    CoerceJavaToLua.coerce(input)
                }
            )
        );
    }

    @Override
    public DUUICommunicationLayer copy() throws Exception {
        return new DUUILuaCommunicationLayer(script);
    }

    public String script() {
        return script;
    }
}
