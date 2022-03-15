import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.TypeSystemUtil;
import org.json.JSONObject;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;

public class DUUIRemoteDriver implements IDUUIDriverInterface {
    private HashMap<String,InstantiatedComponent> _components;
    private OkHttpClient _client;


    public static class Component extends IDUUIPipelineComponent {
        Component(String url) {
            setOption("url",url);
        }
    }

    private static class InstantiatedComponent extends IDUUIPipelineComponent {
        private String _url;
        InstantiatedComponent(IDUUIPipelineComponent comp) {
            _url = comp.getOption("url");
            if(_url == null) {
                throw new InvalidParameterException("Missing parameter URL in the pipeline component descriptor");
            }
        }

        public String getUrl() {
            return _url;
        }
    }
    DUUIRemoteDriver() {
        _components = new HashMap<String,InstantiatedComponent>();
        _client = new OkHttpClient();
    }

    public boolean canAccept(IDUUIPipelineComponent component) {
        return component.getClass().getCanonicalName() == component.getClass().getCanonicalName();
    }

    public String instantiate(IDUUIPipelineComponent component) throws InterruptedException, TimeoutException, UIMAException, SAXException {
        String uuid = UUID.randomUUID().toString();
        while(_components.containsKey(uuid)) {
            uuid = UUID.randomUUID().toString();
        }
        InstantiatedComponent comp = new InstantiatedComponent(component);
        JCas _basic = JCasFactory.createJCas();
        ByteArrayOutputStream arr = new ByteArrayOutputStream();
        XmiCasSerializer.serialize(_basic.getCas(), _basic.getTypeSystem(), arr);
        String cas = arr.toString();
        System.out.printf("[RemoteDriver] Assigned new pipeline component unique id %s\n",uuid);

        JSONObject obj = new JSONObject();
        obj.put("cas",cas);
        obj.put("typesystem","");
        String ok = obj.toString();
        RequestBody bod = RequestBody.create(obj.toString().getBytes(StandardCharsets.UTF_8));

        if(DUUILocalDriver.responsiveAfterTime(comp.getUrl(),bod,10000,_client)) {
            _components.put(uuid,comp);
            System.out.printf("[RemoteDriver][%s] Remote URL %s is online and seems to understand DUUI V1 format!\n", uuid,comp.getUrl());
        }
        else {
            throw new TimeoutException(format("The URL %s did not provide one succesful answer! Aborting...", comp.getUrl()));
        }

        return uuid;
    }

    public DUUIEither run(String uuid, DUUIEither aCas) throws InterruptedException, IOException, SAXException {
        InstantiatedComponent comp = _components.get(uuid);
        if(comp == null) {
            throw new InvalidParameterException("The given instantiated component uuid was not instantiated by the remote driver");
        }
        String cas = aCas.getAsString();
        JSONObject obj = new JSONObject();
        obj.put("cas", cas);
        obj.put("typesystem", "");
        String ok = obj.toString();

        RequestBody bod = RequestBody.create(ok.getBytes(StandardCharsets.UTF_8));

        Request request = new Request.Builder()
                .url(comp.getUrl()+"/v1/process")
                .post(bod)
                .header("Content-Length",String.valueOf(ok.length()))
                .build();
        Response resp = _client.newCall(request).execute();
        if (resp.code() == 200) {
            String body = new String(resp.body().bytes(), Charset.defaultCharset());
            JSONObject response = new JSONObject(body);
            if (response.has("cas") || response.has("error")) {
                aCas.updateStringBuffer(response.getString("cas"));
                return aCas;
            } else {
                System.out.println("The response is not in the expected format!");
                throw new InvalidObjectException("Response is not in the right format!");
            }
        }
        else {
            throw new InvalidObjectException("Response code != 200, error");
        }
    }

    public void destroy(String uuid) {
        _components.remove(uuid);
    }
}