package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import static java.lang.String.format;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.CommunicationLayerException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;

public interface IDUUIRestDriver extends IDUUIDriverInterface {
    int MAX_HTTP_RETRIES = 10;
    Duration RETRY_DELAY = Duration.ofMillis(2000);
    int BODY_PREVIEW_LIMIT = 500;

    /**
     * Method for defining the Lua context to be used, which determines the transfer type between Composer and components.
     * @see DUUILuaContext
     * @param luaContext
     */
    @Override
    public void setLuaContext(DUUILuaContext luaContext);
 
    static HttpResponse<byte[]> sendWithRetries(
            HttpClient client,
            HttpRequest request,
            Instant deadline,
            Duration timeout,
            String prefix) throws Exception {
        int attempts = 0;
        while (Instant.now().isBefore(deadline)) {
            try {
                HttpResponse<byte[]> resp = client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).join();
                System.out.printf(
                        "%s HTTP attempt #%d to %s succeeded with status %d%n",
                        prefix,
                        attempts + 1,
                        request.uri(),
                        resp.statusCode()
                );
                return resp;
            } catch (Exception e) {
                attempts++;
                System.err.printf(
                        "%s HTTP connection error on try #%d to %s: %s (%s)%n",
                        prefix,
                        attempts,
                        request.uri(),
                        e.getClass().getSimpleName(),
                        e.getMessage()
                );
                if (e instanceof CompletionException ce && ce.getCause() != null) {
                    System.err.printf(
                            "%s CompletionException cause: %s (%s)%n",
                            prefix,
                            ce.getCause().getClass().getSimpleName(),
                            ce.getCause().getMessage()
                    );
                }
                if (attempts >= MAX_HTTP_RETRIES) {
                    throw new CommunicationLayerException(format("%s The endpoint (%s) could not provide a response after #%d tries.",
                        prefix, request.uri(), attempts
                    ), e);
                }
                Thread.sleep(RETRY_DELAY.toMillis());
            }
        }
        throw new TimeoutException(
            format("%s The endpoint (%s) could not provide a response after %d milliseconds.",
            prefix, request.uri(), timeout.toMillis()
        ));
    }

    public static String preview(byte[] body, int maxLen, Charset charset) {
        if (body == null || body.length == 0 || maxLen <= 0) {
            return "";
        }
        String text = new String(body, charset);
        if (text.length() > maxLen) {
            return text.substring(0, maxLen) + "...";
        }
        return text;
    }


 
}
