package org.texttechnologylab.duui.rework;

import org.junit.jupiter.api.Test;
import org.texttechnologylab.duui.clients.http.DUUIAsyncBodyHandler;
import org.texttechnologylab.duui.clients.http.DUUIBodyHandler;
import org.texttechnologylab.duui.clients.http.DUUIHttpResponse;
import org.texttechnologylab.duui.clients.http.DUUIRelay;
import org.texttechnologylab.duui.clients.http.DUUIStreamBodyHandler;
import org.texttechnologylab.duui.filesystem.DUUIStream;

import java.io.EOFException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DUUIBodyHandlerGreedyTest {
    @Test
    void bodyHandlerRunsDeserializerAfterHttpCompletion() throws Exception {
        CountDownLatch decoderStarted = new CountDownLatch(1);

        DUUIBodyHandler<String> handler = new DUUIBodyHandler<>(input -> {
            decoderStarted.countDown();
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        });

        HttpResponse.BodySubscriber<String> subscriber = handler.apply(null);
        subscriber.onSubscribe(noopSubscription());

        subscriber.onNext(List.of(ByteBuffer.wrap(new byte[] {'A'})));

        assertEquals(1, decoderStarted.getCount(), "synchronous body handler must buffer until HTTP completion");

        subscriber.onComplete();

        assertEquals("A", subscriber.getBody().toCompletableFuture().get(1, TimeUnit.SECONDS));
        assertEquals(0, decoderStarted.getCount(), "deserializer should run once the response is complete");
    }

    @Test
    void streamBodyHandlerDeserializesEachChunkAsItArrives() throws Exception {
        int annotations = 160;
        CountDownLatch firstChunkDecoded = new CountDownLatch(1);
        DUUIStreamBodyHandler<Integer> handler = new DUUIStreamBodyHandler<>(input -> {
            int value = decodeAnnotationFrame(input);
            if (value == 0) {
                firstChunkDecoded.countDown();
            }
            return value;
        });
        HttpResponse.BodySubscriber<DUUIStream<Integer>> subscriber = handler.apply(null);
        subscriber.onSubscribe(noopSubscription());
        DUUIStream<Integer> stream = subscriber.getBody().toCompletableFuture().get(1, TimeUnit.SECONDS);
        List<Integer> decoded = new ArrayList<>();
        stream.sink(decoded::add);

        for (int i = 0; i < annotations; i++) {
            byte[] payload = ("annotation-" + i).getBytes(StandardCharsets.UTF_8);
            ByteBuffer frame = ByteBuffer.allocate(Integer.BYTES + payload.length);
            frame.putInt(payload.length);
            frame.put(payload);
            subscriber.onNext(List.of(ByteBuffer.wrap(frame.array())));
        }

        assertTrue(firstChunkDecoded.await(1, TimeUnit.SECONDS), "stream body handler should deserialize chunks before completion");
        assertEquals(annotations, decoded.size());
        assertEquals(0, decoded.get(0));
        assertEquals(annotations - 1, decoded.get(annotations - 1));
    }

    @Test
    void asyncBodyHandlerWritesChunksToRelayAndReturnsHttpResponseMetadata() throws Exception {
        DUUIRelay<String> relay = new DUUIRelay<>();
        HttpResponse.BodySubscriber<DUUIHttpResponse> subscriber = new DUUIAsyncBodyHandler(relay).apply(responseInfo(201));
        subscriber.onSubscribe(noopSubscription());

        CountDownLatch responseChunkRead = new CountDownLatch(1);
        Thread reader = new Thread(() -> {
            try {
                byte[] bytes = readFullyOrEof(relay.inputStream(), 1);
                if (bytes != null && bytes[0] == 'x') {
                    responseChunkRead.countDown();
                }
            } catch (Exception ignored) {
            }
        });
        reader.start();

        subscriber.onNext(List.of(ByteBuffer.wrap(new byte[] {'x'})));

        assertTrue(responseChunkRead.await(1, TimeUnit.SECONDS), "async body handler should relay chunks before completion");

        subscriber.onComplete();
        DUUIHttpResponse response = subscriber.getBody().toCompletableFuture().get(1, TimeUnit.SECONDS);
        reader.join(1000);

        assertEquals(201, response.statusCode());
        assertEquals(1, response.bodyBytes());
    }

    private static int decodeAnnotationFrame(InputStream input) throws Exception {
        byte[] header = readFullyOrEof(input, Integer.BYTES);
        if (header == null) {
            throw new EOFException("missing annotation header");
        }
        int length = ByteBuffer.wrap(header).getInt();
        byte[] payload = readFullyOrEof(input, length);
        if (payload == null) {
            throw new EOFException("truncated annotation payload");
        }
        String text = new String(payload, StandardCharsets.UTF_8);
        return Integer.parseInt(text.substring("annotation-".length()));
    }

    private static byte[] readFullyOrEof(InputStream input, int length) throws Exception {
        byte[] buffer = new byte[length];
        int offset = 0;
        while (offset < length) {
            int read = input.read(buffer, offset, length - offset);
            if (read < 0) {
                if (offset == 0) {
                    return null;
                }
                throw new EOFException("truncated frame");
            }
            offset += read;
        }
        return buffer;
    }

    private static Flow.Subscription noopSubscription() {
        return new Flow.Subscription() {
            @Override
            public void request(long n) {
            }

            @Override
            public void cancel() {
            }
        };
    }

    private static HttpResponse.ResponseInfo responseInfo(int statusCode) {
        return new HttpResponse.ResponseInfo() {
            @Override
            public int statusCode() {
                return statusCode;
            }

            @Override
            public HttpHeaders headers() {
                return HttpHeaders.of(Map.of(), (name, value) -> true);
            }

            @Override
            public HttpClient.Version version() {
                return HttpClient.Version.HTTP_1_1;
            }
        };
    }
}
