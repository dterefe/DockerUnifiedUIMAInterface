package org.texttechnologylab.duui.rework;

import org.junit.jupiter.api.Test;
import org.texttechnologylab.duui.clients.http.DUUIBodyHandler;
import org.texttechnologylab.duui.clients.http.DUUIRelay;
import org.texttechnologylab.duui.event.DUUIEvent;
import org.texttechnologylab.duui.event.DUUIEventLevel;
import org.texttechnologylab.duui.event.DUUIEventService;
import org.texttechnologylab.duui.event.DUUIInMemoryEventSink;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DUUIBodyHandlerGreedyTest {
    @Test
    void decoderReadsResponseChunkBeforeHttpCompletion() throws Exception {
        DUUIRelay<String> relay = new DUUIRelay<>();
        CountDownLatch decoderStarted = new CountDownLatch(1);
        CountDownLatch firstByteRead = new CountDownLatch(1);
        CountDownLatch allowDecoderReturn = new CountDownLatch(1);
        CountDownLatch httpCompleted = new CountDownLatch(1);

        DUUIBodyHandler<String> handler = new DUUIBodyHandler<>(
                relay,
                input -> {
                    decoderStarted.countDown();
                    int value = input.read();
                    if (value >= 0) {
                        firstByteRead.countDown();
                    }
                    allowDecoderReturn.await(2, TimeUnit.SECONDS);
                    return Character.toString((char) value);
                },
                DUUIEventService.global()
        );

        HttpResponse.BodySubscriber<String> subscriber = handler.apply(null);
        subscriber.onSubscribe(new Flow.Subscription() {
            @Override
            public void request(long n) {
            }

            @Override
            public void cancel() {
            }
        });

        assertTrue(decoderStarted.await(1, TimeUnit.SECONDS), "decoder did not start on subscribe");

        subscriber.onNext(List.of(ByteBuffer.wrap(new byte[] {'A'})));

        assertTrue(firstByteRead.await(1, TimeUnit.SECONDS), "decoder did not greedily read first response chunk");
        assertFalse(relay.future().isDone(), "decoder should still be blocked by the test gate");
        assertEquals(1, httpCompleted.getCount(), "test must prove read happened before onComplete");

        subscriber.onComplete();
        httpCompleted.countDown();
        allowDecoderReturn.countDown();

        assertEquals("A", subscriber.getBody().toCompletableFuture().get(1, TimeUnit.SECONDS));
    }

    @Test
    void streamingDeserializationOverlapsChunkArrivalAsAnnotationCountGrows() throws Exception {
        int annotations = 160;
        int producerDelayMs = 2;
        int decodeDelayMs = 2;

        long bufferedStarted = System.nanoTime();
        ByteArrayOutputStream buffered = new ByteArrayOutputStream();
        for (int i = 0; i < annotations; i++) {
            sleep(producerDelayMs);
            buffered.write(annotationFrame(i));
        }
        int bufferedCount = decodeAnnotationFrames(new ByteArrayInputStream(buffered.toByteArray()), decodeDelayMs);
        long bufferedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - bufferedStarted);

        DUUIRelay<Integer> relay = new DUUIRelay<>();
        DUUIBodyHandler<Integer> handler = new DUUIBodyHandler<>(
                relay,
                input -> decodeAnnotationFrames(input, decodeDelayMs),
                DUUIEventService.global()
        );
        HttpResponse.BodySubscriber<Integer> subscriber = handler.apply(null);
        subscriber.onSubscribe(new Flow.Subscription() {
            @Override
            public void request(long n) {
            }

            @Override
            public void cancel() {
            }
        });

        long streamingStarted = System.nanoTime();
        for (int i = 0; i < annotations; i++) {
            sleep(producerDelayMs);
            subscriber.onNext(List.of(ByteBuffer.wrap(annotationFrame(i))));
        }
        subscriber.onComplete();
        int streamingCount = subscriber.getBody().toCompletableFuture().get(5, TimeUnit.SECONDS);
        long streamingMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - streamingStarted);

        assertEquals(annotations, bufferedCount);
        assertEquals(annotations, streamingCount);
        System.out.printf(
                "DUUI greedy deserialization scale annotations=%d buffered_ms=%d streaming_ms=%d%n",
                annotations,
                bufferedMs,
                streamingMs
        );
        assertTrue(
                streamingMs < bufferedMs * 0.85,
                "expected greedy deserialization to overlap chunk arrival as annotations grow; buffered_ms="
                        + bufferedMs + " streaming_ms=" + streamingMs
        );
    }

    @Test
    void bodyHandlerEmitsSpecificStandardLogLevels() throws Exception {
        DUUIInMemoryEventSink sink = new DUUIInMemoryEventSink();
        DUUIEventService events = new DUUIEventService(List.of(sink));

        DUUIRelay<String> normalRelay = new DUUIRelay<>();
        HttpResponse.BodySubscriber<String> normal = new DUUIBodyHandler<>(
                normalRelay,
                input -> {
                    input.read();
                    return "ok";
                },
                events
        ).apply(responseInfo(200));
        normal.onSubscribe(noopSubscription());
        normal.onNext(List.of(ByteBuffer.wrap(new byte[] {'x'})));
        normal.onComplete();
        normal.getBody().toCompletableFuture().get(1, TimeUnit.SECONDS);

        DUUIRelay<String> earlyDoneRelay = new DUUIRelay<>();
        HttpResponse.BodySubscriber<String> earlyDone = new DUUIBodyHandler<>(
                earlyDoneRelay,
                input -> "done-before-complete",
                events
        ).apply(responseInfo(200));
        earlyDone.onSubscribe(noopSubscription());
        earlyDone.getBody().toCompletableFuture().get(1, TimeUnit.SECONDS);
        earlyDone.onNext(List.of(ByteBuffer.wrap(new byte[] {'y'})));
        sleep(50);
        earlyDone.onComplete();

        DUUIRelay<String> emptyRelay = new DUUIRelay<>();
        HttpResponse.BodySubscriber<String> empty = new DUUIBodyHandler<>(
                emptyRelay,
                input -> "empty",
                events
        ).apply(responseInfo(200));
        empty.onSubscribe(noopSubscription());
        empty.getBody().toCompletableFuture().get(1, TimeUnit.SECONDS);
        empty.onComplete();

        new DUUIBodyHandler<>(new DUUIRelay<>(), input -> "redirect", events).apply(responseInfo(302));
        new DUUIBodyHandler<>(new DUUIRelay<>(), input -> "client-error", events).apply(responseInfo(404));
        new DUUIBodyHandler<>(new DUUIRelay<>(), input -> "server-error", events).apply(responseInfo(500));

        Set<DUUIEventLevel> levels = sink.events().stream()
                .map(DUUIEvent::level)
                .filter(level -> level != null)
                .collect(Collectors.toCollection(() -> EnumSet.noneOf(DUUIEventLevel.class)));

        assertTrue(levels.containsAll(EnumSet.of(
                DUUIEventLevel.TRACE,
                DUUIEventLevel.DEBUG,
                DUUIEventLevel.INFO,
                DUUIEventLevel.WARNING,
                DUUIEventLevel.WARN,
                DUUIEventLevel.ERROR,
                DUUIEventLevel.CRITICAL
        )), "missing log levels from body handler events: " + levels);
    }

    private static byte[] annotationFrame(int index) {
        byte[] payload = ("annotation-" + index).getBytes(StandardCharsets.UTF_8);
        ByteBuffer frame = ByteBuffer.allocate(Integer.BYTES + payload.length);
        frame.putInt(payload.length);
        frame.put(payload);
        return frame.array();
    }

    private static int decodeAnnotationFrames(InputStream input, int decodeDelayMs) throws Exception {
        int count = 0;
        while (true) {
            byte[] header = readFullyOrEof(input, Integer.BYTES);
            if (header == null) {
                return count;
            }
            int length = ByteBuffer.wrap(header).getInt();
            if (length < 0) {
                throw new IllegalArgumentException("negative annotation payload length");
            }
            byte[] payload = readFullyOrEof(input, length);
            if (payload == null) {
                throw new EOFException("truncated annotation payload");
            }
            sleep(decodeDelayMs);
            count++;
        }
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

    private static void sleep(int millis) throws InterruptedException {
        Thread.sleep(millis);
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
