package org.texttechnologylab.duui.filesystem;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.texttechnologylab.duui.clients.handle.DUUIAddress;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DUUILocalFSClientTest {
    @TempDir
    Path temp;

    @Test
    void documentClientReadsWritesAndListsThroughTypedProxies() throws Exception {
        DUUILocalFSClient client = new DUUILocalFSClient(temp);
        DUUIAddress fileAddress = DUUIAddress.parse(temp.resolve("corpus/doc.txt").toUri().toString());

        DUUIFile file = client.write(fileAddress, new ByteArrayInputStream("DUA".getBytes(StandardCharsets.UTF_8)));

        assertTrue(file.exists());
        assertEquals("doc.txt", file.name());
        try (var streams = client.read(file).stream()) {
            String text = new String(streams.findFirst().orElseThrow().readAllBytes(), StandardCharsets.UTF_8);
            assertEquals("DUA", text);
        }

        List<String> names;
        try (var objects = client.list(client.directory(DUUIAddress.parse(temp.resolve("corpus").toUri().toString())))) {
            names = objects.map(DUUIFileSystemObject::name).sorted().toList();
        }
        assertEquals(List.of("doc.txt"), names);
    }
}
