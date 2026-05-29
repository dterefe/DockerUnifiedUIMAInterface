package org.texttechnologylab.duui.dua.transport;

import java.nio.file.Path;
import java.util.List;
import org.texttechnologylab.duui.dua.graph.DUAGraphCodec;

public interface DUATransportService {
    DUATransportJob exportTransfer(Path target, List<DUAJCasTransferDocument> documents);

    DUATransportJob createMembershipPatch(Path target, List<DUAMembershipPatchDocument> documents);

    DUATransportJob importTransferToArchive(Path transfer,
                                            Path targetArchive,
                                            String universeId,
                                            String corpusId,
                                            DUAGraphCodec codec);

    DUATransportJob exportBareXmi(Path transfer, String documentId, Path target);

    DUATransportJob importBareXmiToArchive(Path xmi,
                                           Path targetArchive,
                                           String universeId,
                                           String corpusId,
                                           String documentId,
                                           DUAGraphCodec codec);

    DUATransportJob job(String jobId);
}
