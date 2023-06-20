package org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.uima.UIMAException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.CasCopier;

public abstract class DUUISegmentationStrategy implements IDUUISegmentationStrategy {
    // The current JCas to be processed, this should not be modified
    protected JCas jCasInput;

    // The final JCas where the generated annotations are merged into
    protected JCas jCasOutput;

    // Set the JCas to be processed, this should reset all state
    public void initialize(JCas jCas) throws UIMAException {
        this.jCasInput = jCas;
        this.initialize();
    }

    // Get final JCas with the generated annotations
    public void finalize(JCas jCas) {
        jCas.reset();

        CasCopier.copyCas(jCasOutput.getCas(), jCas.getCas(), true);

        // Metadata needs to be copied separately
        try {
            DocumentMetaData.copy(jCasOutput, jCas);
        }
        catch (Exception e) {
            // ignore
        }
    }

    // Get next segment of the JCas
    public abstract JCas getNextSegment();

    // Initialize the state of the segmentation strategy,
    // this is called automatically when a new JCas is set
    protected abstract void initialize() throws UIMAException;

    // Merge the segmented JCas back into the output JCas
    public abstract void merge(JCas jCasSegment);
}
