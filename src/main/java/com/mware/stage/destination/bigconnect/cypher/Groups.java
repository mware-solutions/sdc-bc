package com.mware.stage.destination.bigconnect.cypher;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum Groups implements Label {
    CYPHER("Cypher Query"),
    CONNECTION("Connection"),
    ;

    private final String label;

    private Groups(String label) {
        this.label = label;
    }

    @Override
    public String getLabel() {
        return this.label;
    }
}
