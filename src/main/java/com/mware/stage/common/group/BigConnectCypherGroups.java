package com.mware.stage.common.group;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum BigConnectCypherGroups implements Label {
    CYPHER("Cypher Query"),
    CONNECTION("Connection"),
    ;

    private final String label;

    private BigConnectCypherGroups(String label) {
        this.label = label;
    }

    @Override
    public String getLabel() {
        return this.label;
    }
}
