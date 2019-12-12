package com.mware.stage.lib;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum PythonExecutorOutputStreams  implements Label {
    COMMIT("Commit"),
    PROCESS("Process");

    private final String label;

    PythonExecutorOutputStreams(String label) {
        this.label = label;
    }

    @Override
    public String getLabel() {
        return label;
    }
}
