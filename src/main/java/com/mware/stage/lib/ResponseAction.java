package com.mware.stage.lib;

import com.streamsets.pipeline.api.StageException;

public interface ResponseAction {
    void execute(int index, String responseLine) throws StageException;
}
