package com.mware.stage.origin.bigconnect.dataworker.dwsource;

import com.mware.stage.origin.bigconnect.dataworker.common.Groups;
import com.streamsets.pipeline.api.*;

@StageDef(
    version = 1,
    label = "DataWorker Starter",
    description = "Generic origin for DataWorker pipelines",
    icon = "bc.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    onlineHelpRefUrl = ""
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class DataWorkerDSource extends DataWorkerSource {
    @ConfigDef(
        required = true,
        type = ConfigDef.Type.STRING,
        defaultValue = "/opt/config",
        label = "Graph Engine Config Path",
        displayPosition = 10,
        group = "Connection"
    )
    public String configPath;

    @Override
    public String getConfigPath() {
        return configPath;
    }
}
