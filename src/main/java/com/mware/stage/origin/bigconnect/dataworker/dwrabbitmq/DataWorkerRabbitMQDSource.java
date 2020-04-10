package com.mware.stage.origin.bigconnect.dataworker.dwrabbitmq;

import com.mware.stage.origin.bigconnect.dataworker.common.Groups;
import com.streamsets.pipeline.api.*;

import java.util.List;

@StageDef(
    version = 1,
    label = "DataWorker Message Origin",
    description = "Consumes BigConnect DataWorker messages",
    icon = "bc.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    onlineHelpRefUrl = ""
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class DataWorkerRabbitMQDSource extends DataWorkerRabbitMQSource {
    @ConfigDef(
        required = true,
        type = ConfigDef.Type.STRING,
        defaultValue = "/opt/config",
        label = "Graph Engine Config Path",
        displayPosition = 10,
        group = "Connection"
    )
    public String configPath;

    @ConfigDef(
        required = true,
        type = ConfigDef.Type.NUMBER,
        defaultValue = "1",
        label = "Thread Count",
        displayPosition = 20,
        group = "Connection"
    )
    public int threadCount;

    @ConfigDef(
        required = true,
        type = ConfigDef.Type.LIST,
        defaultValue = "",
        label = "DataWorker Repository Pipelines",
        displayPosition = 30,
        group = "Connection"
    )
    public List<String> pipelines;

    @Override
    public String getConfigPath() {
        return configPath;
    }

    @Override
    public int getNumberOfThreads() {
        return threadCount;
    }

    @Override
    public List<String> getPipelines() {
        return pipelines;
    }
}
