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
        type = ConfigDef.Type.BOOLEAN,
        defaultValue = "false",
        label = "Read pipelines from file",
        displayPosition = 30,
        group = "Connection"
    )
    public boolean fromFile;

    @ConfigDef(
        required = false,
        type = ConfigDef.Type.STRING,
        label = "Pipeline list file",
        dependsOn = "fromFile",
        triggeredByValue = "true",
        displayPosition = 40,
        group = "Connection"
    )
    public String pipelineFilePath;

    @ConfigDef(
        required = false,
        type = ConfigDef.Type.LIST,
        defaultValue = "",
        label = "DataWorker Repository Pipelines",
        dependsOn = "fromFile",
        triggeredByValue = "false",
        displayPosition = 50,
        group = "Connection"
    )
    public List<String> pipelines;

    @ConfigDef(
        required = true,
        type = ConfigDef.Type.BOOLEAN,
        defaultValue = "true",
        label = "Pipeline Control REST API",
        displayPosition = 60,
        group = "Connection"
    )
    public boolean restApi;

    @ConfigDef(
        required = false,
        type = ConfigDef.Type.STRING,
        label = "Pipeline Control config file",
        dependsOn = "restApi",
        triggeredByValue = "false",
        displayPosition = 70,
        group = "Connection"
    )
    public String pcConfigPath;

    @ConfigDef(
        required = true,
        type = ConfigDef.Type.NUMBER,
        defaultValue = "0",
        label = "Max running pipeline threads",
        description = "Set to 0 for no limit",
        dependsOn = "restApi",
        triggeredByValue = "false",
        displayPosition = 80,
        group = "Connection"
    )
    public int pipelineThreads;


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

    @Override
    public boolean isFromFile() {
        return fromFile;
    }

    @Override
    public String getPipelineFilePath() {
        return pipelineFilePath;
    }

    @Override
    public boolean isRestApi() {
        return restApi;
    }

    @Override
    public String getPcConfigPath() {
        return pcConfigPath;
    }

    @Override
    public int getPipelineThreads() {
        return pipelineThreads;
    }
}
