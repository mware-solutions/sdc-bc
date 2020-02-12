package com.mware.stage.origin.bigconnect.rabbitmq;

import com.streamsets.pipeline.api.*;

@StageDef(
        version = 1,
        label = "BigConnect RabbitMQ Origin",
        description = "Gets DataWorker messages from RabbitMQ queue",
        icon = "bc.png",
        execution = ExecutionMode.STANDALONE,
        recordsByRef = true,
        onlineHelpRefUrl = ""
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class BigConnectRabbitMqDSource extends BigConnectRabbitMqSource {
    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "/opt/bdl/etc/explorer",
            label = "BigConnect Graph Engine Config Path",
            displayPosition = 0,
            group = "Connection"
    )
    public String configPath;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.NUMBER,
            defaultValue = "1",
            label = "Thread Count",
            displayPosition = 1,
            group = "Connection"
    )
    public int threadCount;


    public String getConfigPath() {
        return configPath;
    }

    @Override
    public int getNumberOfThreads() {
        return threadCount;
    }
}
