package com.mware.stage.processor.bigconnect.rabbitmq;

import com.mware.stage.origin.bigconnect.rabbitmq.Groups;
import com.streamsets.pipeline.api.*;

@StageDef(
        version = 1,
        label = "BDL DataWorker Sender",
        description = "Add messages to the Data Worker queue",
        icon = "bc.png",
        onlineHelpRefUrl = ""
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
@PipelineLifecycleStage
public class BigConnectDataWorkerQueueDProcessor extends BigConnectDataWorkerQueueProcessor {
    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "/opt/bdl/etc/sdcdw",
            label = "Graph Engine Config Path",
            displayPosition = 0,
            group = "Connection"
    )
    public String configPath;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "/mOrigMessage",
            label = "Message field",
            description = "Byte array value for message body that will be passed to RabbitMQ",
            displayPosition = 1,
            group = "Connection"
    )
    public String messageDataField;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "/mOrigPriority",
            label = "Priority field",
            description = "Integer value for message priority that will be passed to RabbitMQ",
            displayPosition = 2,
            group = "Connection"
    )
    public String messagePriorityField;

    public String getConfigPath() {
        return configPath;
    }

    public String getMessageDataField() {
        return messageDataField;
    }

    public String getMessagePriorityField() {
        return messagePriorityField;
    }
}
