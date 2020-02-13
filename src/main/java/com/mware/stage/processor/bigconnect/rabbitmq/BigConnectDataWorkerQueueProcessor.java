package com.mware.stage.processor.bigconnect.rabbitmq;

import com.mware.core.config.Configuration;
import com.mware.core.exception.BcException;
import com.mware.core.lifecycle.LifeSupportService;
import com.mware.core.model.WorkQueueNames;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.model.workQueue.RabbitMQWorkQueueRepository;
import com.mware.stage.common.error.Errors;
import com.mware.stage.destination.bigconnect.simple.Groups;
import com.mware.stage.lib.BigConnectSystem;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.RecordProcessor;

import java.io.File;
import java.util.Iterator;
import java.util.List;

public abstract class BigConnectDataWorkerQueueProcessor extends RecordProcessor {
    public abstract String getConfigPath();
    public abstract String getMessageDataField();
    public abstract String getMessagePriorityField();
    private Configuration configuration;
    private RabbitMQWorkQueueRepository workQueueRepository;
    private WorkQueueNames queueNames;

    @Override
    protected List<ConfigIssue> init() {
        // Validate configuration values and open any required resources.
        List<ConfigIssue> issues = super.init();
        File configDir = new File(getConfigPath());
        if (!configDir.exists() || !configDir.isDirectory()) {
            issues.add(
                    getContext().createConfigIssue(
                            Groups.OntologyMapping.name(), "config", Errors.BC_00, "BigConnect Graph Engine config path does not exist or it's not a directory."
                    )
            );
        }

        try {
            configuration = BigConnectSystem.getInstance().getConfiguration(getConfigPath());
        } catch (Exception e) {
            issues.add(
                    getContext().createConfigIssue(
                            Groups.Connection.name(), "config", Errors.BC_00, "Could not parse configuration: "+e.getMessage()
                    )
            );
            e.printStackTrace();
        }

        try {
            queueNames = new WorkQueueNames(configuration);
            workQueueRepository = new RabbitMQWorkQueueRepository(null, new WorkQueueNames(configuration), configuration, new LifeSupportService());
        } catch (Exception e) {
            issues.add(
                    getContext().createConfigIssue(
                            Groups.Connection.name(), "config", Errors.BC_00, "Could not connect to RabbitMQ: "+e.getMessage()
                    )
            );
            e.printStackTrace();
        }

        return issues;
    }

    @Override
    protected void process(Record record, BatchMaker batchMaker) throws StageException {
        if (!record.has(getMessageDataField())) {
            throw new OnRecordErrorException(Errors.BC_01, record, "Field path " + getMessageDataField() + " was not found.");
        }

        if (!record.has(getMessagePriorityField())) {
            throw new OnRecordErrorException(Errors.BC_01, record, "Field path " + getMessagePriorityField() + " was not found.");
        }

        byte[] messageData = record.get(getMessageDataField()).getValueAsByteArray();
        int priority = record.get(getMessagePriorityField()).getValueAsInteger();

        try {
            workQueueRepository.pushOnQueue(queueNames.getDataWorkerQeueName(), messageData, fromRabbitMqPriority(priority));
        } catch (BcException ex) {
            ex.printStackTrace();
            throw new OnRecordErrorException(Errors.BC_01, record, "Could not send message to queue: " + ex.getMessage());
        }

        batchMaker.addRecord(record);
    }

    @Override
    public void destroy() {
        try {
            workQueueRepository.shutdown();
        } catch (Throwable t) {
            // nothing
        }
    }


    public Priority fromRabbitMqPriority(int priority) {
        switch (priority) {
            case 0:
                return Priority.LOW;
            case 2:
                return Priority.HIGH;
            case 1:
                return Priority.NORMAL;
            default:
                return Priority.NORMAL;
        }
    }
}
