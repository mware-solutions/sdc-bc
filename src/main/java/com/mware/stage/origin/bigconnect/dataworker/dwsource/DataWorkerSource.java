package com.mware.stage.origin.bigconnect.dataworker.dwsource;

import com.mware.stage.common.error.Errors;
import com.mware.stage.destination.bigconnect.simple.Groups;
import com.mware.stage.lib.BigConnectSystem;
import com.mware.stage.origin.bigconnect.dataworker.common.MessageProcessor;
import com.mware.stage.origin.bigconnect.dataworker.common.SdcDataWorkerItem;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Base64;
import java.util.List;

public abstract class DataWorkerSource extends BaseSource {
    private final static Logger LOGGER = LoggerFactory.getLogger(DataWorkerSource.class);

    public abstract String getConfigPath();

    private static final Object lock = new Object();
    public static BigConnectSystem bigConnect;
    private MessageProcessor messageProcessor;

    @Override
    protected List<ConfigIssue> init() {
        // Validate configuration values and open any required resources.
        List<ConfigIssue> issues = super.init();
        File configDir = new File(getConfigPath());
        if (!configDir.exists() || !configDir.isDirectory()) {
            issues.add(
                    getContext().createConfigIssue(
                            Groups.Connection.name(), "config", Errors.BC_00, "" +
                                    "BigConnect Config path does not exist or it's not a directory."));
        }
        try {
            synchronized (lock) {
                if (bigConnect == null || bigConnect.getGraph() == null ||
                        bigConnect.getUserRepository() == null || bigConnect.getAuthorizationRepository() == null) {
                    bigConnect = BigConnectSystem.getInstance();
                    bigConnect.init(getConfigPath());
                }
                messageProcessor = new MessageProcessor(bigConnect, getContext());
            }
        } catch (Exception e) {
            issues.add(
                    getContext().createConfigIssue(
                            Groups.Connection.name(), "config", Errors.BC_00,
                            "Could not connect to BigConnect Graph Engine: " + e.getMessage()));
            LOGGER.error("", e);
        }

        return issues;
    }

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        // Get work message from pipeline's parameters
        final String message = (String)getContext().createELVars().getConstant(SdcDataWorkerItem.WORK_PIPELINE_PARAM);
        byte[] workMessage = Base64.getDecoder().decode(message);

        List<Record> records = null;
        try {
            records = messageProcessor.process(workMessage);
        } catch (Exception e) {
            LOGGER.warn("Couldn't produce record from work message", e.getMessage());
        }
        if (records != null) {
            for (Record record : records) {
                batchMaker.addRecord(record);
            }
        }

        return null;
    }

    @Override
    public void destroy() {
        /**
         * bigConnect is cached and lives as long as SDC lives. Uncomment this if caching is removed
         */
//        if (bigConnect != null && bigConnect.getGraph() != null) {
//            bigConnect.shutDown();
//        }
    }
}
