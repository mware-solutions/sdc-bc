package com.mware.stage.processor.python;

import com.mware.stage.common.error.Errors;
import com.mware.stage.lib.PythonExecutorOutputStreams;
import com.mware.stage.lib.PythonRunnable;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.RecordProcessor;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

public abstract class PythonExecutorProcessor extends RecordProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(PythonExecutorProcessor.class);

    /**
     * Gives access to the UI configuration of the stage provided by the {@link PythonExecutorDProcessor} class.
     */
    public abstract String getScriptPath();

    public abstract String getParamField();

    public abstract String getOutputSeparator();

    public abstract String getTargetField();

    public abstract String getInterpreterPath();

    private PythonRunnable runner;
    private String uuid;

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<ConfigIssue> init() {
        // Validate configuration values and open any required resources.
        List<ConfigIssue> issues = super.init();

        runner = new PythonRunnable(getInterpreterPath(), getScriptPath());

        // If issues is not empty, the UI will inform the user of each configuration issue in the list.
        return issues;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() {
        // Clean up any open resources.
        super.destroy();
        if (runner != null) {
            runner.destroy();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void process(Record record, BatchMaker batchMaker) throws StageException {
        if (!StringUtils.isEmpty(getParamField()) && !record.has(getParamField())) {
            throw new OnRecordErrorException(Errors.BC_01, record, "Parameter field: " + getParamField() + " was set but not found in this record.");
        }
        if (getContext().isStopped()) {
            return;
        }

        uuid = UUID.randomUUID().toString();
        if (!StringUtils.isEmpty(getParamField())) {
            final List<String> params = Arrays.asList(
                    record.get(getParamField()).getValueAsString().split(","));
            runner.setParameters(params);
        }

        Exception e = runner.runWithCallback((index, responseLine) -> {
            final String rid = "py-proc-" + uuid + "::" + index;
            Record record1 = null;
            if (StringUtils.isEmpty(getTargetField())) {
                record1 = getContext().createRecord(rid);
                if (StringUtils.isEmpty(getOutputSeparator())) {
                    record1.set(Field.create(responseLine));
                } else {
                    record1.set(Field.createListMap(createFields(responseLine)));
                }
            } else {
                record1 = record;
                if (StringUtils.isEmpty(getOutputSeparator())) {
                    record1.set(getTargetField(), Field.create(responseLine));
                } else {
                    record1.set(getTargetField(), Field.createListMap(createFields(responseLine)));
                }
            }

            batchMaker.addRecord(record1, getContext().getOutputLanes().get(PythonExecutorOutputStreams.COMMIT.ordinal()));
            batchMaker.addRecord(record1, getContext().getOutputLanes().get(PythonExecutorOutputStreams.PROCESS.ordinal()));

            LOG.debug("Produced record with id: " + rid);
        });

        if (e instanceof StageException) {
            ErrorCode errorCode = Errors.BC_01;
            Object[] params = ((StageException) e).getParams();
            if (params != null && params.length > 0) {
                record.get().getValueAsMap().put("error", Field.create(params[0].toString()));
            }

            batchMaker.addRecord(record, getContext().getOutputLanes().get(PythonExecutorOutputStreams.ERROR.ordinal()));

            throw new OnRecordErrorException(
                    record,
                    errorCode,
                    record.getHeader().getSourceId(),
                    e.getMessage(),
                    e
            );
        }
    }

    private LinkedHashMap<String, Field> createFields(String responseLine) {
        LinkedHashMap<String, Field> fields = new LinkedHashMap<>();
        String parts[] = responseLine.split(getOutputSeparator());
        for (int i = 0; i < parts.length; i++) {
            fields.put("field" + i, Field.create(parts[i]));
        }
        return fields;
    }
}
