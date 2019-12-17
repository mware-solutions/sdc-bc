package com.mware.stage.processor.python;

import com.mware.stage.lib.Errors;
import com.mware.stage.lib.PythonRunnable;
import com.mware.stage.lib.ResponseAction;
import com.mware.stage.lib.Utils;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.RecordProcessor;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public abstract class PythonExecutorProcessor extends RecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(PythonExecutorProcessor.class);

  /**
   * Gives access to the UI configuration of the stage provided by the {@link PythonExecutorDProcessor} class.
   */
  public abstract String getScriptPath();
  public abstract String getParamField();
  public abstract boolean isJson();
  public abstract String getOutputSeparator();

  private PythonRunnable runner;
  private String uuid;

  /** {@inheritDoc} */
  @Override
  protected List<ConfigIssue> init() {
    // Validate configuration values and open any required resources.
    List<ConfigIssue> issues = super.init();

    runner = new PythonRunnable(getScriptPath());

    // If issues is not empty, the UI will inform the user of each configuration issue in the list.
    return issues;
  }

  /** {@inheritDoc} */
  @Override
  public void destroy() {
    // Clean up any open resources.
    super.destroy();
    if (runner != null) {
      runner.destroy();
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void process(Record record, BatchMaker batchMaker) throws StageException {
    if (!StringUtils.isEmpty(getParamField()) && !record.has(getParamField())) {
      throw new OnRecordErrorException(Errors.BC_01, record, "Parameter field: " + getParamField() + " was set but not found in this record.");
    }
    if (getContext().isPreview() || getContext().isStopped()) {
      return;
    }

    uuid = UUID.randomUUID().toString();
    if (!StringUtils.isEmpty(getParamField())) {
      final List<String> params = Arrays.asList(
              record.get(getParamField()).getValueAsString().split(","));
      runner.setParameters(params);
    }

    Exception e = runner.runWithCallback(new ResponseAction() {
      @Override
      public void execute(int index, String responseLine) {
        final String rid = "py-proc-" + uuid + "::" + index;
        Record record = getContext().createRecord(rid);
        Utils.stringToMapRecord(record, responseLine, isJson(), getOutputSeparator());
        for (int i = 0; i < getContext().getOutputLanes().size(); i++) {
          batchMaker.addRecord(record, getContext().getOutputLanes().get(i));
        }
        LOG.info("Produced record with id: " + rid);
      }
    });

    if (e != null && e instanceof StageException) {
      ErrorCode errorCode = Errors.BC_01;
      Object[] params = ((StageException) e).getParams();
      if (params != null && params.length > 0) {
        if (params[0].toString().contains("Profile not public")) {
          errorCode = Errors.BC_CUST_01;
        }
        if (params[0].toString().contains("Needs login")) {
          errorCode = Errors.BC_CUST_02;
        }
      }
      throw new OnRecordErrorException(
              record,
              errorCode,
              record.getHeader().getSourceId(),
              e.getMessage(),
              e
      );
    }
  }

}