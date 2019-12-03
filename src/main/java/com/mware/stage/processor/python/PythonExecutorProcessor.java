package com.mware.stage.processor.python;

import com.mware.stage.lib.Errors;
import com.mware.stage.lib.PythonRunnable;
import com.mware.stage.lib.ResponseAction;
import com.mware.stage.lib.Utils;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public abstract class PythonExecutorProcessor extends SingleLaneRecordProcessor {

  /**
   * Gives access to the UI configuration of the stage provided by the {@link PythonExecutorDProcessor} class.
   */
  public abstract String getScriptPath();
  public abstract String getParamField();
  public abstract String getOutputSeparator();

  private PythonRunnable runner;
  private String uuid;

  /** {@inheritDoc} */
  @Override
  protected List<ConfigIssue> init() {
    // Validate configuration values and open any required resources.
    List<ConfigIssue> issues = super.init();

    runner = new PythonRunnable(getScriptPath());
    uuid = UUID.randomUUID().toString();

    // If issues is not empty, the UI will inform the user of each configuration issue in the list.
    return issues;
  }

  /** {@inheritDoc} */
  @Override
  public void destroy() {
    // Clean up any open resources.
    super.destroy();
  }

  /** {@inheritDoc} */
  @Override
  protected void process(Record record, final SingleLaneBatchMaker batchMaker) throws StageException {
    if (!StringUtils.isEmpty(getParamField()) && !record.has(getParamField())) {
      throw new OnRecordErrorException(Errors.BC_01, record, "Parameter field: " + getParamField() + " was set but not found in this record.");
    }
    if (!StringUtils.isEmpty(getParamField())) {
      final List<String> params = Arrays.asList(
              record.get(getParamField()).getValueAsString().split(","));
      runner.setParameters(params);
    }

    runner.runWithCallback(new ResponseAction() {
      @Override
      public void execute(int index, String responseLine) {
        Record record = getContext().createRecord("py-proc-" + uuid + "::" + index);
        Utils.stringToMapRecord(record, responseLine, getOutputSeparator());
        batchMaker.addRecord(record);
      }
    });
  }

}