package com.mware.stage.origin.python;

import com.mware.stage.lib.PythonRunnable;
import com.mware.stage.lib.ResponseAction;
import com.mware.stage.lib.Utils;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public abstract class PythonExecutorSource extends BaseSource {
  private static final Logger LOG = LoggerFactory.getLogger(PythonExecutorSource.class);

  public abstract String getScriptPath();
  public abstract List<String> getParameters();
  public abstract String getOutputSeparator();

  private PythonRunnable runner;
  private String uuid;

  @Override
  protected List<ConfigIssue> init() {
    // Validate configuration values and open any required resources.
    List<ConfigIssue> issues = super.init();

    runner = new PythonRunnable(getScriptPath());
    runner.setParameters(getParameters());
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
  public String produce(String lastSourceOffset, int maxBatchSize, final BatchMaker batchMaker) throws StageException {
    if ("no-more-data".equals(lastSourceOffset)) {
      return "no-more-data";
    }

    LOG.info("Executing python script from location: " + getScriptPath());
    Exception e = runner.runWithCallback(new ResponseAction() {
        @Override
        public void execute(int index, String responseLine) {
          Record record = getContext().createRecord("py-src-" + uuid + "::" + index);
          Utils.stringToMapRecord(record, responseLine, getOutputSeparator());
          batchMaker.addRecord(record);
        }
    });
    if (e instanceof StageException) {
      throw (StageException)e;
    }

    return String.valueOf("no-more-data");
  }

}
