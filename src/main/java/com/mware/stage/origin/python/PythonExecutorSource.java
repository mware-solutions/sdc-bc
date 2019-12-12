package com.mware.stage.origin.python;

import com.mware.stage.lib.PythonRunnable;
import com.mware.stage.lib.ResponseAction;
import com.mware.stage.lib.Utils;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BasePushSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public abstract class PythonExecutorSource extends BasePushSource {
  private static final Logger LOG = LoggerFactory.getLogger(PythonExecutorSource.class);

  public abstract String getScriptPath();
  public abstract List<String> getParameters();
  public abstract boolean isJson();
  public abstract String getOutputSeparator();
  public abstract int getNumberOfThreads();

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
    if (runner != null) {
      runner.destroy();
    }
  }

  /** {@inheritDoc} */
  @Override
  public void produce(Map<String, String> offsets, int maxBatchSize) throws StageException {
    if (getContext().isPreview()) {
      return;
    }

    final ExecutorService executor = Executors.newFixedThreadPool(getNumberOfThreads());
    final List<Future<Runnable>> futures = new ArrayList<>();

    LOG.info("Executing python script from location: " + getScriptPath());
    final PushSource.Context context = getContext();
    Exception e = runner.runWithCallback(new ResponseAction() {
        @Override
        public void execute(int index, String responseLine) {
          final Future future = executor.submit(new RecordProducer(index, responseLine, context));
          futures.add(future);
        }
    });
    if (e != null && e instanceof StageException) {
      throw (StageException)e;
    }

    if (e == null || !(e instanceof InterruptedException)) {
        // Wait for execution end
        for (Future<Runnable> f : futures) {
          try {
            f.get();
          } catch (InterruptedException | ExecutionException ex) {
            LOG.error("Record generation threads have been interrupted", ex.getMessage());
          }
        }
    }

    executor.shutdownNow();
  }

  class RecordProducer implements Runnable {
    private final Logger log = LoggerFactory.getLogger(RecordProducer.class);

    private int index;
    private String responseLine;
    private PushSource.Context context;
    private boolean killed;

    public RecordProducer(int index, String responseLine, PushSource.Context context) {
        this.index = index;
        this.responseLine = responseLine;
        this.context = context;
        this.killed = false;
    }

    @Override
    public void run() {
      if (killed) {
        return;
      }

      BatchContext batchContext = context.startBatch();
      final String rid = "py-src-" + uuid + "::" + index;
      Record record = context.createRecord(rid);
      Utils.stringToMapRecord(record, responseLine, isJson(), getOutputSeparator());
      for (int i = 0; i < getContext().getOutputLanes().size(); i++) {
        batchContext.getBatchMaker().addRecord(record, getContext().getOutputLanes().get(i));
      }
      context.processBatch(batchContext);
      log.info("Produced record with id: " + rid);
    }

    public void kill() {
      this.killed = true;
    }
  }
}
