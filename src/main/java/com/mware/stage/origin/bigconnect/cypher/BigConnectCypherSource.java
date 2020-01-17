package com.mware.stage.origin.bigconnect.cypher;

import com.mware.bigconnect.driver.Driver;
import com.mware.bigconnect.driver.Session;
import com.mware.bigconnect.driver.Statement;
import com.mware.bigconnect.driver.StatementResult;
import com.mware.stage.lib.CypherUtils;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public abstract class BigConnectCypherSource extends BasePushSource {
  private static final Logger LOG = LoggerFactory.getLogger(BigConnectCypherSource.class);

  public abstract String getQuery();
  public abstract String getConnectionString();
  public abstract CredentialValue getUsername();
  public abstract CredentialValue getPassword();
  public abstract int getNumberOfThreads();

  private Driver driver;

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    init(getContext(), issues);
    return issues;
  }

  private void init(Stage.Context context, List<ConfigIssue> issues) {
    if(issues.isEmpty()) {
      driver = CypherUtils.connect(
              getConnectionString(), getUsername().get(), getPassword().get(), issues, context);
    }
  }

  @Override
  public void destroy() {
    if (driver != null) {
      driver.close();
    }
    super.destroy();
  }

  /** {@inheritDoc} */
  @Override
  public void produce(Map<String, String> offsets, int maxBatchSize) throws StageException {
    final ExecutorService executor = Executors.newFixedThreadPool(getNumberOfThreads());
    final List<Future<Runnable>> futures = new ArrayList<>();
    ELVars variables = getContext().createELVars();
    ELEval queryEval = getContext().createELEval("query");

    try (Session session = CypherUtils.getSession(driver)) {
      String _query = queryEval.eval(variables, getQuery(), String.class);
      LOG.trace("Executing query: {}", _query);
      Statement statement = new Statement(_query);
      StatementResult result = session.run(statement);

      while (result.hasNext()) {
        final Future future = executor.submit(new SDCRecordProducer(getContext(), result.next()));
        futures.add(future);
      }

      // Wait for execution end
      for (Future<Runnable> f : futures) {
        try {
          f.get();
        } catch (InterruptedException | ExecutionException ex) {
          LOG.error("SDC Record generation threads have been interrupted", ex.getMessage());
        }
      }
      executor.shutdownNow();
    }

    // takes a long time to finish
//    try {
//      driver.close();
//      driver = null;
//    } catch (Exception ex) {
//      ex.printStackTrace();
//    }
  }

  class SDCRecordProducer implements Runnable {
    private final Logger log = LoggerFactory.getLogger(BigConnectCypherSource.SDCRecordProducer.class);

    private PushSource.Context context;
    private com.mware.bigconnect.driver.Record cypherRecord;

    public SDCRecordProducer(PushSource.Context context, com.mware.bigconnect.driver.Record cypherRecord) {
      this.context = context;
      this.cypherRecord = cypherRecord;
    }

    @Override
    public void run() {
      BatchContext batchContext = context.startBatch();
      final String rid = "bcc-src-" + UUID.randomUUID().toString();
      Record record = context.createRecord(rid);

      Map<String, Field> row = new HashMap<>();
      if ("node".equals(cypherRecord.get(0).type().name().toLowerCase())) {
        for (Map.Entry<String, Object> e : cypherRecord.get(0).asMap().entrySet()) {
          row.put(e.getKey(), Field.create(e.getValue().toString()));
        }
      } else {
        for (int i = 0; i < cypherRecord.size(); i++) {
          row.put(cypherRecord.keys().get(i), Field.create(cypherRecord.get(i).asString()));
        }
      }
      record.set(Field.create(row));


      batchContext.getBatchMaker().addRecord(record);
      context.processBatch(batchContext);
      log.info("Produced record with id: " + rid);
    }
  }
}
