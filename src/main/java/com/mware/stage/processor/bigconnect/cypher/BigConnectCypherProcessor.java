package com.mware.stage.processor.bigconnect.cypher;

import com.mware.bigconnect.driver.Driver;
import com.mware.bigconnect.driver.Session;
import com.mware.bigconnect.driver.Statement;
import com.mware.bigconnect.driver.StatementResult;
import com.mware.stage.lib.CypherUtils;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.RecordProcessor;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class BigConnectCypherProcessor extends RecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(BigConnectCypherProcessor.class);

  public abstract String getQuery();
  public abstract Map<String, String> getQueryParams();
  public abstract String getConnectionString();
  public abstract CredentialValue getUsername();
  public abstract CredentialValue getPassword();

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
  protected void process(Record record, BatchMaker batchMaker) throws StageException {
    ELVars variables = getContext().createELVars();
    ELEval queryEval = getContext().createELEval("query");

    try (Session session = CypherUtils.getSession(driver)) {
        String _query = queryEval.eval(variables, getQuery(), String.class);
        LOG.trace("Executing query: {}", _query);
        Statement statement = new Statement(_query,
                CypherUtils.prepareCypherParams(record, getQueryParams()));
        StatementResult result = session.run(statement);

        record.set("/cypher", Field.create( // TODO - make "/cypher" field name configurable (if needed)
                          parseCypherResult(result)));
        batchMaker.addRecord(record);
    }
  }

  private List<Field> parseCypherResult(StatementResult result) {
    List<Field> cypherResult = new ArrayList<>();
    Map<String, Object> row;
    while (result.hasNext()) {
      com.mware.bigconnect.driver.Record r = result.next();
      row = r.get(0).asMap();
      final Map<String, Field> srow = new HashMap<>();
      for (Map.Entry<String, Object> e : row.entrySet()) {
        srow.put(e.getKey(), Field.create(e.getValue().toString()));
      }
      cypherResult.add(Field.create(srow));
    }

    return cypherResult;
  }
}