package com.mware.stage.processor.bigconnect.cypher;

import com.mware.bigconnect.driver.*;
import com.mware.bigconnect.driver.Config;
import com.mware.bigconnect.driver.summary.ResultSummary;
import com.mware.stage.destination.bigconnect.cypher.Groups;
import com.mware.stage.destination.bigconnect.cypher.QueryExecErrors;
import com.mware.stage.lib.Utils;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.RecordProcessor;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;

import static com.mware.bigconnect.driver.AuthTokens.basic;
import static com.mware.bigconnect.driver.Logging.none;

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
    if (issues.isEmpty()) {
      try {
        URI uri = URI.create(getConnectionString());
        driver = BigConnect.driver(uri, basic( getUsername().get(), getPassword().get()), insecureConfigBuilder().build());
        driver.verifyConnectivity();
      } catch (Exception e) {
        LOG.error("Can't open connection", e);
        issues.add(
                context.createConfigIssue(
                        Groups.CONNECTION.name(),
                        "config.query",
                        QueryExecErrors.QUERY_EXECUTOR_001,
                        e.getMessage()
                )
        );
      }
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

    try (Session session = getSession()) {
        String _query = queryEval.eval(variables, getQuery(), String.class);
        Map<String, Object> params = new HashMap<>();
        getQueryParams().forEach((k, v) -> {
          if (record.has(v)) {
            Object _v = record.get(v).getValue();
            if (_v instanceof Map) {
              Map<String, Object> fieldMap = (Map<String, Object>) _v;
              fieldMap.forEach((k1, v1) -> {
                if (v1 instanceof Field) {
                  fieldMap.put(k1, ((Field) v1).getValue());
                }
              });
            } else if (_v instanceof ArrayList) {
              List<Field> fieldList = (List<Field>) _v;
              List<Object> valueList = new ArrayList<>();
              fieldList.forEach((f1) -> {
                valueList.add(f1.getValue());
              });
              _v = valueList;
            }
            params.put(k, _v);
          } else {
            LOG.error("Can't find field: " + v);
            throw new StageException(QueryExecErrors.QUERY_EXECUTOR_002, v);
          }
        });
      LOG.trace("Executing query: {}", _query);
      Statement statement = new Statement(_query, params);
      StatementResult result = session.run(statement);
      ResultSummary resultSummary = result.consume();
      batchMaker.addRecord(record);
    }
  }



  public Session getSession() {
    return driver.session(
            SessionConfig.builder()
                    .withDefaultAccessMode(AccessMode.WRITE)
                    .build()
    );
  }

  public static com.mware.bigconnect.driver.Config.ConfigBuilder insecureConfigBuilder() {
    return Config.builder()
            .withoutEncryption()
            .withLogging(none());
  }

}