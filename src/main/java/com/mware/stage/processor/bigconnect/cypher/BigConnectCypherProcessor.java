package com.mware.stage.processor.bigconnect.cypher;

import com.mware.bigconnect.driver.*;
import com.mware.bigconnect.driver.internal.value.*;
import com.mware.bigconnect.driver.types.Node;
import com.mware.bigconnect.driver.types.Relationship;
import com.mware.stage.lib.CypherUtils;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.RecordProcessor;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Date;
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
      driver.closeAsync();
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
        StatementResult result = null;
        final CypherUtils.RetryOnExceptionStrategy retry = new CypherUtils.RetryOnExceptionStrategy();
        while (retry.shouldRetry()) {
          try {
            result = session.run(statement);
            retry.finished();
          } catch (Exception e) {
            try {
              retry.errorOccured();
            } catch (Exception e1) {
              LOG.warn("Record could not be processed: " + e1.getMessage());
            }
          }
        }

        if (result != null) {
          record.set("/cypher", Field.create( parseCypherResult(result) ));
          batchMaker.addRecord(record);
        }
    } catch (Exception ex) {
      ex.printStackTrace();
      throw ex;
    }
  }

  private Map<String, Field> parseCypherResult(StatementResult result) {
    Map<String, Field> cypherResult = new HashMap<>();

    while (result.hasNext()) {
      com.mware.bigconnect.driver.Record r = result.next();
      r.asMap().forEach((k, v) -> {
        cypherResult.put(k, toSdcField(v));
      });
    }

    return cypherResult;
  }

  private Field toSdcField(Object v) {
    if (v instanceof Node) {
      Node n = (Node) v;
      return Field.create(n.asMap(this::bcValueToField));
    } else if (v instanceof Relationship) {
      Relationship r = (Relationship) v;
      return Field.create(r.asMap(this::bcValueToField));
    } else {
      return Field.create(v.toString());
    }
  }

  private Field bcValueToField(Value v) {
    if (v instanceof StringValue) {
      return Field.create(((StringValue)v).asObject());
    } else if (v instanceof BooleanValue) {
      return Field.create(((BooleanValue)v).asObject());
    } else if (v instanceof FloatValue) {
      return Field.create(((FloatValue)v).asObject());
    } else if (v instanceof IntegerValue) {
      return Field.create(((IntegerValue)v).asObject());
    } else if (v instanceof ListValue) {
      return Field.create(v.asList(this::bcValueToField));
    } else if (v instanceof MapValue) {
      return Field.create(v.asMap(this::bcValueToField));
    } else if (v instanceof DateTimeValue) {
      DateTimeValue dtv = (DateTimeValue) v;
      return Field.createZonedDateTime(dtv.asZonedDateTime());
    } else if (v instanceof DateValue) {
      DateValue dtv = (DateValue) v;
      Instant utcStartOfDay = dtv.asLocalDate().atStartOfDay().atZone(ZoneOffset.UTC).toInstant();
      return Field.createDate(Date.from(utcStartOfDay));
    } else if (v instanceof LocalDateTimeValue) {
      LocalDateTimeValue dtv = (LocalDateTimeValue) v;
      return Field.createZonedDateTime(dtv.asLocalDateTime().atZone(ZoneOffset.UTC));
    } else if (v instanceof LocalTimeValue) {
      LocalTimeValue dtv = (LocalTimeValue) v;
      Date d = Date.from(dtv.asLocalTime().atDate(LocalDate.now()).atZone(ZoneOffset.UTC).toInstant());
      return Field.createTime(d);
    }
    else
      return Field.create(v.asString());
  }
}
