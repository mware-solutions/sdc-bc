package com.mware.stage.destination.bigconnect.cypher;

import com.mware.bigconnect.driver.*;
import com.mware.stage.lib.CypherUtils;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseExecutor;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;


public abstract class BigConnectCypherTarget extends BaseExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(BigConnectCypherTarget.class);

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
    public void write(Batch batch) throws StageException {
        ELVars variables = getContext().createELVars();
        ELEval queryEval = getContext().createELEval("query");

        Iterator<Record> it = batch.getRecords();
        try (Session session = CypherUtils.getSession(driver)) {
            while (it.hasNext()) {
                Record record = it.next();
                String _query = queryEval.eval(variables, getQuery(), String.class);

                processARecord(session,
                               _query,
                               CypherUtils.prepareCypherParams(record, getQueryParams()),
                               record);
            }
        }
    }

    private void processARecord(Session session, String _query, Map<String, Object> params, Record record) throws StageException {
        LOG.trace("Executing query: {}", _query);
        Statement statement = new Statement(_query, params);
        final CypherUtils.RetryOnExceptionStrategy retry = new CypherUtils.RetryOnExceptionStrategy();
        while (retry.shouldRetry()) {
            try {
                session.run(statement);
                retry.finished();
            } catch (Exception e) {
                try {
                    retry.errorOccured();
                } catch (Exception e1) {
                    LOG.warn("Record could not be processed: " + e1.getMessage());
                }
            }
        }
    }

    @Override
    public void destroy() {
        if (driver != null) {
            driver.closeAsync();
        }
        super.destroy();
    }
}
