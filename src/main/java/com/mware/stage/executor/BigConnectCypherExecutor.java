package com.mware.stage.executor;

import com.mware.bigconnect.driver.*;
import com.mware.bigconnect.driver.Config;
import com.mware.bigconnect.driver.summary.ResultSummary;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.BaseExecutor;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.mware.bigconnect.driver.AuthTokens.basic;
import static com.mware.bigconnect.driver.Logging.none;

@StageDef(
        version = 3,
        label = "BigConnect Cypher Executor",
        description = "Execute Cypher queries against a BigConnect Bolt server",
        icon = "bc.png",
        onlineHelpRefUrl = ""
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
@PipelineLifecycleStage
public class BigConnectCypherExecutor extends BaseExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(BigConnectCypherExecutor.class);

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.TEXT,
            mode = ConfigDef.Mode.SQL,
            label = "Cpher Query",
            description = "Cypher Query that should be executed for each incoming record.",
            evaluation = ConfigDef.Evaluation.EXPLICIT,
            displayPosition = 10,
            group = "CYPHER"
    )
    public String query;

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.MAP,
            label = "Query Parameters",
            displayPosition = 20,
            group = "CYPHER"
    )
    public Map<String, String> queryParams;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            label = "Bolt Connection String",
            displayPosition = 10,
            group = "CONNECTION"
    )
    public String connectionString = "";

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.CREDENTIAL,
            label = "Username",
            displayPosition = 20,
            group = "CONNECTION"
    )
    public CredentialValue username;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.CREDENTIAL,
            label = "Password",
            displayPosition = 30,
            group = "CONNECTION"
    )
    public CredentialValue password;

    Driver driver;

    @Override
    protected List<ConfigIssue> init() {
        List<ConfigIssue> issues = super.init();
        init(getContext(), issues);
        return issues;
    }

    private void init(Stage.Context context, List<ConfigIssue> issues) {
        if(issues.isEmpty()) {
            try {
                URI uri = URI.create(connectionString);
                driver = BigConnect.driver(uri, basic( username.get(), password.get()), insecureConfigBuilder().build());
                driver.verifyConnectivity();
            } catch (Exception e) {
                LOG.error("Can't open connection", e);
                issues.add(
                        context.createConfigIssue(
                                Groups.CONNECTION.name(),
                                "config.query",
                                QueryExecErrors.QUERY_EXECUTOR_002,
                                e.getMessage()
                        )
                );
            }
        }
    }

    @Override
    public void write(Batch batch) throws StageException {
        ELVars variables = getContext().createELVars();
        ELEval queryEval = getContext().createELEval("query");

        Iterator<Record> it = batch.getRecords();
        try (Session session = getSession()) {
            while (it.hasNext()) {
                Record record = it.next();
                String _query = queryEval.eval(variables, query, String.class);
                Map<String, Object> params = new HashMap<>();
                queryParams.forEach((k, v) -> {
                    Object _v = record.get(v).getValue();
                    params.put(k, _v);
                });
                processARecord(session, _query, params, record);
            }
        } catch (Exception ex) {
            LOG.error("Can't get connection", ex);
            throw new StageException(QueryExecErrors.QUERY_EXECUTOR_002, ex.getMessage());
        }
    }

    private void processARecord(Session session, String _query, Map<String, Object> params, Record record) throws StageException {
        LOG.trace("Executing query: {}", _query);
        Statement statement = new Statement(_query, params);
        StatementResult result = session.run(statement);
        ResultSummary resultSummary = result.consume();

        // maybe trigger events
    }

    @Override
    public void destroy() {
        if (driver != null) {
            driver.close();
        }
        super.destroy();
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
