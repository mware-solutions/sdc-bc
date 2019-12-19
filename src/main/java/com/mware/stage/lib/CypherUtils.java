package com.mware.stage.lib;

import com.mware.bigconnect.driver.*;
import com.mware.stage.common.error.QueryExecErrors;
import com.mware.stage.common.group.BigConnectCypherGroups;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mware.bigconnect.driver.AuthTokens.basic;
import static com.mware.bigconnect.driver.Logging.none;

public class CypherUtils {
    private static final Logger LOG = LoggerFactory.getLogger(CypherUtils.class);

    public static Driver connect(String connectionString, String username, String password,
                                 List<Stage.ConfigIssue> issues, Stage.Context context) {
        Driver driver = null;
        try {
            URI uri = URI.create(connectionString);
            driver = BigConnect.driver(uri, basic(username, password), insecureConfigBuilder().build());
            driver.verifyConnectivity();
        } catch (Exception e) {
            LOG.error("Can't open connection", e);
            issues.add(
                    context.createConfigIssue(
                            BigConnectCypherGroups.CONNECTION.name(),
                            "config.query",
                            QueryExecErrors.QUERY_EXECUTOR_001,
                            e.getMessage()
                    )
            );
        }

        return driver;
    }

    public static Map<String, Object> prepareCypherParams(Record record, Map<String, String> queryParams) {
        final Map<String, Object> params = new HashMap<>();
        queryParams.forEach((k, v) -> {
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

        return params;
    }

    public static Session getSession(Driver driver) {
        return driver.session(
                SessionConfig.builder()
                        .withDefaultAccessMode(AccessMode.WRITE)
                        .build()
        );
    }

    private static com.mware.bigconnect.driver.Config.ConfigBuilder insecureConfigBuilder() {
        return Config.builder()
                .withoutEncryption()
                .withLogging(none());
    }
}
