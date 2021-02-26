package com.mware.stage.lib;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.Maps;
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
import java.util.logging.Level;

import static com.mware.bigconnect.driver.AuthTokens.basic;
import static com.mware.bigconnect.driver.Logging.none;

public class CypherUtils {
    private static final Logger LOG = LoggerFactory.getLogger(CypherUtils.class);

    public static Driver connect(String connectionString, String username, String password,
                                 List<Stage.ConfigIssue> issues, Stage.Context context) {
        Driver driver = null;
        try {
            URI uri = URI.create(connectionString);
            driver = BigConnect.driver(uri, basic(username, password), unsecureBuilder().build());
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

    public static Config.ConfigBuilder secureBuilder() {
        return Config.builder()
                .withEncryption()
                .withTrustStrategy(Config.TrustStrategy.trustAllCertificates());
    }

    public static Config.ConfigBuilder unsecureBuilder() {
        return Config.builder()
                .withoutEncryption();
    }

    public static Map<String, Object> prepareCypherParams(Record record, Map<String, String> queryParams) {
        final Map<String, Object> params = new HashMap<>();
        queryParams.forEach((k, v) -> {
            if (record.has(v)) {
                Object _v = record.get(v).getValue();
                params.put(k, toCypherValue(_v));
            } else {
                LOG.error("Can't find field: " + v);
                throw new StageException(QueryExecErrors.QUERY_EXECUTOR_002, v);
            }
        });

        return params;
    }

    private static Object toCypherValue(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Field) {
            Object result = ((Field)value).getValue();
            if (result instanceof Field || result instanceof Map || result instanceof ArrayList) {
                return toCypherValue(result);
            }
            return result;
        } else if (value instanceof Map) {
            Map<String, Object> fieldMap = Maps.newHashMap((Map<String, Object>) value);
            fieldMap.forEach((k1, v1) -> {
                fieldMap.put(k1, toCypherValue(v1));
            });
            return fieldMap;
        } else if (value instanceof ArrayList) {
            List<Field> fieldList = Lists.newArrayList((List <Field>) value);
            List<Object> valueList = new ArrayList<>();
            fieldList.forEach((f1) -> {
                valueList.add(toCypherValue(f1.getValue()));
            });
            return valueList;
        } else {
            return value;
        }
    }

    public static Session getSession(Driver driver) {
        return driver.session(
                SessionConfig.builder()
                        .withDefaultAccessMode(AccessMode.WRITE)
                        .build()
        );
    }
}
