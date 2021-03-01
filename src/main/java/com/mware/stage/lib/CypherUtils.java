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

import static com.mware.bigconnect.driver.AuthTokens.basic;

public class CypherUtils {
    private static final Logger LOG = LoggerFactory.getLogger(CypherUtils.class);

    public static Driver connect(String connectionString, String username, String password,
                                 List<Stage.ConfigIssue> issues, Stage.Context context) {
        Driver driver = null;
        try {
            URI uri = URI.create(connectionString);
            driver = BigConnect.driver(uri, basic(username, password), unsecureBuilder().build());
            final RetryOnExceptionStrategy retry = new RetryOnExceptionStrategy();
            while (retry.shouldRetry()) {
                try {
                    driver.verifyConnectivity();
                    retry.finished();
                } catch (Exception e) {
                    retry.errorOccured();
                }
            }
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

    public static class RetryOnExceptionStrategy {
        public static final int DEFAULT_RETRIES = 3;
        public static final long DEFAULT_WAIT_TIME_IN_MILLI = 2000;

        private int numberOfRetries;
        private int numberOfTriesLeft;
        private long timeToWait;

        public RetryOnExceptionStrategy() {
            this(DEFAULT_RETRIES, DEFAULT_WAIT_TIME_IN_MILLI);
        }

        public RetryOnExceptionStrategy(int numberOfRetries,
                                             long timeToWait) {
            this.numberOfRetries = numberOfRetries;
            numberOfTriesLeft = numberOfRetries;
            this.timeToWait = timeToWait;
        }

        /**
         * @return true if there are tries left
         */
        public boolean shouldRetry() {
            return numberOfTriesLeft > 0;
        }

        public void errorOccured() throws Exception {
            if (!shouldRetry()) {
                throw new Exception("Retry Failed: Total " + numberOfRetries
                        + " attempts made at interval " + getTimeToWait()
                        + "ms");
            }
            numberOfTriesLeft--;
            waitUntilNextTry();
        }

        public void finished() {
            numberOfTriesLeft = 0;
        }

        public long getTimeToWait() {
            return timeToWait;
        }

        private void waitUntilNextTry() {
            try {
                Thread.sleep(getTimeToWait());
            } catch (InterruptedException ignored) {
            }
        }
    }
}
