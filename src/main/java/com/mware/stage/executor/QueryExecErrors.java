package com.mware.stage.executor;

import com.streamsets.pipeline.api.ErrorCode;

public enum QueryExecErrors implements ErrorCode {
    QUERY_EXECUTOR_001("Failed to execute query '{}': {}"),
    QUERY_EXECUTOR_002("Can't open connection: {}"),
    QUERY_EXECUTOR_004("Error executing statement {} "),
    QUERY_EXECUTOR_005("Connection Error {}"),
    QUERY_EXECUTOR_006("Exception while waiting for completion {}"),
    ;

    private final String msg;

    QueryExecErrors(String msg) {
        this.msg = msg;
    }

    @Override
    public String getCode() {
        return name();
    }

    @Override
    public String getMessage() {
        return msg;
    }
}
