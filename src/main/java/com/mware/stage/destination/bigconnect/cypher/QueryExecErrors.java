package com.mware.stage.destination.bigconnect.cypher;

import com.streamsets.pipeline.api.ErrorCode;

public enum QueryExecErrors implements ErrorCode {
    QUERY_EXECUTOR_001("Can't open connection: {}"),
    QUERY_EXECUTOR_002("Could not find field at path: {}"),
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
