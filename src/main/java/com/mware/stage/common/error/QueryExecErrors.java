package com.mware.stage.common.error;

import com.streamsets.pipeline.api.ErrorCode;

public enum QueryExecErrors implements ErrorCode {
    QUERY_EXECUTOR_001("Can't open connection: {}"),
    QUERY_EXECUTOR_002("Could not find field at path: {}"),
    QUERY_EXECUTOR_003("Invalid field at path: {}. Should be convertible to 'long' value"),
    QUERY_EXECUTOR_004("Message ack/nack error: {}"),
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
