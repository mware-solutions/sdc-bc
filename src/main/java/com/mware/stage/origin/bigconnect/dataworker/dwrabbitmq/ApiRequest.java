package com.mware.stage.origin.bigconnect.dataworker.dwrabbitmq;

public class ApiRequest {
    private String id;
    private String parameters;

    public ApiRequest(String id, String parameters) {
        this.id = id;
        this.parameters = parameters;
    }
}
