package com.mware.stage.destination.bigconnect.cypher;

import com.mware.stage.common.group.BigConnectCypherGroups;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.credential.CredentialValue;

import java.util.Map;

@StageDef(
        version = 1,
        label = "BigConnect Cypher Target",
        description = "Execute Cypher queries against a BigConnect Bolt server [T]",
        icon = "bc.png",
        onlineHelpRefUrl = ""
)
@ConfigGroups(value = BigConnectCypherGroups.class)
@GenerateResourceBundle
@PipelineLifecycleStage
public class BigConnectCypherDTarget extends BigConnectCypherTarget {
    @ConfigDef(
            required = true,
            type = ConfigDef.Type.TEXT,
            mode = ConfigDef.Mode.SQL,
            label = "Cypher Query",
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
            defaultValue = "bolt://localhost:10242",
            displayPosition = 10,
            group = "CONNECTION"
    )
    public String connectionString = "";

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.CREDENTIAL,
            label = "Username",
            defaultValue = "admin",
            displayPosition = 20,
            group = "CONNECTION"
    )
    public CredentialValue username;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.CREDENTIAL,
            label = "Password",
            defaultValue = "admin",
            displayPosition = 30,
            group = "CONNECTION"
    )
    public CredentialValue password;

    @Override
    public String getQuery() {
        return query;
    }

    @Override
    public Map<String, String> getQueryParams() {
        return queryParams;
    }

    @Override
    public String getConnectionString() {
        return connectionString;
    }

    @Override
    public CredentialValue getUsername() {
        return username;
    }

    @Override
    public CredentialValue getPassword() {
        return password;
    }
}
