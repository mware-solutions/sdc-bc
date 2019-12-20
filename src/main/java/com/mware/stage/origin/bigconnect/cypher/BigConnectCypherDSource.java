package com.mware.stage.origin.bigconnect.cypher;

import com.mware.stage.common.group.BigConnectCypherGroups;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.credential.CredentialValue;

@StageDef(
        version = 1,
        label = "BigConnect Cypher Source",
        description = "Execute Cypher queries against a BigConnect Bolt server [O]",
        icon = "bc.png",
        execution = ExecutionMode.STANDALONE,
        recordsByRef = true,
        onlineHelpRefUrl = ""
)
@ConfigGroups(value = BigConnectCypherGroups.class)
@GenerateResourceBundle
public class BigConnectCypherDSource extends BigConnectCypherSource {
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

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.NUMBER,
            defaultValue = "1",
            label = "Thread Count",
            displayPosition = 40,
            group = "CONNECTION"
    )
    public int threadCount;

    @Override
    public String getQuery() {
        return query;
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

    @Override
    public int getNumberOfThreads() {
        return threadCount;
    }
}
