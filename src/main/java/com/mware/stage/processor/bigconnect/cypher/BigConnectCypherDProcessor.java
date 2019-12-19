package com.mware.stage.processor.bigconnect.cypher;

import com.mware.stage.destination.bigconnect.cypher.Groups;
import com.mware.stage.lib.PythonExecutorOutputStreams;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.credential.CredentialValue;

import java.util.Map;

@StageDef(
    version = 1,
    label = "BigConnect Cypher Processor",
    description = "Execute Cypher queries against a BigConnect Bolt server [P]",
    icon = "bc.png",
    onlineHelpRefUrl = ""
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
@PipelineLifecycleStage
public class BigConnectCypherDProcessor extends BigConnectCypherProcessor {

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

  public String getQuery() {
    return query;
  }

  public Map<String, String> getQueryParams() {
    return queryParams;
  }

  public String getConnectionString() {
    return connectionString;
  }

  public CredentialValue getUsername() {
    return username;
  }

  public CredentialValue getPassword() {
    return password;
  }
}