package com.mware.stage.origin.bigconnect.cypher;

import com.mware.stage.lib.PythonExecutorOutputStreams;
import com.streamsets.pipeline.api.*;

import java.util.List;

@StageDef(
    version = 1,
    label = "BigConnect Cypher Source",
    description = "Execute Cypher queries against a BigConnect Bolt server [O]",
    icon = "py.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    onlineHelpRefUrl = ""
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class BigConnectCypherDSource extends BigConnectCypherSource {
  @Override
  public int getNumberOfThreads() {
    return 0;
  }

  //TODO
}
