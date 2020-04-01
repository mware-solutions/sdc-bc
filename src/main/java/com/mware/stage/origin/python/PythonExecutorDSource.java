package com.mware.stage.origin.python;

import com.mware.stage.common.group.PythonExecutorGroups;
import com.mware.stage.lib.PythonExecutorOutputStreams;
import com.streamsets.pipeline.api.*;

import java.util.List;

@StageDef(
    version = 1,
    label = "Python Executor [O]",
    description = "",
    icon = "py.png",
    outputStreams = PythonExecutorOutputStreams.class,
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    onlineHelpRefUrl = ""
)
@ConfigGroups(value = PythonExecutorGroups.class)
@GenerateResourceBundle
public class PythonExecutorDSource extends PythonExecutorSource {

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.STRING,
          defaultValue = "python",
          label = "Python interpreter path",
          displayPosition = 10,
          group = "ExecutorConfig"
  )
  public String interpreterPath;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Script path",
      displayPosition = 10,
      group = "ExecutorConfig"
  )
  public String scriptPath;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.LIST,
      defaultValue = "",
      label = "Script parameters",
      displayPosition = 20,
      group = "ExecutorConfig"
  )
  public List<String> parameters;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Script JSON output",
      displayPosition = 30,
      group = "ExecutorConfig"
  )
  public boolean json;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = ",",
      label = "Script output separator",
      description = "Output separator (only if output is not JSON)",
      dependsOn = "json",
      triggeredByValue = "false",
      displayPosition = 40,
      group = "ExecutorConfig"
  )
  public String outputSeparator;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1",
      label = "Thread Count",
      displayPosition = 50,
      group = "ExecutorConfig"
  )
  public int threadCount;

  @Override
  public List<String> getParameters() {
    return parameters;
  }

  /** {@inheritDoc} */
  @Override
  public String getScriptPath() {
    return scriptPath;
  }

  @Override
  public boolean isJson() {
    return json;
  }

  @Override
  public String getOutputSeparator() {
    return outputSeparator;
  }

  @Override
  public int getNumberOfThreads() {
    return threadCount;
  }

  public String getInterpreterPath() {
    return interpreterPath;
  }
}
