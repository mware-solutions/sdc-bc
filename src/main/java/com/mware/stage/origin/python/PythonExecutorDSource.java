package com.mware.stage.origin.python;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;

import java.util.List;

@StageDef(
    version = 1,
    label = "Python Executor [E]",
    description = "",
    icon = "py.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    onlineHelpRefUrl = ""
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class PythonExecutorDSource extends PythonExecutorSource {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Python script path",
      displayPosition = 10,
      group = "ExecutorConfig"
  )
  public String scriptPath;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.LIST,
      defaultValue = "",
      label = "Python script parameters",
      displayPosition = 20,
      group = "ExecutorConfig"
  )
  public List<String> parameters;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = ",",
      label = "Output separator",
      displayPosition = 30,
      group = "ExecutorConfig"
  )
  public String outputSeparator;

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
  public String getOutputSeparator() {
    return outputSeparator;
  }
}
