package com.mware.stage.processor.python;

import com.mware.stage.common.group.PythonExecutorGroups;
import com.mware.stage.lib.PythonExecutorOutputStreams;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;

@StageDef(
    version = 1,
    label = "Python Executor [P]",
    description = "",
    icon = "py.png",
    outputStreams = PythonExecutorOutputStreams.class,
    onlineHelpRefUrl = ""
)
@ConfigGroups(PythonExecutorGroups.class)
@GenerateResourceBundle
public class PythonExecutorDProcessor extends PythonExecutorProcessor {

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
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Parameter field",
      displayPosition = 20,
      group = "ExecutorConfig"
  )
  public String paramField;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "JSON output",
      displayPosition = 30,
      group = "ExecutorConfig"
  )
  public boolean json;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Output separator",
      description = "Output separator (only if output is not JSON)",
      dependsOn = "json",
      triggeredByValue = "false",
      displayPosition = 40,
      group = "ExecutorConfig"
  )
  public String outputSeparator;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Target Field",
      description = "Field that stores the result (If not set a new record is created)",
      displayPosition = 50,
      group = "ExecutorConfig"
  )
  public String targetField;

  /** {@inheritDoc} */
  @Override
  public String getScriptPath() {
    return scriptPath;
  }

  @Override
  public String getParamField() {
    return paramField;
  }

  @Override
  public String getOutputSeparator() {
    return outputSeparator;
  }

  @Override
  public String getTargetField() {
    return targetField;
  }

  public String getInterpreterPath() {
    return interpreterPath;
  }
}
