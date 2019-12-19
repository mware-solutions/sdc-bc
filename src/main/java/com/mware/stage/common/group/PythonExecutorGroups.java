package com.mware.stage.common.group;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum PythonExecutorGroups implements Label {
  ExecutorConfig("Executor config"),
  ;

  private final String label;

  private PythonExecutorGroups(String label) {
    this.label = label;
  }

  /** {@inheritDoc} */
  @Override
  public String getLabel() {
    return this.label;
  }
}