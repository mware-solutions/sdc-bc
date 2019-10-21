/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mware.stage.commit;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;

import java.util.Map;

@StageDef(
    version = 1,
    label = "BigConnect Commiter",
    description = "",
    icon = "bc.png",
    recordsByRef = true,
    onlineHelpRefUrl = ""
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class BigConnectDTarget extends BigConnectTarget {

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.STRING,
          defaultValue = "/opt/bigconnect/config",
          label = "BigConnect Config Path",
          displayPosition = 0,
          group = "OntologyMapping"
  )
  public String configPath;

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.STRING,
          defaultValue = "/",
          label = "Source Field Path",
          displayPosition = 10,
          group = "OntologyMapping"
  )
  public String fieldPath;

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.STRING,
          defaultValue = "document",
          label = "Concept Type",
          displayPosition = 20,
          group = "OntologyMapping"
  )
  public String concept;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MAP,
      defaultValue = "",
      label = "Field Mapping",
      displayPosition = 30,
      group = "OntologyMapping"
  )
  public Map<String, String> mapping;

  public String getConfigPath() {
    return configPath;
  }

  public String getFieldPath() {
    return fieldPath;
  }

  public String getConcept() {
    return concept;
  }

  public Map<String, String> getMapping() {
    return mapping;
  }
}
