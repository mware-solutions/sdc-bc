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
package com.mware.stage.destination.bigconnect.simple;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;

import java.util.Map;

@StageDef(
    version = 1,
    label = "BigConnect Graph Engine Destination",
    description = "Load data directly into the BigConnect Graph Engine",
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
          type = ConfigDef.Type.STRING,
          defaultValue = "1",
          label = "ID Seed",
          displayPosition = 30,
          group = "OntologyMapping"
  )
  public String idSeed;

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.STRING,
          defaultValue = "*",
          label = "ID fields",
          displayPosition = 40,
          group = "OntologyMapping"
  )
  public String idFields;

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.BOOLEAN,
          defaultValue = "false",
          label = "Create Relationship",
          displayPosition = 50,
          group = "OntologyMapping"
  )
  public boolean createRelationship;

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.STRING,
          defaultValue = "hasEntity",
          label = "Relationship Name",
          displayPosition = 60,
          group = "OntologyMapping"
  )
  public String relationshipName;

  @ConfigDef(
          required = false,
          type = ConfigDef.Type.STRING,
          defaultValue = "1",
          label = "Relationship End ID Seed",
          displayPosition = 70,
          group = "OntologyMapping"
  )
  public String relIdSeed;

  @ConfigDef(
          required = false,
          type = ConfigDef.Type.STRING,
          defaultValue = "",
          label = "Fixed Relationship End ID",
          description = "Other relationship end ID (Relationship End ID Seed will be ignored if this is configured)",
          displayPosition = 80,
          group = "OntologyMapping"
  )
  public String fixedRelId;

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.BOOLEAN,
          defaultValue = "true",
          label = "Relationship Source",
          displayPosition = 90,
          group = "OntologyMapping"
  )
  public boolean relSource;

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.BOOLEAN,
          defaultValue = "true",
          label = "Push to work queue",
          displayPosition = 100,
          group = "OntologyMapping"
  )
  public boolean workQueue;

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.MAP,
          defaultValue = "",
          label = "Field Mapping",
          displayPosition = 110,
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

  public String getIdSeed() {
    return idSeed;
  }

  public String getIdFields() {
    return idFields;
  }

  public boolean isCreateRelationship() {
    return createRelationship;
  }

  public String getRelationshipName() {
    return relationshipName;
  }

  public String getRelIdSeed() {
    return relIdSeed;
  }

  public String getFixedRelId() {
    return fixedRelId;
  }

  public boolean isRelSource() {
    return relSource;
  }

  public boolean isWorkQueue() {
    return workQueue;
  }

  public Map<String, String> getMapping() {
    return mapping;
  }
}
