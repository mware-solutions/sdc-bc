package com.mware.stage.destination.bigconnect.simple;

import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.types.PropertyMetadata;
import com.mware.core.model.properties.types.SingleValueBcProperty;
import com.mware.core.model.schema.Schema;
import com.mware.core.model.schema.SchemaProperty;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.model.workQueue.Priority;
import com.mware.ge.*;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.property.DefaultStreamingPropertyValue;
import com.mware.ge.property.StreamingPropertyValue;
import com.mware.stage.common.error.Errors;
import com.mware.stage.lib.BigConnectSystem;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.lang.StringUtils;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * This target is an example and does not actually write to any destination.
 */
public abstract class BigConnectTarget extends BaseTarget {

  public abstract String getConfigPath();
  public abstract String getFieldPath();
  public abstract String getConcept();
  public abstract String getIdSeed();
  public abstract String getIdFields();
  public abstract boolean isCreateRelationship();
  public abstract String getRelationshipName();
  public abstract String getRelIdSeed();
  public abstract String getFixedRelId();
  public abstract boolean isRelSource();
  public abstract boolean isWorkQueue();
  public abstract Map<String, String> getMapping();

  private BigConnectSystem bigConnect;
  private List<ElementMutation<? extends Element>> elements;

  /** {@inheritDoc} */
  @Override
  protected List<ConfigIssue> init() {
    // Validate configuration values and open any required resources.
    List<ConfigIssue> issues = super.init();
    File configDir = new File(getConfigPath());
    if (!configDir.exists() || !configDir.isDirectory()) {
      issues.add(
          getContext().createConfigIssue(
                  Groups.OntologyMapping.name(), "config", Errors.BC_00, "BigConnect Graph Engine config path does not exist or it's not a directory."
          )
      );
    }

    bigConnect = BigConnectSystem.getInstance();
    try {
        bigConnect.init(getConfigPath());
    } catch(Exception e) {
        issues.add(
                getContext().createConfigIssue(
                        Groups.Connection.name(), "config", Errors.BC_00, "Could not connect to BigConnect Graph Engine: "+e.getMessage()
                )
        );
        e.printStackTrace();
    }
    elements = new ArrayList<>();

    return issues;
  }

  /** {@inheritDoc} */
  @Override
  public void destroy() {
    // Clean up any open resources.
    super.destroy();
  }

  /** {@inheritDoc} */
  @Override
  public void write(Batch batch) throws StageException {
    Iterator<Record> batchIterator = batch.getRecords();

    while (batchIterator.hasNext()) {
      Record record = batchIterator.next();
      try {
        write(record);
      } catch (Exception e) {
        switch (getContext().getOnErrorRecord()) {
          case DISCARD:
            e.printStackTrace();
            break;
          case TO_ERROR:
            e.printStackTrace();
            getContext().toError(record, Errors.BC_01, e);
            break;
          case STOP_PIPELINE:
            e.printStackTrace();
            throw new StageException(Errors.BC_01, e);
          default:
            throw new IllegalStateException(
                Utils.format("Unknown OnError value '{}'", getContext().getOnErrorRecord(), e)
            );
        }
      }
    }
    saveBatch();
  }

  /**
   * Writes a single record to the destination.
   *
   * @param record the record to write to the destination.
   * @throws OnRecordErrorException when a record cannot be written.
   */
  private void write(Record record) throws OnRecordErrorException {
    if (!record.has(getFieldPath())) {
      throw new OnRecordErrorException(Errors.BC_01, record, "Field path " + getFieldPath() + " was not found.");
    }

    final String vertexId = createVertex(record);
    if (isCreateRelationship()) {
        createEdge(record, vertexId);
    }
  }

  private String createVertex(Record record) {
      Field sourceField = record.get(getFieldPath());
      LinkedHashMap<String, Field> fields = sourceField.getValueAsListMap();

      BigConnectSystem bigConnect = BigConnectSystem.getInstance();
      Graph graph = bigConnect.getGraph();
      Schema ontology = bigConnect.getOntology(BigConnectSystem.extractWorkspaceId(SchemaRepository.PUBLIC));
      Visibility visibility = bigConnect.getVisibilityTranslator().getDefaultVisibility();
      VisibilityJson visibilityJson = new VisibilityJson(visibility.getVisibilityString());
      PropertyMetadata propertyMetadata = new PropertyMetadata(new Date(), bigConnect.getSystemUser(), 0d, visibilityJson, visibility);
      final String vertexId = generateId(fields, getIdSeed());

      ElementMutation<Vertex> vb = graph.prepareVertex(vertexId, visibility, getConcept());
      setPropertyValue(BcSchema.VISIBILITY_JSON, vb, visibilityJson, propertyMetadata, visibility);
      setPropertyValue(BcSchema.MODIFIED_DATE, vb, new Date(), propertyMetadata, visibility);
      setPropertyValue(BcSchema.MODIFIED_BY, vb, bigConnect.getSystemUser().getUsername(), propertyMetadata, visibility);

      for (Map.Entry<String, Field> field : fields.entrySet()) {
          if (!getMapping().containsKey(field.getKey())) {
              // Ignore fields that were not mapped
              continue;
          }

          final String propertyName = getMapping().get(field.getKey());
          Metadata metadata = propertyMetadata.createMetadata();
          BcSchema.VISIBILITY_JSON_METADATA.setMetadata(metadata, visibilityJson, visibility);
          if (BcSchema.RAW.isSameName(propertyName)) {
              String str = field.getValue().getValueAsString();
              StreamingPropertyValue rawValue = null;
              try {
                  rawValue = new DefaultStreamingPropertyValue(
                          new ByteArrayInputStream(str.getBytes("UTF-8")), byte[].class);
                  rawValue.searchIndex(false);
                  vb.addPropertyValue("", propertyName, rawValue, metadata, visibility);
              } catch (UnsupportedEncodingException e) {
                  e.printStackTrace();
              }
          } else {
              Object value = castToBcValueType(propertyName, field.getValue(), ontology);
              vb.addPropertyValue("", propertyName, value, metadata, visibility);
          }
      }
      elements.add(vb);

      return vertexId;
  }

  private void createEdge(Record record, String vertexId) {
      final Field sourceField = record.get(getFieldPath());
      final LinkedHashMap<String, Field> fields = sourceField.getValueAsListMap();
      final Visibility defaultVisibility = Visibility.EMPTY;

      String otherVertexId = getFixedRelId();
      if (StringUtils.isEmpty(otherVertexId)) {
          otherVertexId = generateId(fields, getRelIdSeed());
      }

      EdgeBuilderByVertexId eb;
      if (isRelSource()) {
          eb = bigConnect.getGraph().prepareEdge(vertexId, otherVertexId, getRelationshipName(), defaultVisibility);
      } else {
          eb = bigConnect.getGraph().prepareEdge(otherVertexId, vertexId, getRelationshipName(), defaultVisibility);
      }
//      BcSchema.CONCEPT_TYPE.setProperty(eb, SchemaRepository.TYPE_RELATIONSHIP, defaultVisibility);
      BcSchema.MODIFIED_DATE.setProperty(eb, new Date(), defaultVisibility);

      elements.add(eb);
  }

  private void saveBatch() {
      Iterable<Element> savedElements = bigConnect.getGraph().saveElementMutations(elements, bigConnect.getAuthorizations());
      if (isWorkQueue()) {
          bigConnect.getWorkQueueRepository().pushMultipleGraphPropertyQueue(
                  savedElements,
                  null,
                  null,
                  null,
                  null,
                  Priority.HIGH,
                  null,
                  null
          );
      }
      bigConnect.getGraph().flush();
      elements.clear();
  }

  private String generateId(LinkedHashMap<String, Field> fields, String seed) {
    MessageDigest md = null;
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
    final List<String> idFields = Arrays.asList(getIdFields().split(","));
    for (Map.Entry<String, Field> field : fields.entrySet()) {
        if (getIdFields().equals("*")
                        || idFields.contains(field.getKey())) {
            md.update(field.getValue().toString().getBytes());
        }
    }
    md.update(seed.getBytes());
    byte[] digest = md.digest();

    return DatatypeConverter.printHexBinary(digest);
  }

  private void setPropertyValue(SingleValueBcProperty property, ElementMutation<Vertex> m, Object value,
                                PropertyMetadata propertyMetadata, Visibility vertexVisibility) {
    Metadata metadata = propertyMetadata.createMetadata();
    property.setProperty(m, value, metadata, vertexVisibility);
  }

  private Object castToBcValueType(String propertyName, Field field, Schema ontology) {
    SchemaProperty ontologyProperty = ontology.getPropertyByName(propertyName);
    switch(ontologyProperty.getDataType()) {
      case DATE:
        return field.getValueAsDate();
      case DOUBLE:
        return field.getValueAsDouble();
      case INTEGER:
        return field.getValueAsInteger();
      case BOOLEAN:
        return field.getValueAsBoolean();
      default:
        return field.getValueAsString();
    }
  }

}
