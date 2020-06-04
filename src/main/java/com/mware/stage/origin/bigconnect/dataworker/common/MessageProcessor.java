package com.mware.stage.origin.bigconnect.dataworker.common;

import com.google.common.collect.ImmutableList;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerMessage;
import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.workQueue.Priority;
import com.mware.ge.*;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.Value;
import com.mware.stage.lib.BigConnectSystem;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.ProtoSource;
import com.streamsets.pipeline.api.Record;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;


public class MessageProcessor {
    private final static Logger LOGGER = LoggerFactory.getLogger(MessageProcessor.class);

    private final BigConnectSystem bigConnectSystem;
    private final ProtoSource.Context context;

    public MessageProcessor(BigConnectSystem bigConnectSystem, ProtoSource.Context context) {
        this.bigConnectSystem = bigConnectSystem;
        this.context = context;
    }

    private SdcDataWorkerItem tupleDataToWorkerItem(byte[] data) {
        DataWorkerMessage message = DataWorkerMessage.create(data);
        return new SdcDataWorkerItem(message, getElements(message), data);
    }

    private ImmutableList<Element> getElements(DataWorkerMessage message) {
        ImmutableList.Builder<Element> results = ImmutableList.builder();
        if (message.getGraphVertexId() != null && message.getGraphVertexId().length > 0) {
            results.addAll(getVerticesFromMessage(message));
        }
//        if (message.getGraphEdgeId() != null && message.getGraphEdgeId().length > 0) {
//            results.addAll(getEdgesFromMessage(message));
//        }
        return results.build();
    }

    private ImmutableList<Element> getVerticesFromMessage(DataWorkerMessage message) {
        ImmutableList.Builder<Element> vertices = ImmutableList.builder();

        for (String vertexId : message.getGraphVertexId()) {
            Vertex vertex;
            if (message.getStatus() == ElementOrPropertyStatus.DELETION ||
                    message.getStatus() == ElementOrPropertyStatus.HIDDEN) {
//                vertex = bigConnectSystem.getGraph().getVertex(
//                        vertexId,
//                        FetchHints.ALL,
//                        message.getBeforeActionTimestamp(),
//                        bigConnectSystem.getAuthorizations()
//                );
                continue;
            } else {
                vertex = bigConnectSystem.getGraph().getVertex(vertexId,
                        FetchHints.PROPERTIES, bigConnectSystem.getAuthorizations());
            }
            if (doesExist(vertex)) {
                vertices.add(vertex);
            } else {
                LOGGER.warn("Could not find vertex with id: " + vertexId);
            }
        }
        return vertices.build();
    }

    private ImmutableList<Element> getEdgesFromMessage(DataWorkerMessage message) {
        ImmutableList.Builder<Element> edges = ImmutableList.builder();

        for (String edgeId : message.getGraphEdgeId()) {
            Edge edge;
            if (message.getStatus() == ElementOrPropertyStatus.DELETION ||
                    message.getStatus() == ElementOrPropertyStatus.HIDDEN) {
                edge = bigConnectSystem.getGraph().getEdge(edgeId, FetchHints.ALL,
                        message.getBeforeActionTimestamp(), bigConnectSystem.getAuthorizations());
            } else {
                edge = bigConnectSystem.getGraph().getEdge(edgeId, FetchHints.ALL, bigConnectSystem.getAuthorizations());
            }
            if (doesExist(edge)) {
                edges.add(edge);
            } else {
                LOGGER.warn("Could not find edge with id: " + edgeId);
            }
        }
        return edges.build();
    }

    private boolean doesExist(Element element) {
        return element != null;
    }

    public List<Record> process(byte[] data) throws Exception {
        SdcDataWorkerItem workerItem = tupleDataToWorkerItem(data);

        DataWorkerMessage message = workerItem.getMessage();
        if (message.getProperties() != null && message.getProperties().length > 0) {
            return safeExecuteHandlePropertiesOnElements(workerItem);
        } else if (message.getPropertyName() != null) {
            return safeExecuteHandlePropertyOnElements(workerItem);
        } else {
            return safeExecuteHandleAllEntireElements(workerItem);
        }
    }

    private List<Record> safeExecuteHandleAllEntireElements(SdcDataWorkerItem workerItem) throws Exception {
        List<Record> records = new ArrayList<>();
        for (Element element : workerItem.getElements()) {
            records.addAll(safeExecuteHandleEntireElement(element, workerItem.getMessage(), workerItem));
        }

        return records;
    }

    private List<Record> safeExecuteHandlePropertiesOnElements(SdcDataWorkerItem workerItem) throws Exception {
        List<Record> records = new ArrayList<>();
        DataWorkerMessage message = workerItem.getMessage();
        for (Element element : workerItem.getElements()) {
            for (DataWorkerMessage.Property propertyMessage : message.getProperties()) {
                Property property = null;
                String propertyKey = propertyMessage.getPropertyKey();
                String propertyName = propertyMessage.getPropertyName();

                if (StringUtils.isNotEmpty(propertyKey) || StringUtils.isNotEmpty(propertyName)) {
                    if (propertyKey == null) {
                        property = element.getProperty(propertyName);
                    } else {
                        property = element.getProperty(propertyKey, propertyName);
                    }

                    if (property == null) {
                        LOGGER.warn(
                                "Could not find property " + propertyKey + ":" + propertyName
                                        + " on vertex with id: " + element.getId());
                        continue;
                    }
                }

                records.add(
                        safeExecuteHandlePropertyOnElement(element, property, message.getWorkspaceId(),
                                message.getVisibilitySource(), message.getPriority(),
                                message.isTraceEnabled(), propertyMessage.getStatus(),
                                propertyMessage.getBeforeActionTimestampOrDefault(),  workerItem));
            }
        }

        return records;
    }

    private List<Record> safeExecuteHandlePropertyOnElements(SdcDataWorkerItem workerItem) throws Exception {
        List<Record> records = new ArrayList<>();
        DataWorkerMessage message = workerItem.getMessage();
        for (Element element : workerItem.getElements()) {
            Property property = getProperty(element, message);

            if (property == null) {
                // Could be property from another element
                LOGGER.warn(
                        "Could not find property " + message.getPropertyKey() + ":"
                                + message.getPropertyName() + " on vertex with id: " + element.getId());
                property = new Property() {
                    @Override
                    public String getKey() {
                        return message.getPropertyKey();
                    }

                    @Override
                    public String getName() {
                        return message.getPropertyName();
                    }

                    @Override
                    public Value getValue() {
                        return null;
                    }

                    @Override
                    public Long getTimestamp() {
                        return null;
                    }

                    @Override
                    public Visibility getVisibility() {
                        return null;
                    }

                    @Override
                    public Metadata getMetadata() {
                        return null;
                    }

                    @Override
                    public FetchHints getFetchHints() {
                        return null;
                    }

                    @Override
                    public Iterable<Visibility> getHiddenVisibilities() {
                        return null;
                    }

                    @Override
                    public boolean isHidden(Authorizations authorizations) {
                        return false;
                    }
                };
            }
            records.add(safeExecuteHandlePropertyOnElement(element, property, message, workerItem));
        }

        return records;
    }

    private Record safeExecuteHandlePropertyOnElement(Element element, Property property, DataWorkerMessage message,
                                                      SdcDataWorkerItem workerItem) throws Exception {
        return safeExecuteHandlePropertyOnElement(
                element,
                property,
                message.getWorkspaceId(),
                message.getVisibilitySource(),
                message.getPriority(),
                message.isTraceEnabled(),
                message.getStatus(),
                message.getBeforeActionTimestampOrDefault(),
                workerItem);
    }

    private Record safeExecuteHandlePropertyOnElement(
            Element element,
            Property property,
            String workspaceId,
            String visibilitySource,
            Priority priority,
            boolean traceEnabled,
            ElementOrPropertyStatus status,
            long beforeActionTimestamp,
            SdcDataWorkerItem workerItem) throws Exception {
        String propertyText = getPropertyText(property);
        boolean workerInterested = isWorkerInterested(element, property, status);
        if (!workerInterested) {
            LOGGER.debug("We are not interested in property: " + propertyText);
            return null;
        }

        DataWorkerData workData = new DataWorkerData(
                bigConnectSystem.getVisibilityTranslator(),
                element,
                property,
                workspaceId,
                visibilitySource,
                priority,
                traceEnabled,
                beforeActionTimestamp,
                status
        );

        LOGGER.trace("Begin work on element: " + element.getId() + ", property: " + propertyText);
        if (property != null && property.getValue() instanceof StreamingPropertyValue) {
            StreamingPropertyValue spb = (StreamingPropertyValue) property.getValue();
            return safeExecuteStreamingPropertyValue(workData, spb, workerItem);
        } else {
            return safeExecuteNonStreamingProperty(workData, workerItem);
        }
    }

    private List<Record> safeExecuteHandleEntireElement(Element element,
                                                  DataWorkerMessage message,
                                                  SdcDataWorkerItem workerItem) throws Exception {
        List<Record> records = new ArrayList<>();
        records.add(safeExecuteHandlePropertyOnElement(element, null, message, workerItem));
        for (Property property : element.getProperties()) {
            records.add(safeExecuteHandlePropertyOnElement(element, property, message, workerItem));
        }

        return records;
    }

    private Record safeExecuteNonStreamingProperty(DataWorkerData workData, SdcDataWorkerItem workerItem) {
        return produceRecord(null, workData, workerItem);
    }

    private Record safeExecuteStreamingPropertyValue(
            DataWorkerData workData,
            StreamingPropertyValue streamingPropertyValue,
            SdcDataWorkerItem workerItem) throws Exception {
        InputStream in = streamingPropertyValue.getInputStream();
        File tempFile = null;
        Record record;
        try {
            tempFile = copyToTempFile(in, workData);
            in = new FileInputStream(tempFile);
            record = produceRecord(IOUtils.toByteArray(in), workData, workerItem);
        } finally {
            if (tempFile != null) {
                if (!tempFile.delete()) {
                    LOGGER.warn("Could not delete temp file: " + tempFile.getAbsolutePath());
                }
            }
            in.close();
        }

        return record;
    }

    private Property getProperty(Element element, DataWorkerMessage message) {
        if (message.getPropertyName() == null) {
            return null;
        }

        Iterable<Property> properties;
        if (message.getPropertyKey() == null) {
            properties = element.getProperties(message.getPropertyName());
        } else {
            properties = element.getProperties(message.getPropertyKey(), message.getPropertyName());
        }

        Property result = null;
        for (Property property : properties) {
            if (message.getWorkspaceId() != null &&
                    property.getVisibility().hasAuthorization(message.getWorkspaceId())) {
                result = property;
            } else if (result == null) {
                result = property;
            }
        }

        return result;
    }

    private String getPropertyText(Property property) {
        return property == null ? "[none]" : (property.getKey() + ":" + property.getName());
    }

    private File copyToTempFile(InputStream in, DataWorkerData workData) throws IOException {
        String fileExt = null;
        String fileName = BcSchema.FILE_NAME.getOnlyPropertyValue(workData.getElement());
        if (fileName != null) {
            fileExt = FilenameUtils.getExtension(fileName);
        }
        if (fileExt == null) {
            fileExt = "data";
        }

        File tempFile = File.createTempFile("dataWorkerBolt", fileExt);
        workData.setLocalFile(tempFile);
        try (OutputStream tempFileOut = new FileOutputStream(tempFile)) {
            IOUtils.copy(in, tempFileOut);
        } finally {
            in.close();

        }

        return tempFile;
    }

    private boolean isWorkerInterested (
            Element element,
            Property property,
            ElementOrPropertyStatus status) {
        if (status == ElementOrPropertyStatus.DELETION) {
            return isDeleteHandled(element, property);
        } else if (status == ElementOrPropertyStatus.HIDDEN) {
            return isHiddenHandled(element, property);
        } else if (status == ElementOrPropertyStatus.UNHIDDEN) {
            return isUnhiddenHandled(element, property);
        } else {
            return isHandled(element, property);
        }
    }

    public boolean isHandled(Element element, Property property) {
        return true;
    }

    public boolean isDeleteHandled(Element element, Property property) {
        return false;
    }

    public boolean isHiddenHandled(Element element, Property property) {
        return false;
    }

    public boolean isUnhiddenHandled(Element element, Property property) {
        return false;
    }

    public Record produceRecord(byte[] spv, DataWorkerData data, SdcDataWorkerItem workerItem) {
        final String rid = "bcc-src-" + UUID.randomUUID().toString();
        Record record = context.createRecord(rid);
        Map<String, Field> row = new HashMap<>();

        row.put("mOrigMessage", Field.create(workerItem.getOrigMessage()));
        row.put("mOrigPriority", Field.create(SdcDataWorkerItem.toRabbitMQPriority(data.getPriority())));
        row.put("mElementType", Field.create(data.getElement().getElementType().name()));
        row.put("mConceptType", Field.create(
                data.getElement() instanceof Vertex ? ((Vertex)data.getElement()).getConceptType() : ""
        ));
        if (data.getProperty() != null) {
            row.put("mPropertyKey", Field.create(data.getProperty().getKey()));
            row.put("mPropertyName", Field.create(data.getProperty().getName()));
        }

        if (spv != null) {
            row.put("mPropertyData", Field.create(spv));
        }

        if (data.getElement() != null) {
            row.put("element", Field.createListMap(createElementMap(data.getElement())));
        }
        record.set(Field.create(row));
        LOGGER.debug("Produced record with id: " + rid);

        return record;
    }

    private LinkedHashMap<String, Field> createElementMap(final Element element) {
        LinkedHashMap<String, Field> map = new LinkedHashMap<>();
        map.put("id", Field.create(element.getId()));
        for (Property prop :  element.getProperties()) {
            if (prop.getValue() != null) {
                LinkedHashMap<String, Field> propertyMap = new LinkedHashMap<>();
                propertyMap.put("key", Field.create(prop.getKey()));
                if (prop.getValue() instanceof StreamingPropertyValue) {
                    StreamingPropertyValue spv = (StreamingPropertyValue) prop.getValue();
                    try {
                        propertyMap.put("value", Field.create(IOUtils.toByteArray(spv.getInputStream())));
                    } catch (IOException e) {
                        e.printStackTrace();
                        continue;
                    }
                } else {
                    propertyMap.put("value", Field.create(prop.getValue().asObjectCopy().toString()));
                }
                map.put(prop.getName(), Field.createListMap(propertyMap));
            }
        }
        return map;
    }
}
