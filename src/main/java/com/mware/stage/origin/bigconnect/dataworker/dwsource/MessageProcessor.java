package com.mware.stage.origin.bigconnect.dataworker.dwsource;

import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerMessage;
import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.workQueue.Priority;
import com.mware.ge.Element;
import com.mware.ge.Property;
import com.mware.ge.Vertex;
import com.mware.ge.property.StreamingPropertyValue;
import com.mware.stage.lib.BigConnectSystem;
import com.mware.stage.origin.bigconnect.dataworker.common.SdcDataWorkerItem;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
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
    private final Source.Context context;

    public MessageProcessor(BigConnectSystem bigConnectSystem, Source.Context context) {
        this.bigConnectSystem = bigConnectSystem;
        this.context = context;
    }

    public List<Record> process(SdcDataWorkerItem workerItem) throws Exception {
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
                        LOGGER.error(
                                "Could not find property [%s]:[%s] on vertex with id %s",
                                propertyKey, propertyName, element.getId());
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

            if (property != null) {
                records.add(safeExecuteHandlePropertyOnElement(element, property, message, workerItem));
            } else {
                LOGGER.error(
                        "Could not find property [%s]:[%s] on vertex with id %s",
                        message.getPropertyKey(), message.getPropertyName(), element.getId());
            }
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
            LOGGER.debug(
                    "We are not interested in %s %s property %s (%s)",
                    element instanceof Vertex ? "vertex" : "edge", element.getId(), propertyText, status);
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

        LOGGER.debug("Begin work on element %s property %s", element.getId(), propertyText);
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
                    LOGGER.warn("Could not delete temp file %s", tempFile.getAbsolutePath());
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
            if (message.getWorkspaceId() != null && property.getVisibility().hasAuthorization(message.getWorkspaceId())) {
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
                    propertyMap.put("value", Field.create(prop.getValue().toString()));
                }
                map.put(prop.getName(), Field.createListMap(propertyMap));
            }
        }
        return map;
    }
}
