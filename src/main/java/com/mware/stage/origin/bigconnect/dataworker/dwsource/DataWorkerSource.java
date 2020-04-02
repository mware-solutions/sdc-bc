package com.mware.stage.origin.bigconnect.dataworker.dwsource;

import com.google.common.collect.ImmutableList;
import com.mware.core.ingest.dataworker.DataWorkerMessage;
import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.ge.Edge;
import com.mware.ge.Element;
import com.mware.ge.FetchHints;
import com.mware.ge.Vertex;
import com.mware.stage.common.error.Errors;
import com.mware.stage.destination.bigconnect.simple.Groups;
import com.mware.stage.lib.BigConnectSystem;
import com.mware.stage.origin.bigconnect.dataworker.common.SdcDataWorkerItem;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.util.Base64;
import java.util.List;

public abstract class DataWorkerSource extends BaseSource {
    private final static Logger LOGGER = LoggerFactory.getLogger(DataWorkerSource.class);

    public abstract String getConfigPath();

    private BigConnectSystem bigConnect;
    private MessageProcessor messageProcessor;

    @Override
    protected List<ConfigIssue> init() {
        // Validate configuration values and open any required resources.
        List<ConfigIssue> issues = super.init();
        File configDir = new File(getConfigPath());
        if (!configDir.exists() || !configDir.isDirectory()) {
            issues.add(
                    getContext().createConfigIssue(
                            Groups.Connection.name(), "config", Errors.BC_00, "" +
                                    "BigConnect Config path does not exist or it's not a directory."));
        }
        try {
            bigConnect = BigConnectSystem.getInstance();
            bigConnect.init(getConfigPath());
            messageProcessor = new MessageProcessor(bigConnect, getContext());
        } catch (Exception e) {
            issues.add(
                    getContext().createConfigIssue(
                            Groups.Connection.name(), "config", Errors.BC_00,
                            "Could not connect to BigConnect Graph Engine: " + e.getMessage()));
            LOGGER.error("", e);
        }

        return issues;
    }

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        // Get work message from pipeline's parameters
        final String message = (String)getContext().createELVars().getConstant(SdcDataWorkerItem.WORK_PIPELINE_PARAM);
        byte[] workMessage = Base64.getDecoder().decode(message);

        List<Record> records = null;
        try {
            records = messageProcessor.process(tupleDataToWorkerItem(workMessage));
        } catch (Exception e) {
            LOGGER.error("Couldn't produce record from work message", e);
        }
        if (records != null) {
            for (Record record : records) {
                batchMaker.addRecord(record);
            }
        }

        return  "N/A";
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
        if (message.getGraphEdgeId() != null && message.getGraphEdgeId().length > 0) {
            results.addAll(getEdgesFromMessage(message));
        }
        return results.build();
    }

    private ImmutableList<Element> getVerticesFromMessage(DataWorkerMessage message) {
        ImmutableList.Builder<Element> vertices = ImmutableList.builder();

        for (String vertexId : message.getGraphVertexId()) {
            Vertex vertex;
            if (message.getStatus() == ElementOrPropertyStatus.DELETION ||
                    message.getStatus() == ElementOrPropertyStatus.HIDDEN) {
                vertex = bigConnect.getGraph().getVertex(
                        vertexId,
                        FetchHints.ALL,
                        message.getBeforeActionTimestamp(),
                        bigConnect.getAuthorizations()
                );
            } else {
                vertex = bigConnect.getGraph().getVertex(vertexId, FetchHints.ALL, bigConnect.getAuthorizations());
            }
            if (doesExist(vertex)) {
                vertices.add(vertex);
            } else {
                LOGGER.warn("Could not find vertex with id %s", vertexId);
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
                edge = bigConnect.getGraph().getEdge(edgeId, FetchHints.ALL,
                        message.getBeforeActionTimestamp(), bigConnect.getAuthorizations());
            } else {
                edge = bigConnect.getGraph().getEdge(edgeId, FetchHints.ALL, bigConnect.getAuthorizations());
            }
            if (doesExist(edge)) {
                edges.add(edge);
            } else {
                LOGGER.warn("Could not find edge with id %s", edgeId);
            }
        }
        return edges.build();
    }

    private boolean doesExist(Element element) {
        return element != null;
    }

    @Override
    public void destroy() {
        if (bigConnect != null && bigConnect.getGraph() != null) {
            bigConnect.shutDown();
        }
    }
}
