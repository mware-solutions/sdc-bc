package com.mware.stage.origin.bigconnect.rabbitmq;

import com.google.common.collect.ImmutableList;
import com.mware.core.config.Configuration;
import com.mware.core.ingest.WorkerTuple;
import com.mware.core.ingest.dataworker.DataWorkerMessage;
import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.model.workQueue.RabbitMQWorkQueueSpout;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.ge.Edge;
import com.mware.ge.Element;
import com.mware.ge.FetchHints;
import com.mware.ge.Vertex;
import com.mware.stage.common.error.Errors;
import com.mware.stage.destination.bigconnect.simple.Groups;
import com.mware.stage.lib.BigConnectSystem;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BasePushSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;

public abstract class BigConnectRabbitMqSource extends BasePushSource {
    private final static Logger LOGGER = LoggerFactory.getLogger(BigConnectRabbitMqSource.class);
    private BigConnectSystem bigConnect;
    private RabbitMQWorkQueueSpout workerSpout;
    private volatile boolean shouldRun;
    private final TransferQueue<WorkerItemWrapper> tupleQueue = new LinkedTransferQueue<>();
    private List<Thread> processThreads = new ArrayList<>();
    private QueueMessageProcessor messageProcessor;

    public abstract String getConfigPath();

    @Override
    protected List<ConfigIssue> init() {
        // Validate configuration values and open any required resources.
        List<ConfigIssue> issues = super.init();
        File configDir = new File(getConfigPath());
        if (!configDir.exists() || !configDir.isDirectory()) {
            issues.add(
                    getContext().createConfigIssue(
                            Groups.OntologyMapping.name(), "config", Errors.BC_00, "BigConnect Config path does not exist or it's not a directory."
                    )
            );
        }

        try {
            bigConnect = BigConnectSystem.getInstance();
            bigConnect.init(getConfigPath());
            messageProcessor = new QueueMessageProcessor(bigConnect, getContext());
        } catch(Exception e) {
            issues.add(
                    getContext().createConfigIssue(
                            Groups.Connection.name(), "config", Errors.BC_00, "Could not connect to BigConnect Graph Engine: "+e.getMessage()
                    )
            );
            e.printStackTrace();
        }

        try {
            workerSpout = new RabbitMQWorkQueueSpout(bigConnect.getConfiguration().get(Configuration.DW_EXTERNAL_QUEUE_NAME, WorkQueueRepository.DW_DEFAULT_EXTERNAL_QUEUE_NAME));
            workerSpout.setConfiguration(bigConnect.getConfiguration());
            workerSpout.open();
        } catch(Exception e) {
            issues.add(
                    getContext().createConfigIssue(
                            Groups.Connection.name(), "config", Errors.BC_00, "Could not connect to RabbitMQ: "+e.getMessage()
                    )
            );
            e.printStackTrace();
        }

        shouldRun = true;

        return issues;
    }

    protected SdcDataWorkerItem tupleDataToWorkerItem(byte[] data) {
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
            if (message.getStatus() == ElementOrPropertyStatus.DELETION || message.getStatus() == ElementOrPropertyStatus.HIDDEN) {
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
            if (message.getStatus() == ElementOrPropertyStatus.DELETION || message.getStatus() == ElementOrPropertyStatus.HIDDEN) {
                edge = bigConnect.getGraph().getEdge(edgeId, FetchHints.ALL, message.getBeforeActionTimestamp(), bigConnect.getAuthorizations());
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
    public void produce(Map<String, String> lastOffsets, int maxBatchSize) throws StageException {
        startProcessThread();

        try {
            pollWorkerSpout();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void startProcessThread() {
        for (int i=0; i < getNumberOfThreads(); i++) {
            Thread t = new Thread(() -> {
                while (shouldRun) {
                    if (getContext().isStopped()) {
                        shouldRun = false;
                    }

                    WorkerItemWrapper workerItemWrapper = null;
                    try {
                        workerItemWrapper = tupleQueue.poll(2000, TimeUnit.MILLISECONDS);
                    } catch (Exception ex) {
                        LOGGER.error("Could not get next workerItem: "+ex.getMessage());
                    }
                    if (!shouldRun) {
                        return;
                    }

                    if (workerItemWrapper == null)
                        continue;

                    try {
                        messageProcessor.process(workerItemWrapper.getWorkerItem());
                        workerSpout.ack(workerItemWrapper.workerTuple);
                    } catch (Throwable ex) {
                        ex.printStackTrace();
                    }
                }
            });
            t.setName(Thread.currentThread().getName() + "-process-"+i);
            t.start();
            processThreads.add(t);
        }
    }

    private void pollWorkerSpout() throws InterruptedException {
        while (shouldRun) {
            if (getContext().isStopped()) {
                shouldRun = false;
            }

            WorkerItemWrapper workerItemWrapper = null;
            WorkerTuple tuple = null;
            try {
                tuple = workerSpout.nextTuple();
                if (tuple != null) {
                    SdcDataWorkerItem workerItem = tupleDataToWorkerItem(tuple.getData());
                    workerItemWrapper = new WorkerItemWrapper(workerItem, tuple);
                }
            } catch (InterruptedException ex) {
                // do nothing
            } catch (Exception ex) {
                LOGGER.error("Failed to get next tuple", ex);
                Thread.sleep(10 * 1000);
                continue;
            }
            if (workerItemWrapper == null) {
                continue;
            }


            tupleQueue.transfer(workerItemWrapper);
        }
    }

    @Override
    public void destroy() {
        shouldRun = false;

        for (Thread t : processThreads) {
            try {
                t.join(1000);
            } catch (InterruptedException e) {
            }

            t.interrupt();
        }

        if (workerSpout != null)
            workerSpout.close();

        if (bigConnect != null)
            bigConnect.shutDown();
    }

    private static class WorkerItemWrapper {
        private final SdcDataWorkerItem workerItem;
        private final WorkerTuple workerTuple;

        public WorkerItemWrapper(SdcDataWorkerItem workerItem, WorkerTuple workerTuple) {
            this.workerItem = workerItem;
            this.workerTuple = workerTuple;
        }

        public Long getMessageId() {
            return (Long) workerTuple.getMessageId();
        }

        public WorkerTuple getWorkerTuple() {
            return workerTuple;
        }

        public SdcDataWorkerItem getWorkerItem() {
            return workerItem;
        }

        @Override
        public String toString() {
            return "WorkerItemWrapper{" +
                    "messageId=" + getMessageId() +
                    ", workerItem=" + workerItem +
                    '}';
        }
    }
}
