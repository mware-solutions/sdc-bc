package com.mware.stage.origin.bigconnect.dataworker.dwrabbitmq;

import com.google.gson.Gson;
import com.mware.core.config.Configuration;
import com.mware.core.ingest.WorkerTuple;
import com.mware.core.model.workQueue.RabbitMQWorkQueueSpout;
import com.mware.stage.common.error.Errors;
import com.mware.stage.destination.bigconnect.simple.Groups;
import com.mware.stage.lib.BigConnectSystem;
import com.mware.stage.origin.bigconnect.dataworker.common.SdcDataWorkerItem;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BasePushSource;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;

import static com.mware.core.model.workQueue.WorkQueueRepository.DW_DEFAULT_EXTERNAL_QUEUE_NAME;

public abstract class DataWorkerRabbitMQSource extends BasePushSource {
    private final static Logger LOGGER = LoggerFactory.getLogger(DataWorkerRabbitMQSource.class);

    private RabbitMQWorkQueueSpout workerSpout;
    private final TransferQueue<WorkerTuple> tupleQueue = new LinkedTransferQueue<>();

    private List<Thread> processThreads = new ArrayList<>();
    private volatile boolean shouldRun;

    public abstract String getConfigPath();
    public abstract List<String> getPipelines();

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
            final Configuration config = BigConnectSystem.getStaticConfiguration(getConfigPath());
            workerSpout = new RabbitMQWorkQueueSpout(
                    config.get(Configuration.DW_EXTERNAL_QUEUE_NAME, DW_DEFAULT_EXTERNAL_QUEUE_NAME));
            workerSpout.setConfiguration(config);
            workerSpout.open();
        } catch(Exception e) {
            issues.add(
                    getContext().createConfigIssue(
                            Groups.Connection.name(), "config", Errors.BC_00,
                            "Could not connect to RabbitMQ: " + e.getMessage()));
            LOGGER.error("", e);
        }
        shouldRun = true;

        return issues;
    }

    @Override
    public void produce(Map<String, String> lastOffsets, int maxBatchSize) throws StageException {
        startProcessThread();
        try {
            pollWorkerSpout();
        } catch (InterruptedException e) {
            LOGGER.error("", e);
        }
    }

    private void startProcessThread() {
        for (int i=0; i < getNumberOfThreads(); i++) {
            Thread t = new Thread(() -> {
                while (shouldRun) {
                    if (getContext().isStopped()) {
                        shouldRun = false;
                    }

                    WorkerTuple workerTuple = null;
                    try {
                        workerTuple = tupleQueue.poll(2000, TimeUnit.MILLISECONDS);
                    } catch (Exception ex) {
                        LOGGER.error("Could not get next workerItem: "+ex.getMessage());
                    }

                    if (!shouldRun) {
                        return;
                    }
                    if (workerTuple == null) {
                        continue;
                    }

                    try {
                        int failures = 0;
                        for (String pipeline : getPipelines()) {
                            try {
                                runDWPipeline(pipeline, workerTuple.getData());
                            } catch (Exception e) {
                                LOGGER.error("", e);
                                failures += 1;
                            }
                        }
                        if (failures == 0) {
                            workerSpout.ack(workerTuple);
                        }
                    } catch (Throwable ex) {
                        LOGGER.error("", ex);
                    }
                }
            });
            t.setName(Thread.currentThread().getName() + "-process-"+i);
            t.start();
            processThreads.add(t);
        }
    }

    private void runDWPipeline(String pipelineName, byte[] data) throws Exception {
        // TODO - This should be implemented using pipeline control SDK instead (once server db is used)
        final String apiUrl =
                getContext().getConfiguration().get("pipeline.control.url", "")
                        + "/high-level/run-pipeline";
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(SdcDataWorkerItem.WORK_PIPELINE_PARAM, Base64.getEncoder().encodeToString(data));
        ApiRequest req = new ApiRequest(pipelineName, new Gson().toJson(parameters));
        sendPost(apiUrl, new Gson().toJson(req));
    }

    private void sendPost(String url, String body) throws Exception {
        SSLContextBuilder builder = new SSLContextBuilder();
        builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
        final SSLConnectionSocketFactory sslsf =
                new SSLConnectionSocketFactory(builder.build(), NoopHostnameVerifier.INSTANCE);
        final Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", new PlainConnectionSocketFactory())
                .register("https", sslsf)
                .build();
        final PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager(registry);
        cm.setMaxTotal(100);

        HttpPost post = new HttpPost(url);
        HttpEntity stringEntity = new StringEntity(body, ContentType.APPLICATION_JSON);
        post.setEntity(stringEntity);

        try (CloseableHttpClient httpClient =
                     HttpClients.custom().setSSLSocketFactory(sslsf).setConnectionManager(cm).build();
             CloseableHttpResponse response = httpClient.execute(post)) {
            // Nothing to do here
        }
    }


    private void pollWorkerSpout() throws InterruptedException {
        while (shouldRun) {
            if (getContext().isStopped()) {
                shouldRun = false;
            }

            WorkerTuple tuple = null;
            try {
                tuple = workerSpout.nextTuple();
            } catch (InterruptedException ex) {
                LOGGER.warn("Failed to get next tuple", ex);
            } catch (Exception ex) {
                LOGGER.error("Failed to get next tuple", ex);
                Thread.sleep(10 * 1000); // 10s
                continue;
            }
            if (tuple == null) {
                continue;
            }

            tupleQueue.transfer(tuple);
        }
    }

    @Override
    public void destroy() {
        shouldRun = false;
        for (Thread t : processThreads) {
            try {
                t.join(1000);
            } catch (InterruptedException e) {
                LOGGER.error("", e);
            }

            t.interrupt();
        }
        workerSpout.close();
    }
}
