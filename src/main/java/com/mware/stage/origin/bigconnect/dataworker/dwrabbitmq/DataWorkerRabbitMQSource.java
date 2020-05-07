package com.mware.stage.origin.bigconnect.dataworker.dwrabbitmq;

import com.google.gson.Gson;
import com.mware.bigconnect.pipeline.sdk.ControllerFactory;
import com.mware.bigconnect.pipeline.sdk.config.Config;
import com.mware.bigconnect.pipeline.sdk.exception.ControlException;
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import static com.mware.core.model.workQueue.WorkQueueRepository.DW_DEFAULT_EXTERNAL_QUEUE_NAME;

public abstract class DataWorkerRabbitMQSource extends BasePushSource {
    private final static Logger LOGGER = LoggerFactory.getLogger(DataWorkerRabbitMQSource.class);

    private RabbitMQWorkQueueSpout workerSpout;
    private final TransferQueue<WorkerTuple> tupleQueue = new LinkedTransferQueue<>();

    private List<Thread> processThreads = new ArrayList<>();
    private ExecutorService executor;
    private File pipelineFile;
    private Config pipelineControlConfig;
    private List<String> cachedPipelineList;
    private volatile boolean shouldRun;

    public abstract String getConfigPath();
    public abstract List<String> getPipelines();
    public abstract boolean isFromFile();
    public abstract String getPipelineFilePath();
    public abstract boolean isRestApi();
    public abstract String getPcConfigPath();
    public abstract int getPipelineThreads();

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

        if (isFromFile()) {
            pipelineFile = new File(getPipelineFilePath());
            if (!pipelineFile.exists()) {
                issues.add(
                        getContext().createConfigIssue(
                                Groups.Connection.name(), "config", Errors.BC_00, "" +
                                        "Pipeline list file path is invalid."));
            }
        }
        if (!isRestApi()) {
            File pipelineControlConfigFile = new File(getPcConfigPath());
            if (!pipelineControlConfigFile.exists()) {
                issues.add(
                        getContext().createConfigIssue(
                                Groups.Connection.name(), "config", Errors.BC_00, "" +
                                        "Pipeline control config file path is invalid."));
            } else {
                pipelineControlConfig = new Config(getPcConfigPath());
            }
        }

        executor = Executors.newFixedThreadPool(getPipelineThreads() > 0 ? getPipelineThreads() : Integer.MAX_VALUE);
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
                        LOGGER.error("Could not get next workerItem: " + ex.getMessage());
                    }

                    if (!shouldRun) {
                        return;
                    }
                    if (workerTuple == null) {
                        continue;
                    }

                    try {
                        int failures = 0;
                        for (String pipeline : getPipelineList()) {
                            try {
                                runDWPipeline(pipeline, workerTuple);
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

    private List<String> getPipelineList() {
        if (cachedPipelineList != null) {
            return cachedPipelineList;
        }

        if (!isFromFile()) {
            cachedPipelineList = getPipelines();
        } else {
            cachedPipelineList = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(new FileReader(pipelineFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    cachedPipelineList.add(line);
                }
            } catch(IOException e) {
                LOGGER.trace(e.getMessage());
            }
        }

        return cachedPipelineList;
    }

    private void runDWPipeline(String pipelineName, WorkerTuple workerTuple) throws Exception {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(SdcDataWorkerItem.WORK_PIPELINE_PARAM, Base64.getEncoder().encodeToString(workerTuple.getData()));

        if (isRestApi()) {
            final String apiUrl =
                    getContext().getConfiguration().get("pipeline.control.url", "")
                            + "/high-level/run-pipeline";
            ApiRequest req = new ApiRequest(pipelineName, new Gson().toJson(parameters));
            sendPost(apiUrl, new Gson().toJson(req));
        } else {
            executor.submit(() -> {
                try {
                    ControllerFactory.getInstance(pipelineControlConfig)
                            .getHighLevelController()
                            .runPipeline(pipelineName, parameters, getPipelineThreads() > 0,
                                    false, null, null,
                                    (info) -> {},
                                    (timedOut, stats) ->
                                            LOGGER.trace("Finished. TimedOut: " + timedOut + ". Stats: " + stats));
                } catch (ControlException e) {
                    LOGGER.warn("Pipeline execution failed with exception: ", e);
                } catch (RuntimeException e) {
                    workerSpout.fail(workerTuple);
                    e.printStackTrace();
                }
            });
        }
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
        tupleQueue.clear();
        if (executor != null) {
            executor.shutdownNow();
        }
    }
}
