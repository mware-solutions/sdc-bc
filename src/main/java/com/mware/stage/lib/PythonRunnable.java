package com.mware.stage.lib;

import com.streamsets.pipeline.api.StageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class PythonRunnable implements Runnable, ExceptionCatcher {
    private static final Logger LOG = LoggerFactory.getLogger(PythonRunnable.class);

    private Runtime runtime;
    private Process process;
    private Exception ex;

    private String scriptPath;
    private List<String> parameters;
    private ResponseAction action;

    public PythonRunnable(String scriptPath) {
        this.scriptPath = scriptPath;
        runtime = Runtime.getRuntime();
    }

    public Exception runWithCallback(ResponseAction callback) {
        this.action = callback;
        this.run();
        return this.ex;
    }

    @Override
    public void run() {
        try {
            process = runtime.exec(buildCommand());

            processErrorsAsync(process.getErrorStream());
            processResponseAsync(process.getInputStream());

            int exitVal = process.waitFor();
            LOG.info("Python process exited with value: " + exitVal);
        } catch(IOException e) {
            e.printStackTrace();
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void processErrorsAsync(InputStream is) {
        StreamConsumer errorConsumer = new StreamConsumer(is,
                StreamConsumer.StreamType.ERROR,
                new ResponseAction() {
                    @Override
                    public void execute(int index, String responseLine) throws StageException {
                        throw new StageException(Errors.BC_02, responseLine);
                    }
                },
                StreamConsumer.ActionType.END, this);
        errorConsumer.start();
    }

    private void processResponseAsync(InputStream is) {
        StreamConsumer responseConsumer
                = new StreamConsumer(is, StreamConsumer.StreamType.OUTPUT, action,
                                    StreamConsumer.ActionType.INLINE, this);
        responseConsumer.start();
    }

    private String[] buildCommand() {
        if (this.scriptPath == null) {
            throw new RuntimeException("Python script path must not be null.");
        }

        int numParams = 2;
        if (getParameters() != null && !getParameters().isEmpty()) {
            numParams += getParameters().size();
        }
        final String[] cmd = new String[numParams];
        cmd[0] = "python";
        cmd[1] = this.scriptPath;

        // Add script params
        if (getParameters() != null) {
            for (int i = 0; i < getParameters().size(); i++) {
                cmd[2 + i] = getParameters().get(i);
            }
        }

        return cmd;
    }

    public List<String> getParameters() {
        return parameters;
    }

    public void setParameters(List<String> parameters) {
        this.parameters = parameters;
    }

    @Override
    public void handleException(Exception e) {
        this.ex = e;
    }
}
