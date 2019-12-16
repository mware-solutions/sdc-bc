package com.mware.stage.lib;

import com.streamsets.pipeline.api.StageException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.List;

public class PythonRunnable implements Runnable, ExceptionCatcher {
    private static final Logger LOG = LoggerFactory.getLogger(PythonRunnable.class);

    private Runtime runtime;
    private Process process;
    private Exception ex;
    private StreamConsumer errorConsumer;
    private StreamConsumer responseConsumer;

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
            LOG.error("Python process IOE: " + e.getMessage());
            this.destroy();
        } catch(InterruptedException e) {
            final String msg = "Python process was interrupted";
            LOG.error(msg);
            this.ex = new InterruptedException(msg);
            this.destroy();
        } catch(Exception e) {
            LOG.error("Python process generic exception: " + e.getMessage());
            this.destroy();
        }
    }

    private void processErrorsAsync(InputStream is) {
        errorConsumer = new StreamConsumer(is,
                StreamConsumer.StreamType.ERROR,
                new ResponseAction() {
                    @Override
                    public void execute(int index, String responseLine) throws StageException {
                        if (!StringUtils.isEmpty(responseLine)) {
                            throw new StageException(Errors.BC_02, responseLine);
                        }
                    }
                },
                StreamConsumer.ActionType.END, this);
        errorConsumer.start();
    }

    private void processResponseAsync(InputStream is) {
        responseConsumer
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
            for (String param : getParameters()) {
                numParams += param.split(" ").length;
            }
        }
        final String[] cmd = new String[numParams];
        cmd[0] = "python";
        cmd[1] = this.scriptPath;

        // Add script params
        if (getParameters() != null) {
            int idx = 0;
            for (int i = 0; i < getParameters().size(); i++) {
                final String[] subParams = getParameters().get(i).split(" ");
                for (int j =0 ; j < subParams.length; j++) {
                    cmd[2 + idx] = subParams[j];
                    idx++;
                }
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

    public void destroy() {
        if (errorConsumer != null) {
            errorConsumer.kill();
        }
        if (responseConsumer != null) {
            responseConsumer.kill();
        }
        if (process != null) { // Unix only
            Long pid = unixLikeProcessId(process);
            if (pid != null) {
                killProcess(pid.longValue());
            }
            process.destroy();
        }
    }

    private Long unixLikeProcessId(Process process) {
        Class<?> clazz = process.getClass();
        try {
            if (clazz.getName().equals("java.lang.UNIXProcess")) {
                Field pidField = clazz.getDeclaredField("pid");
                pidField.setAccessible(true);
                Object value = pidField.get(process);
                if (value instanceof Integer) {
                    LOG.info("Detected pid: ", value);
                    return ((Integer) value).longValue();
                }
            }
        } catch (SecurityException | IllegalAccessException | IllegalArgumentException | NoSuchFieldException sx) {
            sx.printStackTrace();
        }
        return null;
    }

    public void killProcess(long pid) {
        try {
            runtime.exec("sh -c kill -15 " + pid);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
