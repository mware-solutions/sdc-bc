package com.mware.stage.lib;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

public class PythonRunnable implements Runnable {
    private Runtime runtime;
    private Process process;

    private String scriptPath;
    private List<String> parameters;
    private ResponseAction action;

    public PythonRunnable(String scriptPath) {
        this.scriptPath = scriptPath;
        runtime = Runtime.getRuntime();
    }

    public void runWithCallback(ResponseAction callback) {
        this.action = callback;
        this.run();
    }

    @Override
    public void run() {
        try {
            process = runtime.exec(buildCommand());
            processResponse(process.getInputStream());
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    private void processResponse(InputStream is) throws IOException {
        BufferedReader bfr = new BufferedReader(new InputStreamReader(is));
        String line = "";
        int idx = 0;
        while ((line = bfr.readLine()) != null) {
            if (action != null) {
                action.execute(idx++, line);
            }
        }
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
}
