package com.mware.stage.lib;

import com.streamsets.pipeline.api.StageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class StreamConsumer extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(StreamConsumer.class);

    private InputStream is;
    private StreamType type;
    private ResponseAction action;
    private ActionType actionType;
    private ExceptionCatcher exceptionCatcher;
    private boolean stopped;

    StreamConsumer(InputStream is, StreamType type, ResponseAction action,
                   ActionType actionType, ExceptionCatcher exceptionCatcher) {
        this.is = is;
        this.type = type;
        this.action = action;
        this.actionType = actionType;
        this.exceptionCatcher = exceptionCatcher;
        this.stopped = false;
    }

    public void run() {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(is));
            String line = null;
            int idx = 0;
            StringBuffer sb = new StringBuffer();
            while (!stopped && ((line = br.readLine()) != null)) {
                LOG.info("Process output - " + type.name() + ": " + line);
                sb.append(line);
                sb.append("\n");
                if (action != null && actionType == ActionType.INLINE) {
                    action.execute(idx++, line);
                }
            }
            if (!stopped && action != null && actionType == ActionType.END) {
                action.execute(-1, sb.toString());
            }
        } catch (IOException e) {
            LOG.error("Source process has been stopped");
        } catch (StageException e) {
            this.exceptionCatcher.handleException(e);
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    public void kill() {
        this.stopped = true;
    }

    enum StreamType {
        ERROR,
        OUTPUT
    }

    enum ActionType {
        INLINE,
        END
    }
}
