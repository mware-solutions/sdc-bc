package com.mware.stage.origin.bigconnect.dataworker.common;

import com.google.common.collect.ImmutableList;
import com.mware.core.ingest.dataworker.DataWorkerItem;
import com.mware.core.ingest.dataworker.DataWorkerMessage;
import com.mware.core.model.workQueue.Priority;
import com.mware.ge.Element;

public class SdcDataWorkerItem extends DataWorkerItem {
    public static final String WORK_PIPELINE_PARAM = "dwWork";

    private byte[] origMessage;

    public SdcDataWorkerItem(DataWorkerMessage message, ImmutableList<Element> elements, byte[] origMessage) {
        super(message, elements);
        this.origMessage = origMessage;
    }

    public byte[] getOrigMessage() {
        return origMessage;
    }

    public static Integer toRabbitMQPriority(Priority priority) {
        switch (priority) {
            case HIGH:
                return 2;
            case NORMAL:
                return 1;
            case LOW:
                return 0;
            default:
                return 0;
        }
    }
}
