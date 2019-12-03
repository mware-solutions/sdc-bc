package com.mware.stage.lib;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;

import java.util.HashMap;
import java.util.Map;

public class Utils {
    public static void stringToMapRecord(Record record, String input, String separator) {
        Map<String, Field> map = new HashMap<>();

        final String[] rsp = input.split(separator != null ? separator : ",");
        for (int i = 0; i < rsp.length; i++) {
            map.put("field-" + i, Field.create(rsp[i]));
        }

        record.set(Field.create(map));
    }
}
