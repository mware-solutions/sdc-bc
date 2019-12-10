package com.mware.stage.lib;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Utils {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    public static void stringToMapRecord(Record record, String input, boolean isJson, String separator) {
        Map<String, Field> map = new HashMap<>();

        if (isJson) {
            parseJson(map, input);
        } else {
            parseWithSeparator(map, input, separator);
        }

        record.set(Field.create(map));
    }

    private static void parseWithSeparator(Map<String, Field> map, String input, String separator) {
        final String[] rsp = input.split(separator != null ? separator : ",");
        for (int i = 0; i < rsp.length; i++) {
            map.put("field-" + i, Field.create(rsp[i]));
        }
    }

    private static void parseJson(Map<String, Field> map, String input) {
        final Gson gson = new Gson();
        JsonObject obj = new JsonParser().parse(input).getAsJsonObject();
        Set<Map.Entry<String, JsonElement>> entries = obj.entrySet();
        JsonValue value;
        String textValue;
        for (Map.Entry<String, JsonElement> entry: entries) {
            textValue = "";
            if (entry.getValue() != null) {
                try {
                    value = gson.fromJson(entry.getValue(), JsonValue.class);
                    if (value != null) {
                        textValue = value.getValue();
                        if (value.getEncoded() && textValue != null) {
                            final byte[] decodedBytes = Base64.getDecoder().decode(textValue);
                            textValue = new String(decodedBytes, "UTF-8");
                        }
                    }
                } catch(Throwable t) {
                    LOG.warn("There was an error while parsing json value " +
                            "(not the conventional JsonValue), with message: " + t.getMessage());
                }
            }
            map.put(entry.getKey(), Field.create(textValue));
        }
    }
}
