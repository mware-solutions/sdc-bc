package com.mware.stage.lib;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.formula.functions.T;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.util.*;
import java.util.stream.Collectors;

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
        for (Map.Entry<String, JsonElement> entry: entries) {
            if (entry.getValue() != null) {
                try {
                    Field value = stringJsonValue(gson, entry.getValue());
                    map.put(entry.getKey(), value);
                } catch (IllegalArgumentException ex) {
                    Field field = listJsonValue(gson, entry.getValue());
                    map.put(entry.getKey(), field);
                } catch (Throwable t) {
                    LOG.error("Unknown JsonValue type: "+entry.getValue().getAsString());
                }
            }
        }
    }

    private static Field stringJsonValue(Gson gson, JsonElement value) throws IllegalArgumentException {
        if (value != null) {
            try {
                Type type = new TypeToken<JsonValue<String>>(){}.getType();
                JsonValue<String> stringValue = gson.fromJson(value, type);
                if(stringValue != null) {
                    decodeValue(stringValue);
                    return Field.create(stringValue.getValue());
                } else
                    return Field.create("");
            } catch (JsonSyntaxException ex) {
                throw new IllegalArgumentException();
            } catch (UnsupportedEncodingException ex) {
                LOG.warn("There was an error while parsing json value " +
                        "(not the conventional JsonValue), with message: " + ex.getMessage());
            }
        }

        return null;
    }

    private static Field listJsonValue(Gson gson, JsonElement value) throws IllegalArgumentException {
        if (value != null) {
            try {
                Type type = new TypeToken<JsonValue<List<Object>>>(){}.getType();
                JsonValue<List<Object>> stringValue = gson.fromJson(value, type);
                if(stringValue != null) {
                    List<Field> values = stringValue.getValue().stream()
                            .map(v -> (v != null) ? Field.create(v.toString()) : Field.create(""))
                            .collect(Collectors.toList());
                    return Field.create(values);
                } else
                    return Field.create(Collections.emptyList());
            } catch (JsonSyntaxException ex) {
//                ex.printStackTrace();
                throw new IllegalArgumentException();
            }
        }

        return null;
    }

    private static JsonValue<String> decodeValue(JsonValue<String> stringValue) throws UnsupportedEncodingException {
        if (stringValue != null) {
            if(stringValue.getEncoded() && !StringUtils.isEmpty(stringValue.getValue())) {
                final byte[] decodedBytes = Base64.getDecoder().decode(stringValue.getValue());
                stringValue.setValue(new String(decodedBytes, "UTF-8"));
            }
        }

        return stringValue;
    }
}
