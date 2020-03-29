package com.syh.example.util;

import com.syh.example.exception.JsonException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.PropertyNamingStrategy;

public class JsonUtil {

    private static final ObjectMapper ob = new ObjectMapper();

    static {
        ob.setPropertyNamingStrategy(PropertyNamingStrategy.LOWER_CAMEL_CASE);
    }

    public static <T> T fromJson(String jsonStr, TypeReference<T> typeReference) {
        try {
            return ob.readValue(jsonStr, typeReference);
        } catch (JsonProcessingException e) {
            throw new JsonException("Parse from JSON fail", e);
        }
    }
}
