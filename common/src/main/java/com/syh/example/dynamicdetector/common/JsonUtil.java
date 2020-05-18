package com.syh.example.dynamicdetector.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.syh.example.dynamicdetector.common.exception.JsonException;

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
