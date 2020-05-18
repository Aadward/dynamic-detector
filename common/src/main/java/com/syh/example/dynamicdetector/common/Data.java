package com.syh.example.dynamicdetector.common;

import com.fasterxml.jackson.core.type.TypeReference;
import com.jayway.jsonpath.JsonPath;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

@lombok.Data
@NoArgsConstructor
public class Data implements Serializable {

    private Map<String, Object> value;

    public Data(Map<String, Object> value) {
        this.value = value;
    }

    public static Data fromJson(String jsonStr) {
        return new Data(JsonUtil.fromJson(jsonStr, new TypeReference<Map<String, Object>>() {
        }));
    }

    public static Data fromMap(Map<String, Object> map) {
        return new Data(map);
    }

    public Object get(String expression) {
        return JsonPath.read(value, expression);
    }
}
