package com.syh.example.dynamicdetector.common;

import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class Key {

    private long ruleId;

    private Map<String, Object> values;

    public static Key of(long ruleId, String key, Object value) {
        Map<String, Object> map = Maps.newHashMap();
        map.put(key, value);
        return new Key(ruleId, map);
    }
}

