package com.syh.example.dynamicdetector.checker.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class Keyed<K, V> {

    K key;

    V value;
}
