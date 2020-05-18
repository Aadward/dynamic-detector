package com.syh.example.dynamicdetector.cmdtool;

import com.syh.example.dynamicdetector.common.Data;
import com.syh.example.dynamicdetector.common.Key;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Queryable state:
 *
 * 1. Flink可以自动生成POJO类的Serializer和Deserializer，状态在程序种最好使用POJO，不然需要自定义Serializer和Deserializer
 * 2. 和Flink中的Class需完全一致（放到common包种公用），因为序列化中写入了类的元数据
 */
public class Main {

    public static void main(String[] args) throws UnknownHostException, InterruptedException, ExecutionException {

        QueryableStateClient client = new QueryableStateClient("192.168.50.6", 9069);
        client.setExecutionConfig(new ExecutionConfig());

        MapStateDescriptor<Long, Set<Data>> windowStateDescriptor =
                new MapStateDescriptor<Long, Set<Data>>("windowDescriptor", Types.LONG,
                        TypeInformation.of(new TypeHint<Set<Data>>() {
                        }));

        Map<String, Object> map = new HashMap<>();
        map.put("$.name", "shen.yuhang");
        CompletableFuture<MapState<Long, Set<Data>>> future = client.getKvState(
                JobID.fromHexString("f9c2b9e87681b5c0d5940ea5f9607540"),
                "window-state",
                new Key(1L, map),
                TypeInformation.of(Key.class),
                windowStateDescriptor
        );

        try {
            MapState<Long, Set<Data>> value = future.get();

            for (Set<Data> data : value.values()) {
                System.out.println(data);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
