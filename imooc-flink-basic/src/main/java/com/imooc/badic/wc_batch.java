package com.imooc.badic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author fengwentao@yadingdata.com
 * @date 2021/9/29 14:39
 * @Version 1.0.0
 * @Description TODO
 */
public class wc_batch {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.readTextFile("D:\\coder\\learn\\flinklearn20210923\\imooc-flink-basic\\src\\main\\java\\com\\imooc\\data\\wc.txt")
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        String[] words = value.split(",");
                        for (String word : words) {
                            out.collect(word);
                        }
                    }
                })
                .map(new MapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return new Tuple2<>(value,1);
                    }
                })
                .groupBy(0)
                .sum(1)
                .print();
    }
}
