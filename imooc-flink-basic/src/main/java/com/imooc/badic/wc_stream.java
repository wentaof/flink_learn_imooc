package com.imooc.badic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @author fengwentao@yadingdata.com
 * @date 2021/9/29 13:59
 * @Version 1.0.0
 * @Description TODO
 */
public class wc_stream {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socket_ds = env.socketTextStream("localhost", 9527);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socket_ds.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                String[] words = line.split(",");
                for (String word : words) {
                    out.collect(word);
                }

            }
        })
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return new Tuple2(value, 1);
                    }
                })
                //.keyBy(x -> x.f0)   //lambda表达式写法
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .sum(1);

        sum.print();

        //System.out.println(sum);         //这种方式不能打印出结果

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
