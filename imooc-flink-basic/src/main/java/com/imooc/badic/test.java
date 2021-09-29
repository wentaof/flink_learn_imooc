package com.imooc.badic;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import scala.collection.mutable.ArrayBuffer;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author fengwentao@yadingdata.com
 * @date 2021/9/29 14:27
 * @Version 1.0.0
 * @Description TODO
 */
public class test {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Integer> nums = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));

        nums.print();


    }
}
