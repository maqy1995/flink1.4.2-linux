package org.apache.flink.examples.java.maqy;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

class MyPartition implements Partitioner {
    @Override
    public int partition(Object key, int numPartitions) {
        if(((String)key).equals("a")){
            return 0;
        }else if(((String)key).equals("b") || ((String)key).equals("e")) {
            return 1;
        }else if(((String)key).equals("d")){
            return 2;
        }else
            return 3;
    }
}

public class MapPartition {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        if(args.length != 3){
            System.out.println("the use is   input  output parallelism");
            return;
        }

        String input = args[0];
        String output = args[1];
        int parallelism = Integer.parseInt(args[2]);

        //设置并行度
        env.setParallelism(parallelism);
        DataSet<String> text = env.readTextFile(input);

        //DataSet<String> text1=text.rebalance();
        DataSet<Tuple2<String,Integer>> words = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] strings = value.split("\\W+");
                for(String s:strings){
                    out.collect(new Tuple2<String, Integer>(s,1));
                }
            }
        });
        DataSet<Tuple2<String,Integer>> RangeWords = words.partitionByHash(0);
        //DataSet<Tuple2<String,Integer>> RangeWords1 = words.partitionCustom(new MyPartition(),0);
        //words.print();
        DataSet<Tuple2<String,Integer>> counts = RangeWords.sum(1);
        words.writeAsText(output);
        env.execute("this is a hash partition job!!!");
    }
}
