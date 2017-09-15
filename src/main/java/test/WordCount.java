package test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.ScalaReflection;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class WordCount {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile("E:\\Users\\Administrator\\Workspaces\\IDEA\\spark_test\\src\\main\\resources\\people.txt");

        //测试顾虑方法  和   从一个集合创建JavaRDD   以及union功能
//        JavaRDD<String> result = lines.filter(s->s.contains("error"));


//        JavaRDD<String> unionLine = result.union(test2);
//        for (String one:unionLine.collect()){
//            System.out.println(one);
//        }
        List<Integer> integers = Arrays.asList(12,13,14,15,16);
        JavaRDD<Integer> test2 = ctx.parallelize(integers);

        JavaRDD<Integer> map = test2.flatMap(new FlatMapFunction<Integer, Integer>() {
            @Override
            public Iterator<Integer> call(Integer integer) throws Exception {
                return Arrays.asList(integer+1000).iterator();
            }
        });
        JavaPairRDD<Integer,String> jj = map.mapToPair(new PairFunction<Integer, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Integer integer) throws Exception {
                return new Tuple2<Integer,String>(5,"yes");
            }
        });


        JavaPairRDD<Integer, String> mm = jj.reduceByKey((v1, v2) -> v1+v2);
        for(Tuple2<Integer,String> gg:mm.collect()){
            System.out.println(gg.toString());
        }
    }


}
