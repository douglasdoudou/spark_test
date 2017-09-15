package test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class SparkTest {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        JavaRDD<Integer> numbers = jsc.parallelize(Arrays.asList(1,3,3,4,5,6,7,8,9),2);
        JavaRDD<Integer> gg = numbers.map(integer -> {
            return integer+1;
//            double x = Math.random() * 2 - 1;//Math.random():0~1
//            double y = Math.random() * 2 - 1;
//            return (x * x + y * y <= 1) ? 1 : 0;
        });
//        int b = gg.reduce((integer1,integer2) ->integer1+integer2);
//        System.out.println(b);
        for(Integer jj:gg.collect()) System.out.println(jj);
    }
}
