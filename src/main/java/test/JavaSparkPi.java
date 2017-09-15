package test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

/**
 * 通过概率求单位圆的面积（就是pi）
 */
public class JavaSparkPi {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        int slices = (args.length == 1) ? Integer.parseInt(args[0]) : 9;
        int n = 10000000 * slices;
        List<Integer> l = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            l.add(i);
        }

        JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);
        JavaRDD<Integer> gg= dataSet.map(integer -> {
            double x = Math.random() * 2 - 1;//Math.random():0~1
            double y = Math.random() * 2 - 1;
            return (x * x + y * y <= 1) ? 1 : 0;
        });
        int count = gg.reduce((integer, integer2) -> integer + integer2);

        System.out.println("Pi is roughly " + 4.0 * count / n);

        spark.stop();
    }
}
