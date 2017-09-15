package test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.sql.Struct;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

public class SparkFirst {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local").getOrCreate();
        Dataset<Row> lines = sparkSession.read().json("src/main/resources/people.json");
        lines.show();
        lines.select("name").show();

        List<Row> data = Arrays.asList(
                RowFactory.create(Arrays.asList("Hi I heard about Spark".split(" "))),
                RowFactory.create(Arrays.asList("I wish Java could use case classes".split(" "))),
                RowFactory.create(Arrays.asList("Logistic regression models are neat".split(" ")))
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())});
        Dataset<Row> documentDF = sparkSession.createDataFrame(data, schema);


        documentDF.show(2);
        sparkSession.stop();
    }
}
