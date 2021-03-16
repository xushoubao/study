package spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashSet;

public class SqlDemo {

    public static void main(String[] args) {
        // create spark
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("java spark sql demo")
                .getOrCreate();

        // create datafream
        Dataset<Row> df = spark.read().json("data/test/resources/people.json");
        df.show();

        // run as a sql
        df.createOrReplaceTempView("people");
        Dataset<Row> sqlDF = spark.sql("select count(*) as cnt from people");
        sqlDF.show();

        // close spark
        spark.stop();
    }
}
