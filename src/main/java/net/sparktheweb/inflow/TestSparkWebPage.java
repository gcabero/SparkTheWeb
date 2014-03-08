package net.sparktheweb.inflow;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Created by rajan on 08/03/14.
 */
public class TestSparkWebPage {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext("local", "SparkTheWeb", System.getenv("SPARK_HOME"),new String[]{"target/SparkTheWeb-1.0-SNAPSHOT.jar"});
        JavaRDD<String> logData = sc.textFile("src/main/resources/BBC.html").cache();
        long numAs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains(".js"); }
        }).count();

        System.out.println(numAs);
    }
}
