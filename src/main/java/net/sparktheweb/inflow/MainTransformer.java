package net.sparktheweb.inflow;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by beto on 08/03/14.
 */
public class MainTransformer {

    public static void main(String args[]) throws Exception {
        JavaSparkContext ctx = new JavaSparkContext("local", "MainTransformer",
                System.getenv("SPARK_HOME"), JavaSparkContext.jarOfClass(MainTransformer.class));

        final List<String> lines = Files.readAllLines(Paths.get("src/main/resources/BBC.html"), Charset.forName("UTF-8"));

        //Check this pattern
        final Pattern pattern = Pattern.compile("[^/]*\\.js\"");

        JavaRDD<String> javardd = null;

        JavaRDD<String> sites = ctx.textFile("src/main/resources/top-10.csv");

        //From sites to url of sites

        JavaRDD<String> urls = sites.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return s.split(",")[1];
            }
        });

        //From site to html of site
        JavaRDD<String> htmls = urls.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return lines; //Not calling site yet, just returning same html for all
            }
        });

        //From html of site to js urls

        JavaRDD<String> js = htmls.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                List<String> list = new LinkedList<String>();
                Matcher matcher = pattern.matcher(s);
                while (matcher.find()) {
                    String jssrc = matcher.group();
                    list.add(matcher.group());
                }
                return list;
            }
        });

        javardd = js;

        //from js file names to count per js file name

        //output

        List<String> result = javardd.collect();
        System.out.println(result);
    }
}
