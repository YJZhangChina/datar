package miluroe.datar.demo;

import com.google.common.io.LineReader;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.ml.feature.VectorSlicer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import scala.Function1;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class Housing {

    private static final String path = "/home/miluroe/data/housing.csv";

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Housing Dataset Analysis")
                .config("spark.master", "local").getOrCreate();
        Dataset<Row> dataset = spark.read().format("com.databricks.spark.csv").option("inferSchema", "true")
                .option("header", "true").load("/home/miluroe/data/housing.csv");
        dataset = dataset.filter((FilterFunction<Row>) value -> {
            Object object = value.get(4);
            return object != null;
        });
        for(String column : dataset.columns()) {
            System.out.println(column);
        }
        dataset = dataset.withColumn("median_house_value", dataset.col("median_house_value").divide(1E5));
        dataset = dataset.withColumn("rooms_per_household", dataset.col("total_rooms").divide(dataset.col("households")))
                .withColumn("population_per_household", dataset.col("population").divide(dataset.col("households")))
                .withColumn("bedrooms_per_room", dataset.col("total_bedrooms").divide(dataset.col("total_rooms")));
        dataset = dataset.select("median_house_value",
                "total_bedrooms",
                "population",
                "households",
                "median_income",
                "rooms_per_household",
                "population_per_household",
                "bedrooms_per_room");
        System.out.println(dataset.collectAsList().get(0).toString());
        dataset = new VectorSlicer().setInputCol("median_house_value").setOutputCol("label").transform(dataset);
        System.out.println(dataset.collectAsList().get(0).toString());

//        from pyspark.ml.linalg import DenseVector
//
//                input_data = df.rdd.map(lambda x: (x[0], DenseVector(x[1:])))
//        df = spark.createDataFrame(input_data, ["label", "features"])

//
//
//        LinearRegression regression = new LinearRegression();
//        System.out.println(regression.fit(dataset));
    }

//    public static void exec() {
//        SparkConf conf = new SparkConf();
//        conf.setMaster("local");
//        conf.setAppName("Housing");
//        SparkContext context = new SparkContext(conf);
//        SparkSession session = new SparkSession(context);
//        SQLContext sqlContext = new SQLContext(context);
//        DataFrame dataFrame;
//        JavaRDD<String> input = context.textFile(path);
//        JavaRDD<String[]> csv = input.map(new CSVLineParser());
//        List<String[]> list = csv.collect();
//        System.out.println(list.size());
//        context.stop();
//    }

//    public static void execBySparkSQL() {
//        SparkSession session = SparkSession.builder().appName("Housing").config().getOrCreate();
//        Dataset<Row> df = session.read().format("com.databricks.spark.csv").option("inferSchema", "true")
//                .option("header", "true").load("d:/ab.csv");
//
//        String result = "=========" + df.count();
//        df.show();
////        return new ResponseEntity<>( result, HttpStatus.OK);
//    }
}
