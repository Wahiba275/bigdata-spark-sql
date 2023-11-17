package ma.enset;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class IncidentsAnalysis {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("Incidents Analysis").master("local[*]").getOrCreate();

       //Dataset<Row> csvData = spark.read().option("header", true).option("inferSchema", true).csv("incidents_services_2023.csv");

        String file = "incidents_services_2023.csv";
        Dataset<Row> dataset = spark.read().csv(file).toDF("id", "titre", "description","service" ,"date" );
        dataset.printSchema();
        Dataset<Row> incidentsPerService = dataset.groupBy(col("service"))
                .agg(count("id").alias("number_of_incidents"))
                .orderBy(desc("number_of_incidents"));

        incidentsPerService.show();

        //dataset.select("date").show();
        Dataset<Row> datasetWithDate = dataset.withColumn("date", to_date(col("date"), "yyyy-MM-dd"));
        Dataset<Row> incidentsYear = datasetWithDate.withColumn("year", year(col("date")));
        Dataset<Row> incidentsPerYear = incidentsYear.groupBy("year").count().orderBy(col("count").desc());

        incidentsPerYear.show(2);
        spark.close();

    }
}