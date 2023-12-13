package org.sdia.sparkSQL.exo1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class AppMain {
    public static SparkSession spark;

    public static void main(String[] args) {
        spark = SparkSession.builder()
                .appName("Spark SQL")
                .master("local[*]")
                .getOrCreate();

        // Charge les données des incidents à partir du ficher extension csv
        Dataset<Row> df = loadDataFromFileExCSV(spark);

        //Afficher le nombre d’incidents par service

        df.groupBy(col("service")).count().show();

        //Afficher les deux années où il a y avait plus d’incidents.

        showDeuxAnnePlusIncidents(df);

        // Arrête la session Spark
        spark.stop();

    }

    private static Dataset<Row> loadDataFromFileExCSV(SparkSession spark) {

        return spark.read().option("header", true).csv("incidents.csv");
    }

    public static void showDeuxAnnePlusIncidents(Dataset<Row> df){
        df.createOrReplaceTempView("incidents");

        spark.sql("select date as Year, COUNT(service) as Nums_Incident_Of_Year from incidents GROUP BY Year").show();

        System.out.println("nombre des incidents par date (avec la date est extract à partir function YEAR)");

        spark.sql("select YEAR(date) as Year, COUNT(service) as Nums_Incident_Of_Year from incidents GROUP BY YEAR(date)").show();

        spark.sql("select YEAR(date) as Year, COUNT(service) as Nums_Incident_Of_Year from incidents GROUP BY YEAR(date) HAVING Nums_Incident_Of_Year > 2 LIMIT 2").show();



    }
}
