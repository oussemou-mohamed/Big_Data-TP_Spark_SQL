package org.sdia.sparkSQL;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class AppMain {
    public static SparkSession spark;

    public static void main(String[] args) {
         spark = SparkSession.builder()
                .appName("Spark SQL")
                .master("local[*]")
                .getOrCreate();

        // Charge les données des consultations depuis la base de données
        Dataset<Row> consultationsDF = loadTableFromDatabase(spark, "CONSULTATIONS");

        // Affiche le nombre de consultations par jour
        showConsultationsPerDay(consultationsDF);

        // Charge les données des médecins depuis la base de données
        Dataset<Row> medecinsDF = loadTableFromDatabase(spark, "MEDECINS");

        consultationsDF.createOrReplaceTempView("CONSULTATIONS");
        medecinsDF.createOrReplaceTempView("MEDECINS");

        // Affiche le nombre de consultations par médecin

        Dataset<Row> joinedDF1 = showConsultationsParMedecin();

        joinedDF1.show();

        // Affiche le nombre de patients assistés par chaque médecin

        Dataset<Row> joinedDF2 = showNbrPatientParMedecin();

        joinedDF2.show();


        // Arrête la session Spark
        spark.stop();
    }

    private static Dataset<Row> loadTableFromDatabase(SparkSession spark, String tableName) {
        Map<String, String> options = new HashMap<>();
        options.put("driver", "com.mysql.cj.jdbc.Driver");
        options.put("url", "jdbc:mysql://localhost:3306/DB_HOPITAL");
        options.put("dbtable", tableName);
        options.put("user", "root");
        options.put("password", "");

        return spark.read().option("header", true).format("jdbc").options(options).load();
    }

    private static Dataset<Row> showConsultationsParMedecin(){

        String query = "SELECT `medecins`.`NOM`, `medecins`.`PRENOM`, COUNT(`consultations`.`DATE_CONSULTATIONS`) as NOMBRE_CONSULTATION_MEDECIN " +
                "FROM consultations " +
                "INNER JOIN medecins ON `consultations`.`ID_MEDECIN` = `medecins`.`ID` " +
                "GROUP BY `medecins`.`NOM`, `medecins`.`PRENOM`";
        Dataset<Row> joinedDF = spark.sql(query);

        return  joinedDF;
    }

    private static void showConsultationsPerDay(Dataset<Row> consultationsDF) {
        consultationsDF.groupBy("DATE_CONSULTATIONS").count().show();
    }


    private static Dataset<Row> showNbrPatientParMedecin(){

        String query = "SELECT medecins.NOM,medecins.PRENOM, COUNT(DISTINCT ID_PATIENT) as NombrePatientsAssiste FROM consultations INNER JOIN medecins ON consultations.ID_MEDECIN=medecins.ID GROUP BY medecins.NOM,medecins.PRENOM";

        Dataset<Row> joinedDF = spark.sql(query);

        return  joinedDF;
    }


}
