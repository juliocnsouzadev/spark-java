package br.com.juliocnsouza.spark_essentials.ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GymCompetitors {

    private static void defaultProperties() {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
    }

    public static void main(final String[] args) {
        defaultProperties();
        final SparkSession sparkSession = SparkSession
                .builder()
                .appName("Gym Competitors")
                .config("spark.sql.warehouse.dir", "file:///c:temp")
                .master("local[*]")
                .getOrCreate();

        final Dataset<Row> csvData = sparkSession.read()
                .option("header", true)
                .csv("src/main/resources/gym_competition/gym_competition.csv");
        csvData.show();
    }
}
