import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.*;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.nio.file.Files;
import java.nio.file.Paths;

public class wineTrain {

    private static final String TRAIN_CSV_URL = "~/project/TrainingDataset.csv";
    private static final String VAL_CSV_URL = "~/project/ValidationDataset.csv";

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Wine Quality Prediction")
                .getOrCreate();

        Dataset<Row> trainDF = spark.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", ";")
                .load(TRAIN_CSV_URL);

        String[] featureCols = new String[]{
                "fixedAcidity", "volatileAcidity", "citricAcid", "residualSugar",
                "chlorides", "freeSulfurDioxide", "totalSulfurDioxide", "density",
                "pH", "sulphates", "alcohol"
        };

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(featureCols)
                .setOutputCol("features");
        Dataset<Row> vectorDF = assembler.transform(trainDF);

        StringIndexer labelIndexer = new StringIndexer()
                .setInputCols("quality")
                .setOutputCol("label")
                .fit()

        spark.stop();
    }

}
