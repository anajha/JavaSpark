import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDemo {

    public static void main(String...args){

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Java Spark example")
                .getOrCreate();

        Dataset<Row> peopleDF = spark.read().json("dummydata.json");

        // DataFrames can be saved as Parquet files, maintaining the schema information
        peopleDF.write().parquet("people.parquet");

        // Read in the Parquet file created above.
        // Parquet files are self-describing so the schema is preserved
        // The result of loading a parquet file is also a DataFrame
        Dataset<Row> parquetFileDF = spark.read().parquet("dummyfile.parquet");

        // Parquet files can also be used to create a temporary view and then used in SQL statements
        parquetFileDF.createOrReplaceTempView("parquetFile");
        Dataset<Row> namesDF = spark.sql("SELECT * from parquetFile");

        namesDF.foreach((ForeachFunction<Row>) row->System.out.println(row));
    }
}
