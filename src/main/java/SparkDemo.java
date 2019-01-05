import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf25.DescriptorProtos;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class SparkDemo {

    public static void main(String... args) {

        System.setProperty("hadoop.home.dir", "C://Hadoop");

        HashMap<String, Object> objectMap = new HashMap<>();

        ObjectMapper jsonOutput =new ObjectMapper();

        PublisherExample publisherExample=new PublisherExample();

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Java Spark example")
                .getOrCreate();

        Dataset<Row> parquetFileDF = spark.read().parquet("userdata1.parquet");

        List<String> fieldNames = Arrays.asList(parquetFileDF.schema().fieldNames());

        parquetFileDF.foreach((ForeachFunction<Row>) row -> {

            for(int i=0;i<fieldNames.size();i++)
            {
                objectMap.put(fieldNames.get(i),row.get(i));
            }

            publisherExample.publishMessages(jsonOutput.writeValueAsString(objectMap));
        });
    }
}
