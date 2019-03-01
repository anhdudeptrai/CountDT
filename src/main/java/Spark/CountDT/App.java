package Spark.CountDT;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class App 
{
    public static void main( String[] args )
    {
    	SparkSession sp = 	SparkSession
				.builder()
				.config("spark.some.config.option", "some-value")
				.getOrCreate();
    
    	 Dataset<Row> data = sp.read().parquet("hdfs://192.168.23.200:9000/data/Parquet/PageViewV1/2019_02_21");
    	 data.createOrReplaceTempView("data");
    	 Dataset<Row> kq = sp.sql("SELECT COUNT(DISTINCT guid) FROM (SELECT guid, domain FROM data WHERE domain LIKE '%dantri%')");
    	 kq.show();
    }
}
