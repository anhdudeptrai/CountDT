package Spark.CountDT;

import org.apache.spark.sql.SparkSession;

public class App2 {
	public static void main(String[] args) {
		SparkSession sp = 	SparkSession
				.builder()
				.config("spark.some.config.option", "some-value")
				.getOrCreate();
		long time1 = System.currentTimeMillis();
//		while (true) {
//			long time2 = System.currentTimeMillis();
//			if (time2 - time1 >= 30000) {
//				System.out.println(time2);
//				System.out.println(sp.read().parquet("hdfs://192.168.23.200:9000/data/Parquet/PageViewV1/2019_02_22").count());				
//				time1 = time2;
//			}
//		}
		while (true) {
			System.out.println(System.currentTimeMillis() + " : " + sp.read().parquet("hdfs://192.168.23.200:9000/data/Parquet/PageViewV1/2019_02_22").count());
		}
    	     	
	}
}
