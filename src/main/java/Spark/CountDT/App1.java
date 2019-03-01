package Spark.CountDT;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class App1 {
	public static void main(String[] args) {
		SparkSession sp = 	SparkSession
				.builder()
				.config("spark.some.config.option", "some-value")
				.getOrCreate();
    
    	 Dataset<Row> data = sp.read().parquet("hdfs://192.168.23.200:9000/data/Parquet/PageViewV1/2019_02_22");
    	 data.createOrReplaceTempView("data");
    	 Dataset<Row> kq = (Dataset<Row>) sp.sql("Select guid, Count(*) as Value from data Where domain='dantri.com.vn' and path='/the-gioi/vi-sao-viet-nam-duoc-xem-la-bieu-tuong-trong-thuong-dinh-my-trieu-20190221213037136.htm' group by guid order by Value desc").limit(5);
    	 //kq.show();
    	 kq.write().parquet("hdfs://192.168.23.202:9000/user/dutd/DTtext");
    	     	
	}
}
