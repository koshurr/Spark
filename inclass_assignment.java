package jacob;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.sparkTutorial.rdd.commons.Utils;

import scala.Tuple2;

public class inclass_assignment{
//C:\Users\Students\Desktop
	public static void main(String[] args) throws Exception{
        Logger.getLogger("org").setLevel(Level.ERROR);

		SparkConf conf = new SparkConf().setAppName("cars").setMaster("local[*]");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	
	    JavaRDD<String> lines = sc.textFile("in/cars.csv");
	    @SuppressWarnings("unlikely-arg-type")
		JavaRDD<String> carsInUSA = lines.filter(line -> String.valueOf(line.split(";")[8]).equals("US"));
	    
	    JavaPairRDD<String, String> nameAndMPG =
	            lines.mapToPair( line -> new Tuple2<>(line.split(";")[0],
	            		line.split(";")[1]));
	    
	    for(Tuple2<String, String> line:nameAndMPG.collect()){
            System.out.println("* "+line);
        }

	
	// 
	}
}
