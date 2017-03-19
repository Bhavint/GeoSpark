package org.datasyslab.geospark.spatialOperator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;

import scala.Tuple2;

public class CustomJoinQuery {
	
	public static JavaPairRDD<Envelope, HashSet<Point>> CustomSpatialJoinQuery(SparkContext sc,PointRDD objectRDD, RectangleRDD rectangleRDD)
	{
		
//		SparkConf conf = new SparkConf().setMaster("master").setAppName("JoinQuery");
		final JavaSparkContext scontext = new JavaSparkContext(sc);
//
////		PointRDD objectRDD = new PointRDD(sc, pointRDD, 0, FileDataSplitter.CSV, false, 10, StorageLevel
////				.MEMORY_ONLY_SER()); /*
////										 * The O means spatial attribute starts at
////										 * Column 0 and the 10 means 10 RDD
////										 * partitions
////										 */
////		RectangleRDD rectangleRDD = new RectangleRDD(sc, RectangleRDD, 0, FileDataSplitter.CSV, false,
////				StorageLevel
////						.MEMORY_ONLY_SER()); 
//		// collect rectangles into a java list
		JavaRDD javaRDD = rectangleRDD.getRawSpatialRDD();
		List<Envelope>rectangleRDDList=javaRDD.collect();
		JavaRDD<Point> resultRDD = null;
		List listOfPoints=new ArrayList<Point>();
		List<Tuple2<Envelope, HashSet<Point>>>pointRectanglePairList=new ArrayList<Tuple2<Envelope, HashSet<Point>>>();
		JavaPairRDD<Envelope, HashSet<Point>> resultList = null;
		for (Envelope env : rectangleRDDList) {
			try {
				resultRDD = RangeQuery.SpatialRangeQuery(objectRDD, env, 0, false);
				listOfPoints=resultRDD.collect();
				HashSet resultPointSet=new HashSet<Point>(listOfPoints);
				Tuple2 tuple=new Tuple2<Envelope, HashSet<Point>>(env,resultPointSet);
				pointRectanglePairList.add(tuple);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			resultList=scontext.parallelizePairs(pointRectanglePairList);
			return resultList;
	}
		return resultList;
}
	
	public static JavaPairRDD<Envelope, HashSet<Point>> CustomSpatialJoinQuery2(SparkContext sc1,PointRDD objectRDD, RectangleRDD rectangleRDD)
	{
		
		final JavaSparkContext sc = new JavaSparkContext(sc1);
		

	        List<Tuple2<Envelope, HashSet<Point>>> resultTupleList = new ArrayList<Tuple2<Envelope, HashSet<Point>>>();
	        
	        JavaRDD javaRDD = rectangleRDD.getRawSpatialRDD();
			List<Envelope>envList=javaRDD.collect();
	        //List<Envelope> envList = rectangleRDD.getRawSpatialRDD().collect();

	        // Traverse all the query windows
	        for(int i = 0; i < envList.size(); i++){
	        	try{
	            Envelope envelope = envList.get(i);

	            // Get the result from range query (project requirement)
	            JavaRDD pointRDD = RangeQuery.SpatialRangeQuery(objectRDD, envelope, 0, false);
	            // Extract the list of Point from the result
	            //ArrayList<Point> listPoint = pointRDD.getRawPointRDD().collect();
	            List listPoint = pointRDD.collect();

	            // Create a hashset and put all of the Point into it
	            HashSet<Point> pointSet = new HashSet();
	            for(int j = 0; j < listPoint.size(); j++){
	                Point point = (Point)listPoint.get(j);
	                pointSet.add(point);
	            }

	            // Create Tuple2 object for result since the parallelizePairs function in JavaSparkContext takes Tuple2
	            Tuple2<Envelope, HashSet<Point>> resultTuple = new Tuple2<Envelope, HashSet<Point>>(envelope, pointSet);
	            resultTupleList.add(resultTuple);
	        }catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}}

	        // Transform the result Tuple list to JavaPairRDD
	        return sc.parallelizePairs(resultTupleList);
		
		
		
		
//		SparkConf conf = new SparkConf().setMaster("master").setAppName("JoinQuery");
/*		final JavaSparkContext scontext = new JavaSparkContext(sc);
//
////		PointRDD objectRDD = new PointRDD(sc, pointRDD, 0, FileDataSplitter.CSV, false, 10, StorageLevel
////				.MEMORY_ONLY_SER()); /*
////										 * The O means spatial attribute starts at
////										 * Column 0 and the 10 means 10 RDD
////										 * partitions
////										 */
////		RectangleRDD rectangleRDD = new RectangleRDD(sc, RectangleRDD, 0, FileDataSplitter.CSV, false,
////				StorageLevel
////						.MEMORY_ONLY_SER()); 
//		// collect rectangles into a java list
/*		JavaRDD javaRDD = rectangleRDD.getRawSpatialRDD();
		List<Envelope>rectangleRDDList=javaRDD.collect();
		JavaRDD<Point> resultRDD = null;
		List listOfPoints=new ArrayList<Point>();
		List<Tuple2<Envelope, HashSet<Point>>>pointRectanglePairList=new ArrayList<Tuple2<Envelope, HashSet<Point>>>();
		JavaPairRDD<Envelope, HashSet<Point>> resultList = null;
		for (Envelope env : rectangleRDDList) {
			try {
				resultRDD = RangeQuery.SpatialRangeQuery(objectRDD, env, 0, false);
				listOfPoints=resultRDD.collect();
				HashSet resultPointSet=new HashSet<Point>(listOfPoints);
				Tuple2 tuple=new Tuple2<Envelope, HashSet<Point>>(env,resultPointSet);
				pointRectanglePairList.add(tuple);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			resultList=scontext.parallelizePairs(pointRectanglePairList);
			return resultList;
	}
		return resultList;*/
}

}
