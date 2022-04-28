import com.google.common.collect.Iterables;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class RangeSearch {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        String inputPath = args[0];
        String outputFile = args[1];
        String queryPath = args[2];
        JavaRDD<String> inputData = sc.textFile(inputPath);
        JavaRDD<Point> pointsData = inputData.map(line ->
        {
            String[] t = line.split(",");
            return new Point(Double.parseDouble(t[0]), Double.parseDouble(t[1]));
        });

        List<String> queries = sc.textFile(queryPath).collect();
        int queryConter = 0;
        for (String queryLine : queries) {
            String[] query = queryLine.split(" ");
            if (query[0].equals("search")) {
                String[] t2 = query[1].split(",");
                Range searchRegion = new Range(Double.parseDouble(t2[0]), Double.parseDouble(t2[1]), Double.parseDouble(t2[2]), Double.parseDouble(t2[3]));
                // Range searchRegion = new Range(3000.323, 25000.105, 3100.2154, 30000.9);
                JavaRDD<Point> rangeSearchPoints = pointsData.filter(searchRegion::containsPoint);
                Long rangeSearchPointsCount = rangeSearchPoints.count();

                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH-mm-ss-SSS");
                LocalDateTime now = LocalDateTime.now();

                System.out.println("######" + queryConter + ": number of output: " + rangeSearchPointsCount + ", time: " + dtf.format(now));
//                    FileIOUtil.writePointArrayToFile(rangeSearchPointsList, outputFile + queryConter, sc);
//                    for (int i = 0; i < rangeSearchPointsList.size(); i++) {
//                        System.out.println(rangeSearchPointsList.get(i));
//                    }
                System.out.println("######");
            }
            queryConter = queryConter + 1;
        }

    }
}
