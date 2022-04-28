import com.google.common.collect.Iterables;
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

public class RangeSearchTree {

    public static void main(String[] args) {

        int PARTITIONSIZE = 3;

        SparkConf sparkConf = new SparkConf();

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        String inputPath = args[0];
        String outputPath = "output";
        String outputFile = args[1];
        JavaRDD<String> inputData = sc.textFile(inputPath);
        JavaRDD<Point> pointsData = inputData.map(line ->
        {
            String[] t = line.split(",");
            return new Point(Double.parseDouble(t[0]), Double.parseDouble(t[1]));
        });

        double maxX = pointsData.reduce( (value1, value2) -> {
            if (value1.getX() > value2.getX()) {
                return value1;
            }
            else {
                return value2;
            }
        }).getX();

        double maxY = pointsData.reduce( (value1, value2) -> {
            if (value1.getX() > value2.getX()) {
                return value1;
            }
            else {
                return value2;
            }
        }).getY();

        final int dividerX = (int) (maxX / PARTITIONSIZE) + 1;

        final int dividerY = (int) (maxY / PARTITIONSIZE) + 1;


        JavaPairRDD<Cell, Point> PointWithCell = pointsData.mapToPair(new PairFunction<Point, Cell, Point>() {
            private static final long serialVersionUID = -433072613673987883L;
            public Tuple2<Cell, Point> call(Point p) throws Exception {
                int l = ((int) p.getX() / dividerX) * dividerX;
                int r = l + dividerX;
                int b = ((int) p.getY() / dividerY) * dividerY;
                int t = b + dividerY;
                Cell cell = new Cell(l, r, b, t);

                return new Tuple2<Cell, Point>(cell, p);
            }
        });

        JavaPairRDD<Cell, Iterable<Point>> GroupPointsWithCell = PointWithCell.groupByKey();

        JavaPairRDD<Cell, Tree> CandidatePointsRDD = GroupPointsWithCell.mapValues(new Function<Iterable<Point>, Tree>() {
            @Override
            public Tree call(Iterable<Point> points) throws Exception {
                final long serialVersionUID = 5462223600l;

                Point[] pointsArray = Iterables.toArray(points, Point.class);
                System.out.println("******" + pointsArray.length);
                if (pointsArray == null || pointsArray.length == 0) {
                    return new Tree(0, 0, 0, 0);
                    // return new LinkedList<Point>();
                }

                int l = ((int) pointsArray[0].getX() / dividerX) * dividerX;
                int r = l + dividerX;
                int b = ((int) pointsArray[0].getY() / dividerY) * dividerY;
                int t = b + dividerY;

                Tree tree = new Tree(l, b, r, t);
                for (Integer i = 0; i < pointsArray.length; i++) {
                    tree.addPoint(pointsArray[i]);
                }
                // System.out.println("######" + candidates.size());

                return tree;
            }
        });

        try {
            File myObj = new File("query.txt");
            Scanner myReader = new Scanner(myObj);
            Integer queryConter = 0;
            while (myReader.hasNextLine()) {
                String queryLine = myReader.nextLine();
                String[] query = queryLine.split(" ");
                if (query[0].equals("search")) {
                    String[] t2 = query[1].split(",");
                    Range searchRegion = new Range(Double.parseDouble(t2[0]), Double.parseDouble(t2[1]), Double.parseDouble(t2[2]), Double.parseDouble(t2[3]));
                    // Range searchRegion = new Range(3000.323, 25000.105, 3100.2154, 30000.9);
                    System.out.println("******");
                    JavaPairRDD<Cell, Integer> rangeSearchPoints = CandidatePointsRDD.mapValues((Tree t) -> {
                        Integer searchCount = t.search(searchRegion, null).size();
                        return searchCount;
                    });

                    Tuple2<Cell, Integer> searchResult = rangeSearchPoints.reduce((Tuple2<Cell, Integer> p1, Tuple2<Cell, Integer> p2) -> {
                        Tuple2<Cell, Integer> p3 = new Tuple2<Cell, Integer>(p1._1, p1._2() + p2._2());
                        return p3;
                    });

                    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH-mm-ss-SSS");
                    LocalDateTime now = LocalDateTime.now();

                    System.out.println("######" + queryConter + ": number of output: " + searchResult._2 + ", time: " + dtf.format(now));

                    // FileIOUtil.writePointArrayToFile(searchResult._2, outputFile + queryConter, sc);
                }
                queryConter = queryConter + 1;
            }
            myReader.close();
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }
}
