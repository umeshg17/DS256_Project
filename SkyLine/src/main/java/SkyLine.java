
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class SkyLine {
    public static List<Point> getSkyPoints(List<Point> p1, List<Point> p2) {
        List<Point> l1 = new LinkedList<Point>();
        System.out.println("******");
        printPoints(p1);
        System.out.println("------");
        printPoints(p2);
        System.out.println("------");
        ListIterator c1 = p1.listIterator();
        ListIterator c2 = p2.listIterator();
        while(c1.hasNext() && c2.hasNext()) {
            Point n1 = (Point) c1.next();
            c1.previous();
            Point n2 = (Point) c2.next();
            c2.previous();
            Point lastSelect;
            if (n1.getX() >= n2.getX()) {
                l1.add(n1);
                lastSelect = n1;
            } else {
                l1.add(n2);
                lastSelect = n2;
            }
            do {
                Point nn1 = (Point) c1.next();
                if (nn1.getY() > lastSelect.getY()) {
                    c1.previous();
                    break;
                }
            } while(c1.hasNext());
            do {
                Point nn2 = (Point) c2.next();
                if (nn2.getY() > lastSelect.getY()) {
                    c2.previous();
                    break;
                }
            } while(c2.hasNext());
        }
        while (c1.hasNext()) {
            l1.add((Point) c1.next());
        }
        while (c2.hasNext()) {
            l1.add((Point) c2.next());
        }
        printPoints(l1);
        System.out.println("******");
        return l1;
    }
    public static Integer isDominating(Point p1, Point p2) {
        if (p1.getX() < p2.getX() && p1.getY() < p2.getY()) return 2;
        else if (p2.getX() < p1.getX() && p2.getY() < p1.getY()) return 1;
        else return -1;
    }
    public static void printPoints(List<Point> p) {
        for (Integer i = 0; i < p.size(); i++) {
            System.out.println(p.get(i));
        }
    }
    public static void main(String[] args) throws IOException {


        SparkConf sparkConf = new SparkConf();

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> inputData = sc.textFile(args[0]);
        String outputFile = args[1];


        JavaRDD<Point> pointsData = inputData.map(line -> {
            String[] t = line.split(",");
            return new Point(Double.parseDouble(t[0]), Double.parseDouble(t[1]));
        });
        JavaRDD<List<Point>> pointsList = pointsData.map(point -> {
            List<Point> list = new ArrayList<Point>();
            list.add(point);
            return list;
        });

        List<Point> skyLines = pointsList.reduce(SkyLine::getSkyPoints);

        FileIOUtil.writePointArrayToFile(skyLines, outputFile, sc);
        sc.close();
    }
}
