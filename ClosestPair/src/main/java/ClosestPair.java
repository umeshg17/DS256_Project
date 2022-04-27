import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class ClosestPair {

    public static PairOfPoints getClosestPair(Point[] pointsArray) {
        Arrays.sort(pointsArray, Comparator.comparingDouble(a -> a.getX()));
        return getClosestPair(pointsArray, new Point[pointsArray.length], 0, pointsArray.length - 1);
    }

    private static PairOfPoints getClosestPair(Point[] a, Point[] tmp, int
            left, int right) {
        if (left >= right) {
            return null;
        }

        final int mid = (left + right) >> 1;
        final double medianX = a[mid].getX();

        final PairOfPoints d1 = getClosestPair(a, tmp, left, mid);
        final PairOfPoints d2 = getClosestPair(a, tmp, mid + 1, right);

        // Get the smaller distance from partitions.
        PairOfPoints d;
        if (d1 == null || d2 == null) {
            d = d1 == null ? d2 : d1;
        } else {
            d = d1.getDistance() < d2.getDistance() ? d1 : d2;
        }

        // Arrange points by increasing y-value.
        int i = left, j = mid + 1, k = left;
        while (i <= mid && j <= right) {
            if (a[i].getY() < a[j].getY()) {
                tmp[k++] = a[i++];
            } else {
                tmp[k++] = a[j++];
            }
        }
        while (i <= mid) {
            tmp[k++] = a[i++];
        }
        while (j <= right) {
            tmp[k++] = a[j++];
        }
        for (i = left; i <= right; i++) {
            a[i] = tmp[i];
        }

        k = left;
        for (i = left; i <= right; i++) {
            if (d == null || Math.abs(tmp[i].getX() - medianX) <= d.getDistance()) {
                tmp[k++] = tmp[i];
            }
        }

        for (i = left; i < k; i++) {
            for (j = i + 1; j < k; j++) {
                if (d != null && tmp[j].getY() - tmp[i].getY() >= d.getDistance()) {
                    break;
                } else if (d == null || tmp[i].distanceBetween(tmp[j]) < d.getDistance()) {
                    if (d == null) {
                        d = new PairOfPoints();
                    }
                    if (tmp[i] != tmp[j]) {
                        d.setFirst(tmp[i]);
                        d.setSecond(tmp[j]);
                    }
                }
            }
        }
        return d;
    }

    public static void main(String[] args) {

        int PARTITIONSIZE = 3;
//        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("JD Word Counter");

        SparkConf sparkConf = new SparkConf();

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        String inputPath = args[0];
        String outputPath = args[1];
        JavaRDD<String> inputData = sc.textFile(inputPath);
        JavaRDD<Point> pointsData = inputData.map(line ->
                {
                    String[] t = line.split(",");
                    double x = new Double(t[0]);
                    double y = new Double(t[1]);
                    return new Point(x, y);
                });

//        local

//        List<Point> pointsList = pointsData.collect();
//        Point[] pointsArray = new Point[pointsList.size()];
//        pointsList.toArray(pointsArray);
//        Arrays.sort(pointsArray, Comparator.comparingDouble(Point::getX));
//        PairOfPoints closestPair = getClosestPair(pointsArray);
//
//        System.out.println(closestPair.getFirst().toString() + closestPair.getSecond().toString());
//        pointsData.saveAsTextFile(outputPath);

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

        final int dividerX = (int) (maxX /
                PARTITIONSIZE) + 1;

        final int dividerY = (int) (maxY /
                PARTITIONSIZE) + 1;


        JavaPairRDD<Cell, Point> PointWithCell = pointsData.mapToPair
                (new PairFunction<Point, Cell, Point>() {

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

        JavaPairRDD<Cell, Iterable<Point>> GroupPointsWithCell =
                PointWithCell.groupByKey();

        JavaPairRDD<Cell, Iterable<Point>> CandidatePointsRDD = GroupPointsWithCell.mapValues(new Function<Iterable<Point>, Iterable<Point>>() {
            @Override
            public Iterable<Point> call(Iterable<Point> points) throws Exception {
                final long serialVersionUID = 5462223600l;

                Point[] pointsArray = Iterables.toArray(points, Point.class);
                if (pointsArray == null) {
                    return new ArrayList<Point>();
                }
                Arrays.sort(pointsArray, Comparator.comparingDouble(Point::getX));
                PairOfPoints closestPair = getClosestPair(pointsArray);

                List<Point> candidates = new ArrayList<Point>();
                if (closestPair == null || pointsArray.length == 1) {
                    candidates.add(pointsArray[0]);
                    return candidates;
                }

                candidates.add(closestPair.getFirst());
                candidates.add(closestPair.getSecond());
                for (Point p : pointsArray) {
                    if (p.equals(closestPair.getFirst()) || p.equals(closestPair
                            .getSecond())) {
                        continue;
                    }
                    int l = ((int) p.getX() / dividerX) * dividerX;
                    int r = l + dividerX;
                    int b = ((int) p.getY() / dividerY) * dividerY;
                    int t = b + dividerY;
                    double distance = closestPair.getDistance();
                    if (p.getX() - l <= distance || r - p.getX() <= distance) {
                        candidates.add(p);
                        continue;
                    }
                    if (p.getY() - b <= distance || t - p.getY() <= distance) {
                        candidates.add(p);
                    }
                }

                return candidates;
            }
        });

        JavaRDD<Iterable<Point>> candidatePointsIterables = CandidatePointsRDD.values();
        List<Iterable<Point>> pointInterables = candidatePointsIterables.collect();

        List<Point> candidatePoints = new ArrayList<Point>();
        for (Iterable<Point> points : pointInterables) {
            for (Point point : points) {
                candidatePoints.add(point);
            }
        }

        candidatePoints.sort(Comparator.comparingDouble(Point::getX));

        Point[] pointArray = new Point[candidatePoints.size()];

        candidatePoints.toArray(pointArray);

        PairOfPoints closestPair = getClosestPair(pointArray);

        List<String> outputList = new ArrayList<String>();

        outputList.add(closestPair.getFirst().toString());
        outputList.add(closestPair.getSecond().toString());

        JavaRDD<String> outputRDD = sc.parallelize(outputList);
        outputRDD.saveAsTextFile(outputPath);

    }
}
