import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public final class FileIOUtil {

    private FileIOUtil() {
        throw new IllegalStateException();
    }

    public static synchronized void appendPointArrayToFile(Point[] points,
                                                           String fileName)
            throws IOException {
        BufferedWriter outputWriter = null;
        try {
            outputWriter = new BufferedWriter(new FileWriter(fileName, true));
            for (int i = 0; i < points.length; ++i) {
                outputWriter.write(points[i].getX() + "," + points[i].getY());
                outputWriter.newLine();
            }
        } finally {
            if (outputWriter != null) {
                outputWriter.flush();
                outputWriter.close();
            }
        }
    }

    public static synchronized void writePointArrayToFile(List<Point> points, String fileName, JavaSparkContext sc) throws IOException {
        JavaRDD<Point> listRDD = sc.parallelize(points);
        listRDD.saveAsTextFile(fileName);
    }
}