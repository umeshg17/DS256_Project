import java.io.Serializable;
import java.util.Objects;

public class Point implements Serializable {
    private static final long serialVersionUID = -433072613673987883L;
    private double x;
    private double y;

    public Point(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    public void setX(double x) {
        this.x = x;
    }

    public void setY(double y) {
        this.y = y;
    }

    @Override
    public String toString() {
        return x + "," + y;
    }

    public double distanceBetween(Point point) {
        return Math.hypot(x - point.getX(), y - point.getY());
    }
}