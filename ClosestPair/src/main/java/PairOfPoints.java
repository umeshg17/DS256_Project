public class PairOfPoints {
    private Point first;
    private Point second;

    public PairOfPoints(Point first, Point second) {
        this.first = first;
        this.second = second;
    }

    public PairOfPoints() {
        this.first = null;
        this.second = null;
    }

    public Point getFirst() {
        return first;
    }

    public Point getSecond() {
        return second;
    }

    public void setFirst(Point first) {
        this.first = first;
    }

    public void setSecond(Point second) {
        this.second = second;
    }

    public double getDistance(){
        return first.distanceBetween(second);
    }
}
