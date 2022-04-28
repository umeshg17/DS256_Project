import java.io.Serializable;
import java.util.Objects;

public class Range implements Serializable {
    public Double left, right, top, bottom;

    public Range() {
        right = 0.0;
        left = 0.0;
        top = 0.0;
        bottom = 0.;
    }

    public Range(Double left, Double bottom, Double right, Double top) {
        this.left = left;
        this.right = right;
        this.top = top;
        this.bottom = bottom;
    }

    public Range(int left, int bottom, int right, int top) {
        this.left = new Double(left);
        this.right = new Double(right);
        this.top = new Double(top);
        this.bottom = new Double(bottom);
    }
    public boolean containsPoint(Point point) {
        return point.getX() >= this.left 
            && point.getX() < this.right 
            && point.getY() >= this.bottom 
            && point.getY() < this.top;
    }
    public boolean doesOverlap(Range testRegion) {
        if (testRegion.right < this.left) {
            return false;
        }
        if (testRegion.left > this.right) {
            return false;
        }
        if (testRegion.bottom > this.top) {
            return false;
        }
        if (testRegion.top < this.bottom) {
            return false;
        }
        return true;
    }
    public Range getQuadrant(int quadrantIndex) {
        Double quadrantWidth = (this.right - this.left) / 2;
        Double quadrantHeight = (this.top - this.bottom) / 2;
        // 0=SW, 1=NW, 2=NE, 3=SE
        switch (quadrantIndex) {
        case 0:
            return new Range(left, bottom, left + quadrantWidth, bottom + quadrantHeight);
        case 1:
            return new Range(left, bottom + quadrantHeight, left + quadrantWidth, top);
        case 2:
            return new Range(left + quadrantWidth, bottom + quadrantHeight, right, top);
        case 3:
            return new Range(left + quadrantWidth, bottom, right, bottom + quadrantHeight);
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Range)) return false;
        Range Range = (Range) o;
        return left == Range.left && right == Range.right && top == Range.top && bottom == Range.bottom;
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, top, bottom);
    }

    @Override
    public String toString() {
        return "("+ left + "," + bottom + ", " + right + ", " + top + ")";
    }
}