import java.io.Serializable;
import java.util.Objects;

public class Cell implements Serializable {

    private static final long serialVersionUID = -433072613673987883L;

    public int left, right, top, bottom;

    public Cell() {
        right = 0;
        left = 0;
        top = 0;
        bottom = 0;
    }

    public Cell(int left, int right, int top, int bottom) {
        this.left = left;
        this.right = right;
        this.top = top;
        this.bottom = bottom;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Cell)) return false;
        Cell cell = (Cell) o;
        return left == cell.left && right == cell.right && top == cell.top && bottom == cell.bottom;
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, top, bottom);
    }
}
