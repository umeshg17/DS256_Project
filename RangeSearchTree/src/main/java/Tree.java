import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class Tree implements Serializable {
    private static final int MAX_POINTS = 3;
    private Range area = new Range();
    private List<Point> points = new LinkedList<>();
    private List<Tree> quadTrees = new LinkedList<>();

    public Tree() { }
    public Tree(Range area) {
        this.area = area;
    }
    public Tree(Cell area) {
        this.area.left = Double.valueOf(area.left);
        this.area.bottom = Double.valueOf(area.bottom);
        this.area.right = Double.valueOf(area.right);
        this.area.top = Double.valueOf(area.top);
    }
    public Tree(Integer left, Integer bottom, Integer right, Integer top) {
        this.area.left = Double.valueOf(left);
        this.area.bottom = Double.valueOf(bottom);
        this.area.right = Double.valueOf(right);
        this.area.top = Double.valueOf(top);
    }
    public boolean addPoint(Point point) {
        if (this.area.containsPoint(point)) {
            if (this.points.size() < MAX_POINTS) {
                this.points.add(point);
                return true;
            } else {
                if (this.quadTrees.size() == 0) {
                    createQuadrants();
                }
                return addPointToOneQuadrant(point);
            }
        }
        return false;
    }
    private boolean addPointToOneQuadrant(Point point) {
        boolean isPointAdded;
        for (int i = 0; i < 4; i++) {
            isPointAdded = this.quadTrees.get(i).addPoint(point);
            if (isPointAdded)
                return true;
        }
        return false;
    }
    private void createQuadrants() {
        Range region;
        for (int i = 0; i < 4; i++) {
            region = this.area.getQuadrant(i);
            quadTrees.add(new Tree(region));
        }
    }
    public Integer search2(Range searchRegion, Integer matches) {
        if (matches == null) {
            matches = 0;
        }
        if (!this.area.doesOverlap(searchRegion)) {
            return matches;
        } else {
            for (Point point : points) {
                if (searchRegion.containsPoint(point)) {
                    matches = matches + 1;
                }
            }
            if (this.quadTrees.size() > 0) {
                for (int i = 0; i < 4; i++) {
                    quadTrees.get(i).search2(searchRegion, matches);
                }
            }
        }
        return matches;
    }
    public List<Point> search(Range searchRegion, List<Point> matches) {

        if (matches == null) {

            matches = new LinkedList<Point>();

        }

        if (!this.area.doesOverlap(searchRegion)) {

            return matches;

        } else {

            for (Point point : points) {

                if (searchRegion.containsPoint(point)) {

                    matches.add(point);

                }

            }

            if (this.quadTrees.size() > 0) {

                for (int i = 0; i < 4; i++) {

                    quadTrees.get(i).search(searchRegion, matches);

                }

            }

        }

        return matches;

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Tree)) return false;
        Tree tree = (Tree) o;
        
        return area.left.equals(tree.area.left) && area.bottom.equals(tree.area.bottom)  && area.right.equals(tree.area.right) && area.top.equals(tree.area.top);
    }

    @Override
    public int hashCode() {
        return Objects.hash(area.left,area. right, area.top, area.bottom);
    }
}