package com.dododo.osmtosql.comparator;

import net.postgis.jdbc.geometry.*;

import java.util.Objects;
import java.util.stream.IntStream;

public class GeometryUtil {

    private GeometryUtil() {}

    public static LineString join(LineString l1, LineString l2) {
        Point[] result = new Point[Objects.requireNonNull(l1).numPoints() + Objects.requireNonNull(l2).numPoints()];

        IntStream.range(0, l1.numPoints())
                .forEach(index -> result[index] = l1.getPoint(index));
        IntStream.range(0, l2.numPoints())
                .forEach(index -> result[l1.numPoints() + index] = l2.getPoint(index));

        return new LineString(result);
    }

    public static LineString reverse(LineString l) {
        Point[] result = new Point[Objects.requireNonNull(l).numPoints()];

        IntStream.iterate(l.numPoints() - 1, index -> index - 1)
                .limit(l.numPoints())
                .forEach(index -> result[l.numPoints() - index - 1] = l.getPoint(index));

        return new LineString(result);
    }

    public static boolean equals(LinearRing r1, LinearRing r2) {
        if (r1 == null && r2 == null) {
            return true;
        } else if (r1 == null ^ r2 == null) {
            return false;
        }

        for (int i = 0; i < r1.numPoints(); i++) {
            if (r1.getPoint(i).equals(r2.getPoint(0))) {
                return equals(r1, r2, i);
            }
        }

        return true;
    }

    public static boolean contains(LinearRing r1, LinearRing r2) {
        if (r1 == null || r2 == null) {
            return false;
        }

        javafx.scene.shape.Polygon polygon = convert(r1);

        return IntStream.range(0, r2.numPoints())
                .mapToObj(r2::getPoint)
                .allMatch(point -> polygon.contains(point.x, point.y));
    }

    private static boolean equals(LinearRing r1, LinearRing r2, int shift) {
        for (int i = 0; i < r2.numPoints(); i++) {
            if (!r1.getPoint((shift + i) % r1.numPoints()).equals(r2.getPoint(i))) {
                return false;
            }
        }

        return true;
    }

    private static javafx.scene.shape.Polygon convert(Geometry geom) {
        double[] points = new double[geom.numPoints() * 2];

        for (int i = 0; i < geom.numPoints(); i++) {
            points[i * 2] = geom.getPoint(i).x;
            points[i * 2 + 1] = geom.getPoint(i).y;
        }

        return new javafx.scene.shape.Polygon(points);
    }
}
