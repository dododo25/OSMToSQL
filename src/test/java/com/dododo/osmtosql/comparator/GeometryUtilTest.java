package com.dododo.osmtosql.comparator;

import net.postgis.jdbc.geometry.LineString;
import net.postgis.jdbc.geometry.LinearRing;
import net.postgis.jdbc.geometry.Point;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

class GeometryUtilTest {

    @Test
    void testJoinShouldReturnLineString() throws SQLException {
        LineString l1 = new LineString("LINESTRING(0 0, 50 50, 100 100)");
        LineString l2 = new LineString("LINESTRING(100 100, 150 150, 200 200)");
        LineString l3 = new LineString("LINESTRING(10 10, 5 5, 0 0)");

        LineString e1 = new LineString("LINESTRING(0 0, 50 50, 100 100, 100 100, 150 150, 200 200)");
        LineString e2 = new LineString("LINESTRING(10 10, 5 5, 0 0, 0 0, 50 50, 100 100)");

        assertEquals(e1, GeometryUtil.join(l1, l2));
        assertEquals(e2, GeometryUtil.join(l3, l1));

        assertThrows(NullPointerException.class, () -> GeometryUtil.join(null, l1));
        assertThrows(NullPointerException.class, () -> GeometryUtil.join(l1, null));
        assertThrows(NullPointerException.class, () -> GeometryUtil.join(null, null));
    }

    @Test
    void testReverseShouldReturnLineString() throws SQLException {
        LineString l1 = new LineString("LINESTRING(0 0, 50 50, 100 100)");
        LineString e1 = new LineString("LINESTRING(100 100, 50 50, 0 0)");

        assertEquals(e1, GeometryUtil.reverse(l1));
        assertThrows(NullPointerException.class, () -> GeometryUtil.reverse(null));
    }

    @Test
    void testEqualsShouldReturnBoolean() throws SQLException {
        assertTrue(GeometryUtil.equals(new LinearRing("0 0, 100 0, 100 100, 0 100"), new LinearRing("0 0, 100 0, 100 100, 0 100")));
        assertTrue(GeometryUtil.equals(new LinearRing("0 0, 100 0, 100 100, 0 100"), new LinearRing("100 0, 100 100, 0 100, 0 0")));
        assertFalse(GeometryUtil.equals(new LinearRing("0 0, 100 0, 100 100, 0 100"), new LinearRing("100 0, 100 100, 0 100, 10 10")));

        assertTrue(GeometryUtil.equals(null, null));
        assertFalse(GeometryUtil.equals(new LinearRing(new Point[0]), null));
        assertFalse(GeometryUtil.equals(null, new LinearRing(new Point[0])));
    }

    @Test
    void testContainsShouldReturnBoolean() throws SQLException {
        assertTrue(GeometryUtil.contains(new LinearRing("0 0, 100 0, 100 100, 0 100"), new LinearRing("10 10, 90 10, 90 90, 10 90")));
        assertFalse(GeometryUtil.contains(new LinearRing("0 0, 100 0, 100 100, 0 100"), new LinearRing("0 0, 100 0, 100 100, 0 100")));
        assertFalse(GeometryUtil.contains(new LinearRing("0 0, 100 0, 100 100, 0 100"), new LinearRing("0 0, 100 0, 110 110, 0 100")));

        assertFalse(GeometryUtil.contains(null, null));
        assertFalse(GeometryUtil.contains(new LinearRing(new Point[0]), null));
        assertFalse(GeometryUtil.contains(null, new LinearRing(new Point[0])));
    }
}
