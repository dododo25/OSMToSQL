package com.dododo.osmtosql.model;

import net.postgis.jdbc.geometry.Geometry;

public interface OsmEntity {

    long getId();

    Geometry getGeom();
}
