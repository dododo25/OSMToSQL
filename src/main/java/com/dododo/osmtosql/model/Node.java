package com.dododo.osmtosql.model;

import com.dododo.osmtosql.Constants;
import com.dododo.osmtosql.service.CreateTableRequestBuilder;
import com.dododo.osmtosql.service.SaveEntityRequestBuilder;
import net.postgis.jdbc.geometry.Point;

import java.util.Collection;
import java.util.HashSet;

public class Node implements OsmEntity {

    private final long id;

    private final double lat;

    private final double lon;

    private final Point geom;

    private final Collection<Tag<Node>> tags;

    public Node(long id, double lat, double lon) {
        this.id = id;
        this.lat = lat;
        this.lon = lon;
        this.geom = new Point(lon, lat);
        this.tags = new HashSet<>();
    }

    @Override
    public long getId() {
        return id;
    }

    public double getLatitude() {
        return lat;
    }

    public double getLongitude() {
        return lon;
    }

    @Override
    public Point getGeom() {
        return geom;
    }

    public Collection<Tag<Node>> getTags() {
        return tags;
    }

    public void addTag(String key, String value) {
        tags.add(new Tag<>(this, key, value));
    }

    public static class CreateNodesTableRequestBuilder extends CreateTableRequestBuilder {

        public CreateNodesTableRequestBuilder(String schemaName) {
            super(schemaName, Constants.NODES_TABLE);
            addPrimaryColumn("id", Type.BIGINT, null, null);
            addColumn("lat", Type.DECIMAL, 10, 7, false);
            addColumn("lon", Type.DECIMAL, 10, 7, false);
            addColumn("geom", Type.GEOMETRY, null, null, false);
        }
    }

    public static class CreateNodeTagsTableRequestBuilder extends CreateTableRequestBuilder {

        public CreateNodeTagsTableRequestBuilder(String schemaName) {
            super(schemaName, Constants.NODE_TAGS_TABLE);
            addPrimaryColumn("id", Type.BIGINT, null, null);
            addPrimaryColumn("k", Type.VARCHAR, 255, null);
            addColumn("v", Type.VARCHAR, 255, null, true);
            asForeignColumn("id", schemaName, Constants.NODES_TABLE, "id");
        }
    }

    public static class SaveRequestBuilder extends SaveEntityRequestBuilder<Node> {

        public SaveRequestBuilder(String schemaName) {
            super(schemaName, Constants.NODES_TABLE);
            mapLong("id", Node::getId);
            mapDouble("lat", Node::getLatitude);
            mapDouble("lon", Node::getLongitude);
            mapGeometry("geom", Node::getGeom);
        }
    }
}
