package com.dododo.osmtosql.model;

import com.dododo.osmtosql.Constants;
import com.dododo.osmtosql.service.CreateTableRequestBuilder;
import com.dododo.osmtosql.service.SaveEntityRequestBuilder;
import net.postgis.jdbc.geometry.ComposedGeom;

import java.util.*;

public class Way implements OsmEntity {

    private final long id;

    private final List<NodeRef> refs;

    private final Collection<Tag<Way>> tags;

    private ComposedGeom geom;

    public Way(long id) {
        this.id = id;
        this.refs = new ArrayList<>();
        this.tags = new HashSet<>();
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public ComposedGeom getGeom() {
        return geom;
    }

    public void setGeom(ComposedGeom geom) {
        this.geom = geom;
    }

    public List<NodeRef> getRefs() {
        return refs;
    }

    public void addNodeRef(long ref) {
        refs.add(new NodeRef(ref, (short) refs.size()));
    }

    public Collection<Tag<Way>> getTags() {
        return tags;
    }

    public void addTag(String key, String value) {
        tags.add(new Tag<>(this, key, value));
    }

    public class NodeRef {

        private final long nodeId;

        private final short index;

        private NodeRef(long nodeId, short index) {
            this.nodeId = nodeId;
            this.index = index;
        }

        public long getWayId() {
            return Way.this.id;
        }

        public long getNodeId() {
            return nodeId;
        }

        public short getIndex() {
            return index;
        }
    }

    public static class CreateWaysTableRequestBuilder extends CreateTableRequestBuilder {

        public CreateWaysTableRequestBuilder(String schemaName) {
            super(schemaName, Constants.WAYS_TABLE);
            addPrimaryColumn("id", Type.BIGINT, null, null);
            addColumn("geom", Type.GEOMETRY, null, null, false);
        }
    }

    public static class CreateWayNodesTableRequestBuilder extends CreateTableRequestBuilder {

        public CreateWayNodesTableRequestBuilder(String schemaName) {
            super(schemaName, Constants.WAY_NODES_TABLE);
            addColumn("id", Type.BIGINT, null, null, false);
            addColumn("node_id", Type.BIGINT, null, null, false);
            addColumn("index", Type.SMALLINT, null, null, false);
            asForeignColumn("id", schemaName, Constants.WAYS_TABLE, "id");
            asForeignColumn("node_id", schemaName, Constants.NODES_TABLE, "id");
        }
    }

    public static class CreateWayTagsTableRequestBuilder extends CreateTableRequestBuilder {

        public CreateWayTagsTableRequestBuilder(String schemaName) {
            super(schemaName, Constants.WAY_TAGS_TABLE);
            addPrimaryColumn("id", Type.BIGINT, null, null);
            addPrimaryColumn("k", Type.VARCHAR, 255, null);
            addColumn("v", Type.VARCHAR, 255, null, true);
            asForeignColumn("id", schemaName, Constants.WAYS_TABLE, "id");
        }
    }

    public static class SaveRequestBuilder extends SaveEntityRequestBuilder<Way> {

        public SaveRequestBuilder(String schemaName) {
            super(schemaName, Constants.WAYS_TABLE);
            mapLong("id", Way::getId);
            mapGeometry("geom", Way::getGeom);
        }
    }

    public static class SaveWayNodesRequestBuilder extends SaveEntityRequestBuilder<NodeRef> {

        public SaveWayNodesRequestBuilder(String schemaName) {
            super(schemaName, Constants.WAY_NODES_TABLE);
            mapLong("id", NodeRef::getWayId);
            mapLong("node_id", NodeRef::getNodeId);
            mapShort("index", NodeRef::getIndex);
        }
    }
}
