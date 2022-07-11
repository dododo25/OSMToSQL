package com.dododo.osmtosql.model;

import com.dododo.osmtosql.Constants;
import com.dododo.osmtosql.service.SaveEntityRequestBuilder;
import net.postgis.jdbc.geometry.Geometry;

import java.util.Collection;
import java.util.HashSet;

public class Relation implements OsmEntity {

    private final long id;

    private Geometry geom;

    private boolean complete;

    private final Collection<Member> members;

    private final Collection<Tag<Relation>> tags;

    public Relation(long id) {
        this.id = id;
        this.complete = true;
        this.members = new HashSet<>();
        this.tags = new HashSet<>();
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public Geometry getGeom() {
        return geom;
    }

    public void setGeom(Geometry geom) {
        this.geom = geom;
    }

    public boolean isComplete() {
        return complete;
    }

    public void setComplete(boolean complete) {
        this.complete = complete;
    }

    public Collection<Member> getMembers() {
        return members;
    }

    public void addMember(long ref, String type, String role) {
        members.add(new Member(ref, type, role));
    }

    public Collection<Tag<Relation>> getTags() {
        return tags;
    }

    public void addTag(String key, String value) {
        tags.add(new Tag<>(this, key, value));
    }

    public class Member {

        private final long ref;

        private final String type;

        private final String role;

        public Member(long ref, String type, String role) {
            this.ref = ref;
            this.type = type;
            this.role = role;
        }

        public long getRelationId() {
            return Relation.this.id;
        }

        public long getRef() {
            return ref;
        }

        public String getType() {
            return type;
        }

        public String getRole() {
            return role;
        }
    }

    public static class CreateTableRequestBuilder extends com.dododo.osmtosql.service.CreateTableRequestBuilder {

        public CreateTableRequestBuilder(String schemaName) {
            super(schemaName, Constants.RELATIONS_TABLE);
            addPrimaryColumn("id", Type.BIGINT, null, null);
            addColumn("geom", Type.GEOMETRY, null, null, false);
        }
    }

    public static class CreateRelationMembersTableRequestBuilder
            extends CreatePartsRelationMembersTableRequestBuilder {

        public CreateRelationMembersTableRequestBuilder(String schemaName) {
            super(schemaName);
            asForeignColumn("id", schemaName, Constants.RELATIONS_TABLE, "id");
        }
    }

    public static class CreatePartsRelationMembersTableRequestBuilder
            extends com.dododo.osmtosql.service.CreateTableRequestBuilder {

        public CreatePartsRelationMembersTableRequestBuilder(String schemaName) {
            super(schemaName, Constants.RELATION_MEMBERS_TABLE);
            addColumn("id", Type.BIGINT, null, null, false);
            addColumn("member_id", Type.BIGINT, null, null, false);
            addColumn("role", Type.VARCHAR, 255, null, true);
            addColumn("type", Type.VARCHAR, 8, null, false);
        }
    }

    public static class CreateTagTableRequestBuilder extends CreatePartsTagTableRequestBuilder {

        public CreateTagTableRequestBuilder(String schemaName) {
            super(schemaName);
            asForeignColumn("id", schemaName, Constants.RELATIONS_TABLE, "id");
        }
    }

    public static class CreatePartsTagTableRequestBuilder
            extends com.dododo.osmtosql.service.CreateTableRequestBuilder {

        public CreatePartsTagTableRequestBuilder(String schemaName) {
            super(schemaName, Constants.RELATION_TAGS_TABLE);
            addPrimaryColumn("id", Type.BIGINT, null, null);
            addPrimaryColumn("k", Type.VARCHAR, 255, null);
            addColumn("v", Type.VARCHAR, 255, null, true);
        }
    }

    public static class SaveRequestBuilder extends SaveEntityRequestBuilder<Relation> {

        public SaveRequestBuilder(String schemaName) {
            super(schemaName, Constants.RELATIONS_TABLE);
            mapLong("id", Relation::getId);
            mapGeometry("geom", Relation::getGeom);
        }
    }

    public static class SaveRelationMembersRequestBuilder extends SaveEntityRequestBuilder<Member> {

        public SaveRelationMembersRequestBuilder(String schemaName) {
            super(schemaName, Constants.RELATION_MEMBERS_TABLE);
            mapLong("id", Member::getRelationId);
            mapLong("member_id", Member::getRef);
            mapString("type", Member::getType);
            mapString("role", Member::getRole);
        }
    }
}
