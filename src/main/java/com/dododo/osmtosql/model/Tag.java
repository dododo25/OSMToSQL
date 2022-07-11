package com.dododo.osmtosql.model;

import com.dododo.osmtosql.service.SaveEntityRequestBuilder;

import java.util.Objects;

public class Tag<T extends OsmEntity> {

    private final T entity;

    private final String key;

    private final String value;

    public Tag(T entity, String key, String value) {
        this.entity = entity;
        this.key = key;
        this.value = value;
    }

    public T getEntity() {
        return entity;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(entity.getId(), key);
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || obj instanceof Tag && equals((Tag<?>) obj);
    }

    private boolean equals(Tag<?> tag) {
        return tag.entity.getClass().equals(this.entity.getClass())
                && tag.entity.getId() == this.entity.getId() && tag.key.equals(this.key);
    }

    public static class SaveEntityTagRequestBuilder<T extends OsmEntity> extends SaveEntityRequestBuilder<Tag<T>> {

        public SaveEntityTagRequestBuilder(String schemaName, String tableName) {
            super(schemaName, tableName);
            mapLong("id", tag -> tag.getEntity().getId());
            mapString("k", Tag::getKey);
            mapString("v", Tag::getValue);
        }
    }
}
