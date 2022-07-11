package com.dododo.osmtosql.job;

import com.dododo.osmtosql.model.Relation;
import com.dododo.osmtosql.model.Tag;
import com.dododo.osmtosql.service.PostgreSQLService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class SaveRelationTagsJob extends AbstractJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(SaveRelationTagsJob.class);

    private final PostgreSQLService service;

    private final Tag.SaveEntityTagRequestBuilder<Relation> builder;

    private final Collection<Tag<Relation>> tags;

    public SaveRelationTagsJob(PostgreSQLService service, Tag.SaveEntityTagRequestBuilder<Relation> builder,
                               Collection<Tag<Relation>> tags) {
        this.service = service;
        this.builder = builder;
        this.tags = tags;
    }

    @Override
    public Integer call() throws Exception {
        long startTime = System.currentTimeMillis();

        LOGGER.info("started.");
        service.saveAll(builder, tags);

        LOGGER.info("finished successfully in {}ms.", System.currentTimeMillis() - startTime);
        return callNext();
    }
}
