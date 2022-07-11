package com.dododo.osmtosql.job;

import com.dododo.osmtosql.Constants;
import com.dododo.osmtosql.model.Relation;
import com.dododo.osmtosql.service.PostgreSQLService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class SaveRelationsJob extends AbstractJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(SaveRelationsJob.class);

    private final PostgreSQLService service;

    private final Collection<Relation> relations;

    public SaveRelationsJob(PostgreSQLService service, Collection<Relation> relations) {
        this.service = service;
        this.relations = relations;
    }

    @Override
    public Integer call() throws Exception {
        long startTime = System.currentTimeMillis();

        LOGGER.info("started.");
        service.saveAll(new Relation.SaveRequestBuilder(Constants.OSM_SCHEMA), relations);

        LOGGER.info("finished successfully in {}ms.", System.currentTimeMillis() - startTime);
        return callNext();
    }
}
