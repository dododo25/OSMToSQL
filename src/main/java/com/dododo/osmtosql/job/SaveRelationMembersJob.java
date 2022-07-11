package com.dododo.osmtosql.job;

import com.dododo.osmtosql.model.Relation;
import com.dododo.osmtosql.service.PostgreSQLService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class SaveRelationMembersJob extends AbstractJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(SaveRelationMembersJob.class);

    private final PostgreSQLService service;

    private final Relation.SaveRelationMembersRequestBuilder builder;

    private final Collection<Relation.Member> members;

    public SaveRelationMembersJob(PostgreSQLService service, Relation.SaveRelationMembersRequestBuilder builder,
                                  Collection<Relation.Member> members) {
        this.service = service;
        this.builder = builder;
        this.members = members;
    }

    @Override
    public Integer call() throws Exception {
        long startTime = System.currentTimeMillis();

        LOGGER.info("started.");
        service.saveAll(builder, members);

        LOGGER.info("finished successfully in {}ms.", System.currentTimeMillis() - startTime);
        return next != null ? next.call() : 0;
    }
}
