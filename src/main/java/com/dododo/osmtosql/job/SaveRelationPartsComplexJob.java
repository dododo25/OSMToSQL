package com.dododo.osmtosql.job;

import com.dododo.osmtosql.Constants;
import com.dododo.osmtosql.model.Relation;
import com.dododo.osmtosql.model.Tag;
import com.dododo.osmtosql.service.PostgreSQLService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class SaveRelationPartsComplexJob extends ComplexAbstractJob {

    private final PostgreSQLService service;

    private final Collection<Relation> relations;

    public SaveRelationPartsComplexJob(PostgreSQLService service, Collection<Relation> relations) {
        this.service = service;
        this.relations = relations;
    }

    @Override
    protected List<Callable<Integer>> prepareJobs() {
        List<Callable<Integer>> jobs = new ArrayList<>();

        Collection<Relation> filtered = relations.stream().filter(relation -> !relation.isComplete())
                .collect(Collectors.toSet());

        Relation.SaveRelationMembersRequestBuilder saveMembersRequestBuilder = new Relation
                .SaveRelationMembersRequestBuilder(Constants.PARTS_SCHEMA);
        Tag.SaveEntityTagRequestBuilder<Relation> saveTagsRequestBuilder = new Tag.SaveEntityTagRequestBuilder<>(
                Constants.PARTS_SCHEMA, Constants.RELATION_TAGS_TABLE);

        jobs.add(new SaveRelationMembersJob(service, saveMembersRequestBuilder, filtered
                .stream().flatMap(relation -> relation.getMembers().stream()).collect(Collectors.toSet())));
        jobs.add(new SaveRelationTagsJob(service, saveTagsRequestBuilder, filtered
                .stream().flatMap(relation -> relation.getTags().stream()).collect(Collectors.toSet())));

        return jobs;
    }
}
