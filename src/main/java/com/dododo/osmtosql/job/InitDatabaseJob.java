package com.dododo.osmtosql.job;

import com.dododo.osmtosql.Constants;
import com.dododo.osmtosql.model.Node;
import com.dododo.osmtosql.model.Relation;
import com.dododo.osmtosql.model.Way;
import com.dododo.osmtosql.service.PostgreSQLService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class InitDatabaseJob extends ComplexAbstractJob {

    private final PostgreSQLService service;

    public InitDatabaseJob(PostgreSQLService service) {
        this.service = service;
    }

    @Override
    protected List<Callable<Integer>> prepareJobs() {
        List<Callable<Integer>> jobs = new ArrayList<>();

        jobs.add(() -> service.dropSchema(Constants.OSM_SCHEMA));
        jobs.add(() -> service.dropSchema(Constants.PARTS_SCHEMA));
        jobs.add(() -> service.createSchema(Constants.OSM_SCHEMA));
        jobs.add(() -> service.createSchema(Constants.PARTS_SCHEMA));
        jobs.add(() -> service.createExtension(Constants.POSTGIS_EXTENSION));
        jobs.add(() -> service.createTable(new Node.CreateNodesTableRequestBuilder(Constants.OSM_SCHEMA)));
        jobs.add(() -> service.createTable(new Node.CreateNodeTagsTableRequestBuilder(Constants.OSM_SCHEMA)));
        jobs.add(() -> service.createTable(new Way.CreateWaysTableRequestBuilder(Constants.OSM_SCHEMA)));
        jobs.add(() -> service.createTable(new Way.CreateWayNodesTableRequestBuilder(Constants.OSM_SCHEMA)));
        jobs.add(() -> service.createTable(new Way.CreateWayTagsTableRequestBuilder(Constants.OSM_SCHEMA)));
        jobs.add(() -> service.createTable(new Relation.CreateTableRequestBuilder(Constants.OSM_SCHEMA)));
        jobs.add(() -> service.createTable(new Relation
                .CreateRelationMembersTableRequestBuilder(Constants.OSM_SCHEMA)));
        jobs.add(() -> service.createTable(new Relation.CreateTagTableRequestBuilder(Constants.OSM_SCHEMA)));
        jobs.add(() -> service.createTable(new Relation
                .CreatePartsRelationMembersTableRequestBuilder(Constants.PARTS_SCHEMA)));
        jobs.add(() -> service.createTable(new Relation.CreatePartsTagTableRequestBuilder(Constants.PARTS_SCHEMA)));

        return jobs;
    }
}
