package com.dododo.osmtosql.job;

import com.dododo.osmtosql.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

public class CheckRelationCompletenessJob extends AbstractJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(CheckRelationCompletenessJob.class);

    private final Map<Long, Relation> relations;

    private final Collection<Long> nodeIds;

    private final Collection<Long> wayIds;

    public CheckRelationCompletenessJob(Map<Long, Relation> relations, Collection<Long> nodeIds,
                                        Collection<Long> wayIds) {
        this.relations = relations;
        this.nodeIds = nodeIds;
        this.wayIds = wayIds;
    }

    @Override
    public Integer call() throws Exception {
        long startTime = System.currentTimeMillis();

        LOGGER.info("started.");

        Collection<Relation> closedRelations = relations.values().stream()
                .filter(CheckRelationCompletenessJob::containsRelationAsMember)
                .collect(Collectors.toSet());

        Collection<Relation> specialRelations = relations.values().stream()
                .filter(relation -> !containsRelationAsMember(relation))
                .collect(Collectors.toSet());

        validateClosed(closedRelations);
        validateSpecial(specialRelations);

        LOGGER.info("finished successfully in {}ms.", System.currentTimeMillis() - startTime);
        return callNext();
    }

    private void validateClosed(Collection<Relation> relations) {
        for (Relation relation : relations) {
            if (relation.isComplete()) {
                boolean flag = isComplete(relation);

                if (!flag) {
                    relation.setComplete(false);
                }
            }
        }
    }

    private void validateSpecial(Collection<Relation> relations) {
        Collection<Relation> invalidRelations = new HashSet<>();

        for (Relation relation : relations) {
            if (relation.isComplete()) {
                boolean flag = isComplete(relation);

                if (!flag) {
                    relation.setComplete(false);
                    invalidRelations.add(relation);
                }
            }
        }

        if (!invalidRelations.isEmpty()) {
            validateSpecial(relations);
        }
    }

    private boolean isComplete(Relation relation) {
        return !relation.isComplete() || relation.getMembers().stream().allMatch(this::isValid);
    }

    private boolean isValid(Relation.Member member) {
        switch (member.getType().toLowerCase()) {
            case "node":
                return nodeIds.contains(member.getRef());
            case "way":
                return wayIds.contains(member.getRef());
            case "relation":
                return relations.containsKey(member.getRef()) && isComplete(relations.get(member.getRef()));
            default:
                throw new IllegalArgumentException();
        }
    }

    private static boolean containsRelationAsMember(Relation relation) {
        return relation.getMembers().stream()
                .anyMatch(member -> member.getType().equals("relation"));
    }
}
