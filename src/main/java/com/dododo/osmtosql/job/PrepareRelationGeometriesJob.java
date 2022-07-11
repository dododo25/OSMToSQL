package com.dododo.osmtosql.job;

import com.dododo.osmtosql.comparator.GeometryUtil;
import com.dododo.osmtosql.model.*;
import net.postgis.jdbc.geometry.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PrepareRelationGeometriesJob extends AbstractJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(PrepareRelationGeometriesJob.class);

    private final Map<Long, Relation> relations;

    private final Map<Long, Point> nodeGeometries;

    private final Map<Long, ComposedGeom> wayGeometries;

    public PrepareRelationGeometriesJob(Map<Long, Relation> relations, Map<Long, Point> nodeGeometries,
                                        Map<Long, ComposedGeom> wayGeometries) {
        this.relations = relations;
        this.nodeGeometries = nodeGeometries;
        this.wayGeometries = wayGeometries;
    }

    @Override
    public Integer call() throws Exception {
        long startTime = System.currentTimeMillis();

        LOGGER.info("started.");
        relations.values().stream().filter(Relation::isComplete)
                .forEach(relation -> prepareGeometry(relation, new HashSet<>()));

        LOGGER.info("finished successfully in {}ms.", System.currentTimeMillis() - startTime);
        return callNext();
    }

    private void prepareGeometry(Relation relation, Collection<Relation> processed) {
        if (processed.contains(relation)) {
            return;
        }

        Collection<Relation.Member> members = relation.getMembers();

        members.stream().filter(member -> member.getType().equals("relation"))
                .forEach(member -> prepareGeometry(relations.get(member.getRef()), processed));

        List<Geometry> mainGeometries = members.stream()
                .filter(member -> !member.getRole().matches("inner|outer"))
                .map(this::getGeometry)
                .collect(Collectors.toList());

        Collection<LinearRing> outerRings = new HashSet<>();
        Collection<LinearRing> innerRings = new HashSet<>();

        prepareRings(members.stream()
                .filter(member -> member.getRole().equalsIgnoreCase("outer"))
                .map(this::getGeometry).collect(Collectors.toSet()), outerRings);
        prepareRings(members.stream()
                .filter(member -> member.getRole().equalsIgnoreCase("inner"))
                .map(this::getGeometry).collect(Collectors.toSet()), innerRings);

        outerRings.forEach(ring -> {
            List<LinearRing> rings = new ArrayList<>();

            rings.add(ring);

            innerRings.forEach(innerRing -> {
                if (GeometryUtil.contains(ring, innerRing)) {
                    rings.add(innerRing);
                }
            });

            mainGeometries.add(new Polygon(rings.toArray(new LinearRing[0])));
        });

        if (mainGeometries.size() == 1) {
            relation.setGeom(mainGeometries.get(0));
        } else {
            relation.setGeom(new GeometryCollection(mainGeometries.toArray(new Geometry[0])));
        }

        processed.add(relation);
    }

    private Geometry getGeometry(Relation.Member member) {
        switch (member.getType().toLowerCase()) {
            case "node":
                return nodeGeometries.get(member.getRef());
            case "way":
                return wayGeometries.get(member.getRef());
            case "relation":
                return relations.get(member.getRef()).getGeom();
            default:
                throw new IllegalArgumentException();
        }
    }

    private static void prepareRings(Collection<Geometry> geometries, Collection<LinearRing> rings) {
        Collection<Geometry> geometriesToAdd = new HashSet<>();
        Collection<Geometry> geometriesToRemove = new HashSet<>();

        for (Geometry geometry : geometries) {
            if (geometry.getFirstPoint().equals(geometry.getLastPoint())) {
                LinearRing ring = new LinearRing(IntStream.range(0, geometry.numPoints())
                        .mapToObj(geometry::getPoint).toArray(Point[]::new));

                rings.add(ring);

                geometriesToRemove.add(geometry);
            } else {
                LineString lineString = prepareGeometry(geometry, geometries);

                if (lineString != null) {
                    geometriesToRemove.add(lineString);
                    geometriesToRemove.add(geometry);

                    if (lineString.getLastPoint().equals(geometry.getFirstPoint())) {
                        geometriesToAdd.add(GeometryUtil.join(lineString, (LineString) geometry));
                    } else if (lineString.getLastPoint().equals(geometry.getLastPoint())) {
                        geometriesToAdd.add(GeometryUtil
                                .join(lineString, GeometryUtil.reverse((LineString) geometry)));
                    }

                    break;
                }
            }
        }

        geometries.removeAll(geometriesToRemove);
        geometries.addAll(geometriesToAdd);

        if (!geometries.isEmpty()) {
            prepareRings(geometries, rings);
        }
    }

    private static LineString prepareGeometry(Geometry geom, Collection<Geometry> geometries) {
        for (Geometry anotherGeometry : geometries) {
            if (anotherGeometry == geom) {
                continue;
            }

            if (anotherGeometry.getLastPoint().equals(geom.getFirstPoint())
                    || anotherGeometry.getLastPoint().equals(geom.getLastPoint())) {
                return (LineString) anotherGeometry;
            }
        }

        return null;
    }
}
