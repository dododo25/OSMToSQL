package com.dododo.osmtosql.job;

import com.dododo.osmtosql.Constants;
import com.dododo.osmtosql.handler.OsmEntitiesDefaultHandler;
import com.dododo.osmtosql.model.Tag;
import com.dododo.osmtosql.model.Way;
import com.dododo.osmtosql.service.PostgreSQLService;
import net.postgis.jdbc.geometry.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

public class SaveWaysJob extends AbstractJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(SaveWaysJob.class);

    private final PostgreSQLService service;

    private final SAXParserFactory factory;

    private final File file;

    private final Map<Long, Point> nodeGeometries;

    private final Map<Long, ComposedGeom> wayGeometries;

    public SaveWaysJob(SAXParserFactory factory, PostgreSQLService service, File file,
                       Map<Long, Point> nodeGeometries, Map<Long, ComposedGeom> wayGeometries) {
        this.factory = factory;
        this.service = service;
        this.file = file;
        this.nodeGeometries = nodeGeometries;
        this.wayGeometries = wayGeometries;
    }

    @Override
    public Integer call() throws Exception {
        long startTime = System.currentTimeMillis();

        SAXParser saxParser = factory.newSAXParser();
        DefaultHandler handler = new OsmWaysDefaultHandler();

        LOGGER.info("started.");
        saxParser.parse(file, handler);

        LOGGER.info("{} entities were saved.", wayGeometries.size());
        LOGGER.info("finished successfully in {}ms.", System.currentTimeMillis() - startTime);
        
        return callNext();
    }

    public class OsmWaysDefaultHandler extends OsmEntitiesDefaultHandler {

        private Way lastWay;

        @Override
        protected void startWayElement(long id) {
            lastWay = new Way(id);
        }

        @Override
        protected void startNdElement(long ref) {
            lastWay.addNodeRef(ref);
        }

        @Override
        protected void startTagElement(String k, String v) {
            if (lastWay != null) {
                lastWay.addTag(k, v);
            }
        }

        @Override
        protected void endWayElement() {
            prepareGeometry(lastWay);

            service.save(new Way.SaveRequestBuilder(Constants.OSM_SCHEMA), lastWay);
            service.saveAll(new Way.SaveWayNodesRequestBuilder(Constants.OSM_SCHEMA), lastWay.getRefs());
            service.saveAll(new Tag.SaveEntityTagRequestBuilder<>(Constants.OSM_SCHEMA, Constants.WAY_TAGS_TABLE),
                    lastWay.getTags());

            wayGeometries.put(lastWay.getId(), lastWay.getGeom());

            lastWay = null;
        }

        private void prepareGeometry(Way way) {
            LinkedList<Point> points = way.getRefs()
                    .stream()
                    .map(Way.NodeRef::getNodeId)
                    .map(nodeGeometries::get)
                    .collect(Collectors.toCollection(LinkedList::new));

            if (Objects.equals(points.getFirst(), points.getLast()) && points.size() > 2) {
                way.setGeom(new Polygon(new LinearRing[]{new LinearRing(points.toArray(new Point[0]))}));
            } else {
                way.setGeom(new LineString(points.toArray(new Point[0])));
            }
        }
    }
}
