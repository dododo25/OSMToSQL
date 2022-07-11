package com.dododo.osmtosql.job;

import com.dododo.osmtosql.Constants;
import com.dododo.osmtosql.handler.OsmEntitiesDefaultHandler;
import com.dododo.osmtosql.model.Node;
import com.dododo.osmtosql.model.Tag;
import com.dododo.osmtosql.service.PostgreSQLService;
import net.postgis.jdbc.geometry.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.util.Map;

public class SaveNodesJob extends AbstractJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(SaveNodesJob.class);

    private final SAXParserFactory factory;

    private final PostgreSQLService service;

    private final File file;

    private final Map<Long, Point> geometries;

    public SaveNodesJob(SAXParserFactory factory, PostgreSQLService service, File file,
                        Map<Long, Point> geometries) {
        this.factory = factory;
        this.service = service;
        this.file = file;
        this.geometries = geometries;
    }

    @Override
    public Integer call() throws Exception {
        long startTime = System.currentTimeMillis();

        SAXParser saxParser = factory.newSAXParser();
        DefaultHandler handler = new OsmNodesDefaultHandler();

        LOGGER.info("started.");
        saxParser.parse(file, handler);

        LOGGER.info("{} entities were saved.", geometries.size());
        LOGGER.info("finished successfully in {}ms.", System.currentTimeMillis() - startTime);

        return callNext();
    }

    private class OsmNodesDefaultHandler extends OsmEntitiesDefaultHandler {

        private Node lastNode;

        @Override
        protected void startNodeElement(long id, double lat, double lon) {
            lastNode = new Node(id, lat, lon);
        }

        @Override
        protected void startTagElement(String k, String v) {
            if (lastNode != null) {
                lastNode.addTag(k, v);
            }
        }

        @Override
        protected void endNodeElement() {
            service.save(new Node.SaveRequestBuilder(Constants.OSM_SCHEMA), lastNode);
            service.saveAll(new Tag.SaveEntityTagRequestBuilder<>(Constants.OSM_SCHEMA, Constants.NODE_TAGS_TABLE),
                    lastNode.getTags());

            geometries.put(lastNode.getId(), lastNode.getGeom());

            lastNode = null;
        }
    }
}
