package com.dododo.osmtosql.job;

import com.dododo.osmtosql.handler.OsmEntitiesDefaultHandler;
import com.dododo.osmtosql.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.util.Map;

public class CollectRelationsJob extends AbstractJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(CollectRelationsJob.class);

    private final SAXParserFactory factory;

    private final File file;

    private final Map<Long, Relation> relations;

    public CollectRelationsJob(SAXParserFactory factory, File file, Map<Long, Relation> relations) {
        this.factory = factory;
        this.file = file;
        this.relations = relations;
    }
    @Override
    public Integer call() throws Exception {
        long startTime = System.currentTimeMillis();

        SAXParser saxParser = factory.newSAXParser();
        DefaultHandler handler = new OsmRelationsDefaultHandler();

        LOGGER.info("started.");
        saxParser.parse(file, handler);

        LOGGER.info("{} relations entities were collected.", relations.size());
        LOGGER.info("finished successfully in {}ms.", System.currentTimeMillis() - startTime);

        return callNext();
    }

    public class OsmRelationsDefaultHandler extends OsmEntitiesDefaultHandler {

        private Relation lastRelation;

        @Override
        protected void startRelationElement(long id) {
            lastRelation = new Relation(id);
        }

        @Override
        protected void startMemberElement(long ref, String type, String role) {
            lastRelation.addMember(ref, type, role);
        }

        @Override
        protected void startTagElement(String k, String v) {
            if (lastRelation != null) {
                lastRelation.addTag(k, v);
            }
        }

        @Override
        protected void endRelationElement() {
            relations.put(lastRelation.getId(), lastRelation);
            lastRelation = null;
        }
    }
}
