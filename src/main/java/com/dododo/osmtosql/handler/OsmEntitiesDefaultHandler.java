package com.dododo.osmtosql.handler;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.Objects;

public abstract class OsmEntitiesDefaultHandler extends DefaultHandler {

    @Override
    public final void startElement(String uri, String localName, String qName, Attributes attr) throws SAXException {
        switch (qName.toLowerCase()) {
            case "node":
                startNodeElement(Long.parseLong(attr.getValue("id")),
                        Double.parseDouble(attr.getValue("lat")),
                        Double.parseDouble(attr.getValue("lon")));
                break;
            case "way":
                startWayElement(Long.parseLong(attr.getValue("id")));
                break;
            case "relation":
                startRelationElement(Long.parseLong(attr.getValue("id")));
                break;
            case "nd":
                startNdElement(Long.parseLong(attr.getValue("ref")));
                break;
            case "member":
                startMemberElement(Long.parseLong(attr.getValue("ref")),
                        Objects.requireNonNull(attr.getValue("type")).toLowerCase(),
                        Objects.requireNonNull(attr.getValue("role")));
                break;
            case "tag":
                startTagElement(Objects.requireNonNull(attr.getValue("k")),
                        Objects.requireNonNull(attr.getValue("v")));
                break;
            case "osm":
            case "bounds":
            case "meta":
            case "note":
                break;
            default:
                throw new SAXException(String.format("Unknown tag '%s'", qName));
        }
    }

    @Override
    public final void endElement(String uri, String localName, String qName) throws SAXException {
        switch (qName.toLowerCase()) {
            case "node":
                endNodeElement();
                break;
            case "way":
                endWayElement();
                break;
            case "relation":
                endRelationElement();
                break;
            case "osm":
            case "bounds":
            case "meta":
            case "note":
            case "nd":
            case "member":
            case "tag":
                break;
            default:
                throw new SAXException(String.format("Unknown tag '%s'", qName));
        }
    }

    protected void startNodeElement(long id, double lat, double lon) {

    }

    protected void startWayElement(long id) {

    }

    protected void startRelationElement(long id) {

    }

    protected void startNdElement(long ref) {

    }

    protected void startMemberElement(long ref, String type, String role) {

    }

    protected void startTagElement(String k, String v) {

    }

    protected void endNodeElement() {

    }

    protected void endWayElement() {

    }

    protected void endRelationElement() {

    }
}
