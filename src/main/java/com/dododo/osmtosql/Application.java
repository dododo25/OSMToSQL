package com.dododo.osmtosql;

import com.dododo.osmtosql.job.*;
import com.dododo.osmtosql.model.Relation;
import com.dododo.osmtosql.service.PostgreSQLService;
import net.postgis.jdbc.geometry.ComposedGeom;
import net.postgis.jdbc.geometry.Point;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import java.util.concurrent.Callable;

@Command(
        name = "OSMToSQL",
        description = "Converts an OpenStreetMap xml-like format into PostgreSQL db.",
        version = "OSMToSQL v 1.0.1",
        mixinStandardHelpOptions = true
)
public class Application implements Callable<Integer> {

    @Option(names = {"--host"}, description = "JDBC PostgreSQL database connection host name.", required = true, order = 0)
    private String host;

    @Option(names = {"--port"}, description = "JDBC PostgreSQL database connection port.", required = true, order = 1)
    private int port;

    @Option(names = {"--db"}, description = "JDBC PostgreSQL database name.", required = true, order = 2)
    private String db;

    @Option(names = {"--user"}, description = "PostgreSQL database user.", required = true, order = 3)
    private String user;

    @Option(names = {"--password"}, description = "PostgreSQL database password.", required = true, order = 4)
    private String password;

    @Option(names = {"--file"}, description = "Path to file that contains OpenStreetMap data.", required = true, order = 5)
    private String pathname;

    @Override
    public Integer call() throws Exception {
        Map<Long, Point> nodeGeometries = Collections.synchronizedMap(new HashMap<>());
        Map<Long, ComposedGeom> wayGeometries = new HashMap<>();

        Map<Long, Relation> relations = new HashMap<>();

        Connection connection = DriverManager
                .getConnection(String.format("jdbc:postgresql://%s:%s/%s", host, port, db), user, password);
        PostgreSQLService service = new PostgreSQLService(connection);

        SAXParserFactory factory = SAXParserFactory.newInstance();
        File file = new File(pathname);

        AbstractJob job1 = new InitDatabaseJob(service);
        AbstractJob job2 = new SaveNodesJob(factory, service, file, nodeGeometries);
        AbstractJob job3 = new SaveWaysJob(factory, service, file, nodeGeometries, wayGeometries);
        AbstractJob job4 = new CollectRelationsJob(factory, file, relations);
        AbstractJob job5 = new CheckRelationCompletenessJob(relations, nodeGeometries.keySet(),
                wayGeometries.keySet());
        AbstractJob job6 = new PrepareRelationGeometriesJob(relations, nodeGeometries, wayGeometries);
        AbstractJob job7 = new SaveRelationsComplexJob(service, relations.values());
        AbstractJob job8 = new SaveRelationPartsComplexJob(service, relations.values());

        job1.setNext(job2);
        job2.setNext(job3);
        job3.setNext(job4);
        job4.setNext(job5);
        job5.setNext(job6);
        job6.setNext(job7);
        job7.setNext(job8);

        return job1.call();
    }

    public static void main(String[] args) {
        CommandLine cmd = new CommandLine(new Application());
        int exitCode = cmd.execute(args);
        System.exit(exitCode);
    }
}
