package joe.database;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.bson.types.ObjectId;
import org.neo4j.cypher.ExecutionEngine;
import org.neo4j.cypher.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import static org.neo4j.kernel.impl.util.FileUtils.deleteRecursively;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 *
 * @author Joe O Flaherty
 */
public class Neo4JImplementation implements IRepository {

    long totalTime = 0;
    long averageTime = 0;
    long max = 0;
    long min = 0;
    
    GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();
    GraphDatabaseService db = dbFactory.newEmbeddedDatabase("C:/data/dbTester3");

    //Azure statsD client
    StatsDClient statsdSave = new NonBlockingStatsDClient("Save", "137.135.253.106", 80);
    StatsDClient statsdFind = new NonBlockingStatsDClient("Find", "137.135.253.106", 80);
    StatsDClient statsdDelete = new NonBlockingStatsDClient("Delete", "137.135.253.106", 80);

    //VMWare statsD client
    //StatsDClient statsd = new NonBlockingStatsDClient("Save", "192.168.45.130", 8125);
    public enum Student implements Label {

        COMPUTING, BUSINESS;
    }

    public String save() {
         
        //GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();
        //GraphDatabaseService db = dbFactory.newEmbeddedDatabase("C:/data/dbA2");
        try (org.neo4j.graphdb.Transaction tx = db.beginTx()) {
            for (int i = 1; i <= 100; i++) {
                long duration = System.nanoTime();
                max = duration;
                long startTime = System.nanoTime();
                Node studentNode = db.createNode(Student.COMPUTING);
                studentNode.setProperty("sName", "Joe");
                studentNode.setProperty("sNumber", "1234" + i);
                studentNode.setProperty("sCourse", "Software Dev");
                studentNode.setProperty("createdDate", "Today");
                
                long endTime = System.nanoTime();
                duration = endTime - startTime;
                min = duration;
                if (duration > max) {
                    max = duration;
                }
                duration = endTime - startTime;
                if (duration < min) {
                    min = duration;
                }
                totalTime += duration;

                statsdSave.incrementCounter("Neo4J Save Counter");
                statsdSave.recordGaugeValue("Neo4J Save Gauge", 100);
                statsdSave.recordExecutionTime("Neo4J Save Timer", duration);
                statsdSave.recordSetEvent("Neo4J Save SetEvent", "One");
                statsdSave.recordExecutionTime("Neo4J Save Maximum", max);
                statsdSave.recordExecutionTime("Neo4J Save Minimum", min);

            }
            averageTime = totalTime / 100;
            statsdSave.recordExecutionTime("Neo4J Save Total Time(100)", totalTime);
            statsdSave.recordExecutionTime("Neo4J Save Average Time(100)", averageTime);
            tx.success();
            tx.close();

        }
        
        //db.shutdown();
        return "";

    }

    public String saveUnique() {

        //GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();
        //GraphDatabaseService db = dbFactory.newEmbeddedDatabase("C:/data/dbU3");
        try (org.neo4j.graphdb.Transaction tx = db.beginTx()) {
            for (int i = 1; i <= 100; i++) {
                long startTime = System.nanoTime();
                Node studentNode = db.createNode(Student.COMPUTING);
                studentNode.setProperty("sName", "Joe");
                studentNode.setProperty("sNumber", "1234" + i);
                studentNode.setProperty("sCourse", "Software Dev");
                studentNode.setProperty("createdDate", "Today");
                long endTime = System.nanoTime();
                long duration = endTime - startTime;
                totalTime += duration;

                statsdSave.incrementCounter("Neo4J Save Counter");
                statsdSave.recordGaugeValue("Neo4J Save Gauge", 100);
                statsdSave.recordExecutionTime("Neo4J Save Timer", duration);
                statsdSave.recordSetEvent("Neo4J Save SetEvent", "One");

            }
            averageTime = totalTime / 100;
            statsdSave.recordExecutionTime("Neo4J Save Total Time(100)", totalTime);
            statsdSave.recordExecutionTime("Neo4J Save Average Time(100)", averageTime);
            tx.success();
            tx.close();
        }

        return "";

    }

    public String find() {

        //GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase("C:/data/dbU");
        //ExecutionEngine execEngine;
        //execEngine = new ExecutionEngine(graphDb,StringLogger.SYSTEM);
        long startTime = System.nanoTime();
        // ExecutionResult execResult = execEngine.execute("MATCH n RETURN n");
        //graphDb.getAllNodes();
        //String results = execResult.dumpToString();
        //List<String> columns = (List<String>) execResult.columns();
        System.out.println("in Neo Find");
        long endTime = System.nanoTime();
        long duration = endTime - startTime;

        statsdFind.incrementCounter("Neo4J Find Counter");
        statsdFind.recordGaugeValue("Neo4J Find Gauge", 100);
        statsdFind.recordExecutionTime("Neo4J Find Timer", duration);
        statsdFind.recordSetEvent("Neo4J Find SetEvent", "One");

        return "Found successfully";//+graphDb.getAllNodes().toString();

        //graphDb.shutdown();
    }

    public String findSingle(ObjectId Id) {
        return "";
    }

    public void deleteDir(File dir) {
        File[] files = dir.listFiles();

        for (File myFile : files) {
            if (myFile.isDirectory()) {
                deleteDir(myFile);
            }
            myFile.delete();

        }

    }

    private void clearDbPath() {

        try {
            deleteRecursively(new File("C:/data/dbTest"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
