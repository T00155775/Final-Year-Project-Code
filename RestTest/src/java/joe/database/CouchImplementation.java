package joe.database;

import com.google.gson.JsonObject;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import joe.test.MyPathResource;
import org.bson.types.ObjectId;
import org.lightcouch.CouchDbClient;
import org.lightcouch.Response;

/**
 *
 * @author Joe O Flaherty T00155775
 */
public class CouchImplementation implements IRepository {

    long totalTime = 0;
    long averageTime = 0;
    long max = 0;
    long min = 0;

    //Azure statsD client
    StatsDClient statsdSave = new NonBlockingStatsDClient("Save", "137.135.253.106", 80);
    StatsDClient statsdFind = new NonBlockingStatsDClient("Find", "137.135.253.106", 80);
    StatsDClient statsdDelete = new NonBlockingStatsDClient("Delete", "137.135.253.106", 80);
    //VMWare statsD client
    //StatsDClient statsd = new NonBlockingStatsDClient("Save", "192.168.45.130", 8125);

    /**
     *
     * @return
     */
    @Override
    public String find() {

        List<JsonObject> allDocs = null;
        CouchDbClient dbClient = new CouchDbClient("studentws", true, "http", "127.0.0.1", 5984, "joe", "xc4wbbbb");
        try {
            long duration;

            long startTime = System.nanoTime();
            allDocs = dbClient.view("_all_docs").includeDocs(true).query(JsonObject.class);
            long endTime = System.nanoTime();
            duration = endTime - startTime;

            statsdFind.incrementCounter("CouchDB Find Counter");
            statsdFind.recordGaugeValue("CouchDB Find Gauge", 100);
            statsdFind.recordExecutionTime("CouchDB Find Timer", duration);
            statsdFind.recordSetEvent("CouchDB Find SetEvent", "One");

        } catch (Exception ex) {
            Logger.getLogger(MyPathResource.class.getName()).log(Level.SEVERE, null, ex);
        }
        dbClient.shutdown();
        return allDocs.toString();

    }

    public String saveUnique() throws IOException {

        CouchDbClient dbClient = new CouchDbClient("studentws", true, "http", "127.0.0.1", 5984, "joe", "xc4wbbbb");

        Response resp;
        long duration = System.nanoTime();
        for (int i = 1; i <= 100; i++) {
            JsonObject object = new JsonObject();
            object.addProperty("snumber", "1234567" + i);
            object.addProperty("sname", "student" + i);
            object.addProperty("scourse", "computing");
            object.addProperty("created date", new Date().toString());

            try {

                max = duration;
                long startTime = System.nanoTime();
                resp = dbClient.save(object);
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

                statsdSave.incrementCounter("CouchDB_Save Counter");
                statsdSave.recordGaugeValue("CouchDB_Save Gauge", 100);
                statsdSave.recordExecutionTime("CouchDB_Save Timer", duration);
                statsdSave.recordSetEvent("CouchDB_Save SetEvent", "One");
                statsdSave.recordExecutionTime("CouchDB_Save Maximum", max);
                statsdSave.recordExecutionTime("CouchDB_Save Minimum", min);

            } catch (org.lightcouch.DocumentConflictException e) {
                //insertion of something that already exists causes
                //Exception org.lightcouch.DocumentConflictException: << Status: 409 (Conflict)
                System.out.println(e.getMessage());
            }
            averageTime = totalTime / 100;
            statsdSave.recordExecutionTime("CouchDB_Save Total Time(100)", totalTime);
            statsdSave.recordExecutionTime("CouchDB_Save Average Time(100)", averageTime);
            statsdSave.recordExecutionTime("CouchDB_Save Maximum Final", max);
            statsdSave.recordExecutionTime("CouchDB _Save Minimum Final", min);
        }
        dbClient.shutdown();
        return "Save couchdb successful";
    }

    public String save() {

        CouchDbClient dbClient = new CouchDbClient("studentws", true, "http", "127.0.0.1", 5984, "joe", "xc4wbbbb");

        Response resp;
        for (int i = 1; i <= 100; i++) {
            JsonObject object = new JsonObject();
            object.addProperty("snumber", "1234567" + i);
            object.addProperty("sname", "student" + i);
            object.addProperty("scourse", "computing");
            object.addProperty("created date", new Date().toString());

            try {
                long duration;
                long startTime = System.nanoTime();
                resp = dbClient.save(object);
                long endTime = System.nanoTime();
                duration = endTime - startTime;

                totalTime += duration;

                statsdSave.incrementCounter("CouchDB Save Counter");
                statsdSave.recordGaugeValue("CouchDB Save Gauge", 100);
                statsdSave.recordExecutionTime("CouchDB Save Timer", duration);
                statsdSave.recordSetEvent("CouchDB Save SetEvent", "One");

            } catch (org.lightcouch.DocumentConflictException e) {
                //insertion of something that already exists causes
                //Exception org.lightcouch.DocumentConflictException: << Status: 409 (Conflict)
            }
            averageTime = totalTime / 100;
            statsdSave.recordExecutionTime("CouchDB Save Total Time(100)", totalTime);
            statsdSave.recordExecutionTime("CouchDB Save Average Time(100)", averageTime);

        }
        dbClient.shutdown();
        return "Save couchdb successful";
    }

    public String findSingle(ObjectId Id) {

        return "";
    }

    public String delete() {

        long duration = System.nanoTime();

        //dbClient.shutdown();
        return "in couchDB delete";
    }

}
