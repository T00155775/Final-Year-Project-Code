package joe.database;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import joe.test.MyPathResource;
import org.bson.types.ObjectId;

/**
 *
 * @author Joe O Flaherty
 */
public class MongoImplementation implements IRepository {

    private MongoClient client;
    private DB testDB;
    private DBCollection testCollection;
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
    public String find() {

        DBCursor cursor = null;
        try {
            long duration;
            MongoClient mongo = new MongoClient("localhost", 27017);
            DB db = mongo.getDB("testWs");
            DBCollection collection = db.getCollection("studentWs");

            long startTime = System.nanoTime();
            cursor = collection.find();
            long endTime = System.nanoTime();
            duration = endTime - startTime;

            statsdFind.incrementCounter("MongoDB Find Counter");
            statsdFind.recordGaugeValue("MongoDB Find Gauge", 100);
            statsdFind.recordExecutionTime("MongoDB Find Timer", duration);
            statsdFind.recordSetEvent("MongoDB Find SetEvent", "One");

        } catch (UnknownHostException ex) {
            Logger.getLogger(MyPathResource.class.getName()).log(Level.SEVERE, null, ex);
        }
        String a = "";
        try {
            while (cursor.hasNext()) {
                a = a + cursor.next() + "\n";
            }
        } finally {
            cursor.close();
        }
        return a;

    }

    public String save() {
        DBCursor cursor;
        long duration = System.nanoTime();

        try {
            MongoClient mongo = new MongoClient("localhost", 27017);

            //db will be created if it does not exist
            DB db = mongo.getDB("testWs");

            DBCollection collection = db.getCollection("studentWs");
            for (int i = 1; i <= 100; i++) {
                BasicDBObject document = new BasicDBObject();

                document.put("sname" + i, "Mongo Save All");
                document.put("snumber", "Testing");
                document.put("scourse", "Testing");
                document.put("createdDate", new Date());

                max = duration;
                long startTime = System.nanoTime();
                collection.insert(document);
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

                statsdSave.incrementCounter("MongoDB Save Counter");
                statsdSave.recordGaugeValue("MongoDB Save Gauge", 100);
                statsdSave.recordExecutionTime("MongoDB Save Timer", duration);
                statsdSave.recordSetEvent("MongoDB Save SetEvent", "One");
                statsdSave.recordExecutionTime("MongoDB Save Maximum", max);
                statsdSave.recordExecutionTime("MongoDB Save Minimum", min);
            }
        } catch (UnknownHostException ex) {
            Logger.getLogger(MyPathResource.class.getName()).log(Level.SEVERE, null, ex);
        }
        averageTime = totalTime / 100;
        statsdSave.recordExecutionTime("MongoDB Save Total Time(100)", totalTime);
        statsdSave.recordExecutionTime("MongoDB Save Average Time(100)", averageTime);

        return "in Mongo Save " + duration + " nanoseconds";

    }

    // Method used to find a specific document in a collection based on its _id
    /**
     *
     * @param id
     * @return
     */
    @Override
    public String findSingle(ObjectId id) {

        DBCursor cursor = null;

        try {
            MongoClient mongo = new MongoClient("localhost", 27017);

            //db will be created if it does not exist
            DB db = mongo.getDB("testWs");

            DBCollection collection = db.getCollection("studentWs");

            BasicDBObject searchQuery = new BasicDBObject("_id", id);
            //searchQuery.put("_id",id);

            long startTime = System.nanoTime();
            cursor = collection.find(searchQuery);
            long endTime = System.nanoTime();
            long duration = endTime - startTime;

            statsdFind.incrementCounter("Mongo_Find One Counter");
            statsdFind.recordGaugeValue("Mongo_Find One Gauge", 100);
            statsdFind.recordExecutionTime("Mongo_Find One Timer", duration);
            statsdFind.recordSetEvent("Mongo_Find One SetEvent", "One");

        } catch (UnknownHostException ex) {
            Logger.getLogger(MyPathResource.class.getName()).log(Level.SEVERE, null, ex);
        }
        String a = "In findByID " + cursor;
        /* try {
         while(cursor.hasNext()) {
         a = a + cursor.next()+"\n";
         }
         } finally {
         cursor.close();
                    
         } */
        return a;
    }

    public String saveUnique() {

        long duration = System.nanoTime();

        try {
            MongoClient mongo = new MongoClient("localhost", 27017);

            //db will be created if it does not exist
            DB db = mongo.getDB("testWs");

            DBCollection collection = db.getCollection("studentWs");
            for (int i = 1; i <= 100; i++) {
                BasicDBObject document = new BasicDBObject();
                document.put("sname" + i, "Mongo Unique Save");
                document.put("snumber", "Testing");
                document.put("scourse", "Testing");
                document.put("createdDate", new Date());

                max = duration;
                long startTime = System.nanoTime();
                collection.insert(document);
                long endTime = System.nanoTime();
                duration = endTime - startTime;
                min = duration;
                if (duration > max) {
                    max = duration;
                }
                if (duration < min) {
                    min = duration;
                }
                totalTime += duration;

                statsdSave.incrementCounter("MongoDB Save Counter");
                statsdSave.recordGaugeValue("MongoDB Save Gauge", 100);
                statsdSave.recordExecutionTime("MongoDB Save Timer", duration);
                statsdSave.recordSetEvent("MongoDB Save SetEvent", "One");
                statsdSave.recordExecutionTime("MongoDB Save Maximum", max);
                statsdSave.recordExecutionTime("MongoDB Save Minimum", min);
            }
        } catch (UnknownHostException ex) {
            Logger.getLogger(MyPathResource.class.getName()).log(Level.SEVERE, null, ex);
        }
        averageTime = totalTime / 100;
        statsdSave.recordExecutionTime("MongoDB  Save Total Time(100)", totalTime);
        statsdSave.recordExecutionTime("MongoDB Save Average Time(100)", averageTime);
        
        return "in Save Unique " + duration + " nanoseconds";

    }

    public String delete() {

        long duration = System.nanoTime();

        try {
            MongoClient mongo = new MongoClient("localhost", 27017);

            //db will be created if it does not exist
            DB db = mongo.getDB("testWs");

            DBCollection collection = db.getCollection("studentWs");

            long startTime = System.nanoTime();
            collection.drop();
            long endTime = System.nanoTime();
            duration = endTime - startTime;
            //added for debugging

            statsdDelete.incrementCounter("MongoDB Delete Counter");
            statsdDelete.recordGaugeValue("MongoDB Delete Gauge", 100);
            statsdDelete.recordExecutionTime("MongoDB Delete Timer", duration);
            statsdDelete.recordSetEvent("MongoDB Delete SetEvent", "One");

        } catch (UnknownHostException ex) {
            Logger.getLogger(MyPathResource.class.getName()).log(Level.SEVERE, null, ex);
        }

        return "in Delete Collection " + duration + " nanoseconds";

    }

}
