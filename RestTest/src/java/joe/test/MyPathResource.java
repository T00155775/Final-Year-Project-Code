package joe.test;

import java.io.IOException;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import joe.database.CouchImplementation;
import joe.database.MongoImplementation;
import joe.database.Neo4JImplementation;
import joe.database.SQLImplementation;

/**
 * REST Web Service
 *
 * @author Joe O Flaherty
 */
@Path("MyPath")
public class MyPathResource {

    @Context
    private UriInfo context;

    /**
     * Creates a new instance of MyPathResource
     */
    public MyPathResource() {
        this.testMongo = new MongoImplementation();
        this.testMySQL = new SQLImplementation();
        this.testImp = new MongoImplementation();
       // this.testNeo = new Neo4JImplementation();
    }
    Neo4JImplementation testNeo;
    MongoImplementation testImp;
    //CouchImplementation couchTest = new CouchImplementation();
    SQLImplementation testMySQL;
    MongoImplementation testMongo;
    
    /**
     *
     * @param db
     * @return
     */
    @GET
    @Path("/find/{db}")
    @Produces("application/json")
    public String find(@PathParam("db") String db) {

        if (db.equals("mongo")) {
            //MongoImplementation testImp = new MongoImplementation();
            testMongo.find();
            return testMongo.find();

        }
        if (db.equals("neo")) {

            Neo4JImplementation testNeo = new Neo4JImplementation();
            testNeo.find();
            return testNeo.find();
        }
        if (db.equals("couch")) {

            CouchImplementation couchTest = new CouchImplementation();

            return couchTest.find();
        }
        if (db.equals("mysql")) {

            //SQLImplementation testMySQL = new SQLImplementation();

            return testMySQL.find();
        }
        if (db.equals("all")) {

            //MongoImplementation testImp = new MongoImplementation();
            CouchImplementation couchTest = new CouchImplementation();
            Neo4JImplementation testNeo = new Neo4JImplementation();
            //SQLImplementation testMySQL = new SQLImplementation();
            return testNeo.find() + "\n\n" + testImp.find() + "\n\n" + couchTest.find() + "\n\n" + testMySQL.find();
        } else {
            return "Failed";
        }

    }

    /**
     *
     * @param db
     * @return
     * @throws java.io.IOException
     */
    @POST
    @Path("/save/{db}")
    //@Consumes("text/plain")
    public String save(@PathParam("db") String db) throws IOException {

        if (db.equals("mongo")) {
            //MongoImplementation testImp = new MongoImplementation();
            testMongo.save();
            return "Save MongoDB only successful";
        }
        if (db.equals("neo")) {
            Neo4JImplementation testNeo = new Neo4JImplementation();
            testNeo.save();
            return " Save Neo4J only successful";
        }
        if (db.equals("couch")) {

            CouchImplementation couchTest = new CouchImplementation();
            couchTest.save();
            return "Save CouchDB only successful";
        }
        if (db.equals("mysql")) {

            //SQLImplementation testMySQL = new SQLImplementation();
            testMySQL.save();
            return "Save MySQL only successful";
        }
        if (db.equals("all")) {
            //MongoImplementation testImp = new MongoImplementation();
            testMongo.save();
            Neo4JImplementation testNeo = new Neo4JImplementation();
            testNeo.save();
            CouchImplementation couchTest = new CouchImplementation();
            couchTest.save();
            //SQLImplementation testMySQL = new SQLImplementation();
            testMySQL.save();
            return "Save all Successful";
        } else {
            return "Failed";
        }

    }

    @GET
    @Path("/delete/{db}")
    @Produces("application/json")
    public String delete(@PathParam("db") String db) {
        CouchImplementation couchTest = new CouchImplementation();
        if (db.equals("mongo")) {
            //MongoImplementation testImp = new MongoImplementation();
            testMongo.delete();
            return "MongoDB collection deleted";
        }
        if (db.equals("couch")) {
            
            couchTest.delete();
            return "CouchDB database deleted";
        } 
        if (db.equals("mysql")) {

            //SQLImplementation testMySQL = new SQLImplementation();
            testMySQL.delete();
            return "MySQL all records deleted";
        }
        if(db.equals("all")){
            testMongo.delete();
            couchTest.delete();
            testMySQL.delete();
            return "Delete all db's executed";
        }
        else {
            return "Delete Failed";
        }

    }
    
    @GET
    @Path("/findspecific/{db}/{id}")
    @Produces("application/json")
    public String findSpecific(@PathParam("db") String db,@PathParam("id") String id) {
        if (db.equals("mysql")) {

            //SQLImplementation testMySQL = new SQLImplementation();
            
            return testMySQL.findSpecific(id);
        }
        else
            return "Find specific successful test";
    }
    

}
