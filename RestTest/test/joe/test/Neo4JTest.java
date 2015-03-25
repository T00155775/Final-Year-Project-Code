/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package joe.test;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestGraphDatabaseFactory;

/**
 *
 * @author Joe
 */
public class Neo4JTest {

   

        GraphDatabaseService graphDb;

        /**
         * Create temporary database for each unit test.
         */
// START SNIPPET: beforeTest
        @Before
        public void prepareTestDatabase() {
            graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase("C:/data/dbTest");
        }
// END SNIPPET: beforeTest

        /**
         * Shutdown the database.
         */
// START SNIPPET: afterTest
        @After
        public void destroyTestDatabase() {
            graphDb.shutdown();
        }
// END SNIPPET: afterTest

       /* @Test
        public void startWithConfiguration() {
// START SNIPPET: startDbWithConfig
            GraphDatabaseService db = new TestGraphDatabaseFactory()
                    .newImpermanentDatabaseBuilder()
                    .setConfig(GraphDatabaseSettings.nodestore_mapped_memory_size, "10M")
                    .setConfig(GraphDatabaseSettings.string_block_size, "60")
                    .setConfig(GraphDatabaseSettings.array_block_size, "300")
                    .newGraphDatabase();
// END SNIPPET: startDbWithConfig
            db.shutdown();
        }*/

        @Test
        public void shouldCreateNode() {
// START SNIPPET: unitTest
            Node n;
            try (Transaction tx = graphDb.beginTx()) {
                n = graphDb.createNode();
                n.setProperty("name", "Nancy");
                tx.success();
            }
// The node should have a valid id
            assertThat(n.getId(), is(greaterThan(-1L)));
// Retrieve a node by using the id of the created node. The id's and
// property should match.
            try (Transaction tx = graphDb.beginTx()) {
                Node foundNode = graphDb.getNodeById(n.getId());
                assertThat(foundNode.getId(), is(n.getId()));
                assertThat((String) foundNode.getProperty("name"), is("Nancy"));
            }
// END SNIPPET: unitTest
        }
}
