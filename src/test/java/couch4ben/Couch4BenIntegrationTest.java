package couch4ben;

import org.databene.benerator.test.BeneratorIntegrationTest;
import org.databene.jdbacl.DBUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class Couch4BenIntegrationTest extends BeneratorIntegrationTest{

    private static final String TEST_ENVIRONMENT = "couchdbtest";

    @Before
    @After
    public void clearCollection() {
        CouchDB db = CouchDBTestUtil.createAndClearCouchClient(TEST_ENVIRONMENT, "mit_user", dataModel);
        context.set("db", db);
    }

    @Test
    public void testFixedSubGenerationCount() {
//        if (!DBUtil.existsEnvironment(TEST_ENVIRONMENT)) {
//            LOGGER.info("Skipping test since no environment " + TEST_ENVIRONMENT + " is defined");
//            return;
//        }
        // create 3 persons
        parseAndExecute(
                "<setup>" +
                        "	<import platforms='couchdb' />" +
                        "	<couchdb id='db' environment='" + TEST_ENVIRONMENT + "' />" +
                        "	<generate type='mit_user' count='3' consumer='db'>" +
                        "		<attribute name='name' type='string' />" +
                        "		<attribute name='age' type='int' min='18' max='78' />" +
                        "		<part name='addresses' container='list' count='2'>" +
                        "			<attribute name='street' pattern='[A-Z][a-z]{4} Street'/>" +
                        "			<attribute name='houseNo' type='int' min='2' max='9' />" +
                        "		</part>" +
                        "	</generate>" +
                        "</setup>");

        // verify results
        CouchDB db = (CouchDB) context.get("db");
        verifyPersonConstraints(db, 3, null, false, 2, 2);
    }

    private void verifyPersonConstraints(CouchDB db, long personCount, Integer expectedAge, boolean streetByName, int minAddressCount, int maxAddressCount) {

        List<DBObject> allDocuments = db.getAllDocuments();

        assertEquals(personCount, allDocuments.size());
        HashSet<Integer> usedCounts = new HashSet<Integer>();
        for(DBObject doc : allDocuments){
            String name = (String) doc.get("name");
            assertNotNull(name);
            Object age = doc.get("age");
            assertNotNull(age);
            assertTrue(age instanceof Integer);
            if (expectedAge != null)
                assertEquals(expectedAge, age);
            List<Map<String, Object>> addresses = (List<Map<String, Object>>) doc.get("addresses");
            assertNotNull(addresses);
            int addressCount = addresses.size();
            assertTrue(addressCount >= minAddressCount);
            assertTrue(addressCount <= maxAddressCount);
            usedCounts.add(addressCount);
            for (int i = 0; i < addressCount; i++) {
                String street = (String) addresses.get(i).get("street");
                assertTrue(street.endsWith(" Street"));
                if (streetByName)
                    assertEquals(street.charAt(0), name.charAt(0));
            }
            System.out.println("OK: " + doc);
        }
        if (minAddressCount < maxAddressCount)
            assertTrue("Expected more than one address count, but found " + usedCounts, usedCounts.size() > 1);
    }


}
