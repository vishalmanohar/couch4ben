package couch4ben;

import org.databene.model.data.DataModel;

public abstract class CouchDBTestUtil {

	public static CouchDB createAndClearCouchClient(
            String environment, String dbName, DataModel dataModel) {
        CouchDB db = createCouchDBClient(environment, dbName, dataModel);
		db.deleteAll();
		return db;
	}

	public static CouchDB createCouchDBClient(String environment,
                                              String dbName, DataModel dataModel) {
        return CouchDBUtil.createCouchDBForEnvironment(environment, dbName, dataModel);
	}


}
