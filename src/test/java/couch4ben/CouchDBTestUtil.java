package couch4ben;

import org.databene.model.data.DataModel;

public abstract class CouchDBTestUtil {

	public static CouchDB createAndClearCouchClient(
			String environment, String collectionNameToClear, DataModel dataModel) {
        CouchDB db = createCouchDBClient(environment, dataModel);
		db.deleteAll();
		return db;
	}

	public static CouchDB createCouchDBClient(String environment,
			DataModel dataModel) {
        return CouchDBUtil.createCouchDBForEnvironment(environment, "db", dataModel);
	}


}
