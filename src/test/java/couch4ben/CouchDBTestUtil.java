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
		//JDBCConnectData connectData = DBUtil.getConnectData(environment);
        CouchDB db = new CouchDB("db", dataModel);
		//String[] tokens = StringUtil.splitOnLastSeparator(connectData.url, ':');
		db.setHost("localhost");
        db.setPort(5984);
		db.setDatabase("test");
		db.getTypeDescriptors(); // cause the database to be initialized
		return db;
	}


}
