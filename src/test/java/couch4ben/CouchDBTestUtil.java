package couch4ben;

import org.databene.commons.StringUtil;
import org.databene.jdbacl.DBUtil;
import org.databene.jdbacl.JDBCConnectData;
import org.databene.model.data.DataModel;

public abstract class CouchDBTestUtil {

	public static CouchDB createAndClearCouchClient(
			String environment, String collectionNameToClear, DataModel dataModel) {
        CouchDB db = createCouchDBClient(environment, dataModel);
		//clearCollection(db, collectionNameToClear); // always start with an empty collection
		return db;
	}

	public static CouchDB createCouchDBClient(String environment,
			DataModel dataModel) {
		//JDBCConnectData connectData = DBUtil.getConnectData(environment);
        CouchDB db = new CouchDB("db", dataModel);
		//String[] tokens = StringUtil.splitOnLastSeparator(connectData.url, ':');
		db.setHost("localhost");
        db.setPort(5984);
//		if (tokens.length > 1)
//			db.setPort(Integer.parseInt(tokens[1]));
//		if (connectData.user != null)
//			db.setUser(connectData.user);
//		if (connectData.password != null)
//			db.setPassword(connectData.password);
		db.setDatabase("test");
		db.getTypeDescriptors(); // cause the database to be initialized
		return db;
	}

//	public static void clearCollection(CouchDB db, String collectionName) {
//	}
//
//	public static void printDbEntities(String testTypeName, CouchDB db) {
//		DBCollection collection = db.db.(testTypeName);
//		DBCursor cursor = collection.find();
//		while (cursor.hasNext()) {
//			System.out.println(cursor.next());
//		}
//	}

}
