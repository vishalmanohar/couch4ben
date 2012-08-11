package couch4ben;

import org.databene.commons.ConfigurationError;
import org.databene.commons.StringUtil;
import org.databene.jdbacl.DBUtil;
import org.databene.jdbacl.JDBCConnectData;
import org.databene.model.data.DataModel;

public class CouchDBUtil {

	public static CouchDB createCouchDBForEnvironment(String environment, String id, DataModel dataModel) {
        CouchDB couchDB = new CouchDB(id, dataModel);
//        couchDB.setHost("localhost");
//        couchDB.setPort(5984);
		JDBCConnectData connectData = DBUtil.getConnectData(environment);
		if (StringUtil.isEmpty(connectData.url))
			throw new ConfigurationError("No URL defined for environment " + environment);
		String[] urlTokens = StringUtil.splitOnLastSeparator(connectData.url, ':');

		if (urlTokens.length > 1)
            couchDB.setPort(Integer.parseInt(urlTokens[1]));
        couchDB.setUser(connectData.user);
		couchDB.setPassword(connectData.password);
//		if (StringUtil.isEmpty(connectData.catalog))
//			throw new ConfigurationError("No catalog configured for environment '" + environment + "'");
        couchDB.setDatabase(id);
		return couchDB;
	}

}
