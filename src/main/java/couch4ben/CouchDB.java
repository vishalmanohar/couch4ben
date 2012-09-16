package couch4ben;

import org.databene.benerator.storage.AbstractStorageSystem;
import org.databene.commons.Context;
import org.databene.model.data.*;
import org.databene.webdecs.DataSource;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CouchDB extends AbstractStorageSystem {

	private static final Logger LOGGER = LoggerFactory.getLogger(CouchDB.class);

	private String id;
	private String host;
	private int port;
	private String database;
	private String user;
	private String password;

	private boolean connected;
    CouchDbConnector db;
    private Entity2DbObjectConverter entity2objectConverter;

	private DefaultDescriptorProvider descriptorProvider;

	// constructors ----------------------------------------------------------------------------------------------------

	public CouchDB(String id, DataModel dataModel) {
		setDataModel(dataModel);
		this.id = id;
		this.host = null;
		this.port = 27017;
		this.user = null;
		this.password = null;
		this.connected = false;
		this.db = null;
        HttpClient httpClient = new StdHttpClient.Builder().build();
        CouchDbInstance dbInstance = new StdCouchDbInstance(httpClient);
        db = dbInstance.createConnector(id, true);

		this.entity2objectConverter = new Entity2DbObjectConverter();
		this.descriptorProvider = new DefaultDescriptorProvider(id, dataModel);
	}

	// properties ------------------------------------------------------------------------------------------------------
	
	public String getId() {
		return id;
	}

    public DataSource<Entity> queryEntities(String s, String s1, Context context) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public DataSource<?> queryEntityIds(String s, String s1, Context context) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public DataSource<?> query(String s, boolean b, Context context) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setId(String id) {
		this.id = id;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}
	
	// DescriptorProvider interface implementation

	public TypeDescriptor[] getTypeDescriptors() {
		//beConnected();
		return descriptorProvider.getTypeDescriptors();
	}

	public TypeDescriptor getTypeDescriptor(String typeName) {
		//beConnected();
		return descriptorProvider.getTypeDescriptor(typeName);
	}
	
	// query methods from StorageSystem interface ----------------------------------------------------------------------

//	public DataSource<Entity> queryEntities(String typeName, String selector, Context context) {
//		LOGGER.debug("queryEntities({}, {})", typeName, selector);
//		beConnected();
//		BasicDBObject query = new BasicDBObject();
//		if (!StringUtil.isEmpty(selector))
//			throw new UnsupportedOperationException("'selector' is not yet supported in Mongo DB queries");
//		// TODO define selector syntax (query.append("username", "johnd"));
//		QueryDataSource source = new QueryDataSource(query, typeName, db);
//		ComplexTypeDescriptor type = (ComplexTypeDescriptor) getTypeDescriptor(typeName);
//		return new ConvertingDataSource<DBObject, Entity>(source, new DbObject2EntityConverter(this, type));
//	}
//
//	public DataSource<?> queryEntityIds(String type, String selector, Context context) {
//		LOGGER.debug("queryEntityIds({}, {})", type, selector);
//		beConnected();
//		BasicDBObject query = new BasicDBObject();
//		if (!StringUtil.isEmpty(selector))
//			throw new UnsupportedOperationException("'selector' is not yet supported in Mongo DB queries");
//		// TODO define selector syntax (query.append("username", "johnd"));
//		QueryDataSource source = new QueryDataSource(query, type, db);
//		return new ConvertingDataSource<DBObject, Object>(source, object2idConverter);
//	}
//
//	public DataSource<?> query(String selector, boolean simplify, Context context) {
//		LOGGER.debug("query({})", selector);
//		beConnected();
//		throw new UnsupportedOperationException("General-purpose queries are not supported by Mongo DB");
//	}
	
	// data manipulation methods from StorageSystem interface ----------------------------------------------------------
	
	@Override
	public Object execute(String command) {
		throw new UnsupportedOperationException("MongoSystem.execute() is not implemented"); // TODO implement MongoSystem.execute
	}
	
	public void store(Entity entity) {
		LOGGER.debug("store({})", entity);
		//beConnected();
		String entityType = entity.type();
		if (entityType == null)
			throw new RuntimeException("Trying to persist an entity without type: " + entity);
		DBObject doc = entity2objectConverter.convert(entity);
        doc.put("type", entity.type());
        store(doc);
    }

    public void store(DBObject doc) {
        db.create(doc);
    }

    public void update(Entity entity) {
		LOGGER.debug("update({})", entity);
		DBObject doc = entity2objectConverter.convert(entity);
		db.update(doc);
	}
	
	// other methods from StorageSystem interface ----------------------------------------------------------------------
	
	public void flush() {
	}
	
	public void close() {
	}
	
//	public void beConnected() {
//		if (!connected) {
//			try {
//				this.mongo = new Mongo(host, port);
//				this.db = mongo.getDB(database);
//				if (!StringUtil.isEmpty(user) && !db.authenticate(user, StringUtil.getChars(password)))
//					throw new ConfigurationError("Authentication failed for user '" + user + "' on " + host + ":" + port);
//				for (String collectionName : db.getCollectionNames())
//					descriptorProvider.addTypeDescriptor(new ComplexTypeDescriptor(collectionName, this));
//				this.connected = true;
//			} catch (Exception e) {
//				throw new RuntimeException("Error connecting database '" + database + "' at " + host + ":" + port, e);
//			}
//		}
//	}

    public void deleteAll() {
        List<DBObject> allDocuments = getAllDocuments();
        for(DBObject doc : allDocuments){
            db.delete(doc);
        }
    }

	public ComplexTypeDescriptor getOrCreatePartType(String typeName) {
		ComplexTypeDescriptor type = (ComplexTypeDescriptor) getTypeDescriptor(typeName);
		if (type == null)
			type = new ComplexTypeDescriptor(typeName, this);
		return type;
	}

    public List<DBObject> getAllDocuments() {
        List<String> allDocIds = db.getAllDocIds();
        List<DBObject> allDocs = new ArrayList<DBObject>();
        for(String docId : allDocIds){
            allDocs.add(db.get(DBObject.class, docId));
        }
        return allDocs;
    }
}
