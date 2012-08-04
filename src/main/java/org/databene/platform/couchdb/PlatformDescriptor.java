package org.databene.platform.couchdb;

import couch4ben.CouchDBParser;
import org.databene.benerator.DefaultPlatformDescriptor;

public class PlatformDescriptor extends DefaultPlatformDescriptor {

	public PlatformDescriptor() {
		super(PlatformDescriptor.class.getPackage().getName());
		addParser(new CouchDBParser());
	}

}
