/*
 * (c) Copyright 2012 by Volker Bergmann. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, is permitted under the terms of the
 * GNU General Public License (GPL).
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * WITHOUT A WARRANTY OF ANY KIND. ALL EXPRESS OR IMPLIED CONDITIONS,
 * REPRESENTATIONS AND WARRANTIES, INCLUDING ANY IMPLIED WARRANTY OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE OR NON-INFRINGEMENT, ARE
 * HEREBY EXCLUDED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

package couch4ben;

import org.databene.commons.ConversionException;
import org.databene.commons.converter.ThreadSafeConverter;
import org.databene.model.data.ComplexTypeDescriptor;
import org.databene.model.data.Entity;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DbObject2EntityConverter extends ThreadSafeConverter<DBObject, Entity> {
	
	CouchDB db;
	ComplexTypeDescriptor type;
	
	public DbObject2EntityConverter(CouchDB db, ComplexTypeDescriptor type) {
		super(DBObject.class, Entity.class);
		this.db = db;
		this.type = type;
	}

	public Entity convert(DBObject sourceValue) throws ConversionException {
		return convertDBObject(sourceValue, type);
	}
	
	private Entity convertDBObject(DBObject doc, ComplexTypeDescriptor type) throws ConversionException {
		return convertDBObject(doc, new Entity(type));
	}

	private Object convertDBObject(DBObject value, String typeName) {
		ComplexTypeDescriptor type = db.getOrCreatePartType(typeName);
		return convertDBObject(value, type);
	}

	private Entity convertDBObject(DBObject doc, Entity entity) {
		for (String componentName : doc.keySet()) {
			Object componentValue = doc.get(componentName);
			componentValue = convertComponent(componentValue, entity.type() + '.' + componentName);
			entity.setComponent(componentName, componentValue);
		}
		return entity;
	}

	private Object convertComponent(Object value, String componentName) {
		if (value == null)
			return null;
		else if (value instanceof List)
			return convertList((List<?>) value, componentName);
		else if (value.getClass().isArray())
			return convertArray(value, componentName);
		else if (value instanceof Set)
			return convertSet((Set<?>) value, componentName);
		else if (value instanceof DBObject) // attention: MongoDB's collections implement DBObject too!
			return convertDBObject((DBObject) value, componentName);
		else
			return value;
	}

	public ComplexTypeDescriptor partType(String componentName) {
		return db.getOrCreatePartType(componentName);
	}

	private List<?> convertList(List<?> list, String componentName) {
		List<Object> result = new ArrayList<Object>(list.size());
		for (Object element : list)
			result.add(convertComponent(element, componentName));
		return result;
	}

	private Object convertArray(Object array, String componentName) {
		int length = Array.getLength(array);
		Object[] result = new Object[length];
		for (int i = 0; i < length; i++)
			result[i] = convertComponent(Array.get(array, i), componentName);
		return result;
	}

	private Object convertSet(Set<?> set, String componentName) {
		Set<Object> result = new HashSet<Object>(set.size());
		for (Object element : set)
			result.add(convertComponent(element, componentName));
		return result;
	}

}