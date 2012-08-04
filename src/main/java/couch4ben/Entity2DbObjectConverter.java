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
import org.databene.model.data.Entity;

import java.lang.reflect.Array;
import java.util.*;

public class Entity2DbObjectConverter extends ThreadSafeConverter<Entity, DBObject> {

	protected Entity2DbObjectConverter() {
		super(Entity.class, DBObject.class);
	}

	public DBObject convert(Entity entity) throws ConversionException {
		DBObject doc = new DBObject();
		for (Map.Entry<String, Object> component : entity.getComponents().entrySet()) {
			Object value = component.getValue();
			if (value instanceof Entity)
				value = convert((Entity) value);
			else if (value instanceof List)
				value = convertList((List<?>) value);
			else if (value instanceof Set)
				value = convertSet((Set<?>) value);
			else if (value != null && value.getClass().isArray())
				value = convertArray(value);
			doc.put(component.getKey(), value);
		}

		return doc;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Object convertList(List<?> list) {
		List result = new ArrayList();
		for (Object element : list) {
			if (element instanceof Entity)
				element = convert((Entity) element);
			result.add(element);
		}
		return result;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Set<?> convertSet(Set<?> set) {
		Set result = new HashSet();
		for (Object element : set) {
			if (element instanceof Entity)
				element = convert((Entity) element);
			result.add(element);
		}
		return result;
	}

	private Object[] convertArray(Object array) {
		int length = Array.getLength(array);
		Object[] result = new Object[length];
		for (int i = 0; i < length; i++) {
			Object element = Array.get(array, i);
			if (element instanceof Entity)
				element = convert((Entity) element);
			result[i] = element;
		}
		return result;
	}

}
