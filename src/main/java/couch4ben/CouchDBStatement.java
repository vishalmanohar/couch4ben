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

import org.databene.benerator.engine.BeneratorContext;
import org.databene.benerator.engine.Statement;
import org.databene.commons.ErrorHandler;
import org.databene.commons.Level;
import org.databene.script.Expression;

public class CouchDBStatement implements Statement {
	
	private String id;
    private String dbName;
    private Expression<String> envEx;
	private Expression<ErrorHandler> errHandlerEx;

	public CouchDBStatement(String id, String dbName, Expression<String> envEx, Expression<ErrorHandler> errHandlerEx) {
		this.id = id;
        this.dbName = dbName;
        this.envEx = envEx;
		this.errHandlerEx = errHandlerEx;
	}

	public boolean execute(BeneratorContext context) {
		try {
			String environment = context.resolveRelativeUri(envEx.evaluate(context));
			CouchDB db = CouchDBUtil.createCouchDBForEnvironment(environment, dbName, context.getDataModel());
			context.set(id, db);
		} catch (Exception e) {
			getErrorHandler(context).handleError("Error connecting CouchDB", e);
		}
    	return true;
	}

	private ErrorHandler getErrorHandler(BeneratorContext context) {
		ErrorHandler handler = errHandlerEx.evaluate(context);
		return (handler != null ? handler : new ErrorHandler("dbsanity", Level.fatal));
	}

}
