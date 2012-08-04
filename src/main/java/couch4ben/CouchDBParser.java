package couch4ben;

import org.databene.benerator.engine.BeneratorRootStatement;
import org.databene.benerator.engine.Statement;
import org.databene.benerator.engine.parser.xml.AbstractBeneratorDescriptorParser;
import org.databene.benerator.engine.parser.xml.BeneratorParseContext;
import org.databene.benerator.engine.statement.IfStatement;
import org.databene.benerator.engine.statement.WhileStatement;
import org.databene.commons.CollectionUtil;
import org.databene.commons.ConfigurationError;
import org.databene.commons.ErrorHandler;
import org.databene.script.Expression;
import org.w3c.dom.Element;

import static org.databene.benerator.engine.DescriptorConstants.*;
import static org.databene.benerator.engine.parser.xml.DescriptorParserUtil.parseScriptableStringAttribute;

public class CouchDBParser extends AbstractBeneratorDescriptorParser {
	
    static final String EL_COUCHDB = "couchdb";

    public CouchDBParser() {
	    super(EL_COUCHDB,
	    		CollectionUtil.toSet(ATT_ID),
	    		CollectionUtil.toSet(ATT_ENVIRONMENT, ATT_ON_ERROR),
	    		BeneratorRootStatement.class, IfStatement.class, WhileStatement.class);
    }

	@Override
	public CouchDBStatement doParse(Element element, Statement[] parentPath, BeneratorParseContext context) {
		String id = element.getAttribute("id");
        Expression<String> envEx = parseScriptableStringAttribute(ATT_ENVIRONMENT, element);
        if (envEx == null)
        	throw new ConfigurationError("no environment specified in <" + EL_COUCHDB + "> element");
		Expression<ErrorHandler> errHandlerEx = parseOnErrorAttribute(element, EL_COUCHDB);
		return new CouchDBStatement(id, envEx, errHandlerEx);
    }

}
