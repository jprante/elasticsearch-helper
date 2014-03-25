package org.xbib.elasticsearch.action.ingest;

import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.SourceToParse;

import java.io.IOException;
import java.util.Stack;

public class DocResolver {

    Stack<Doc> docs = new Stack();

    public DocResolver(String index, String type, String id) {

    }

    public void resolve(SourceToParse source) throws IOException {
        XContentParser parser = source.parser();
        try {
            if (parser == null) {
                parser = XContentHelper.createParser(source.source());
            }
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new MapperParsingException("Malformed content, must start with an object");
            }
            String currentFieldName = parser.currentName();
            while (token != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.START_OBJECT) {
                    //serializeObject(context, currentFieldName);
                } else if (token == XContentParser.Token.START_ARRAY) {
                    //serializeArray(context, currentFieldName);
                } else if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.VALUE_NULL) {
                    //serializeNullValue(context, currentFieldName);
                } else if (token == null) {
                    throw new MapperParsingException("object mapping for [" + source.type() + "] tried to parse as object, but got EOF, has a concrete value been provided to it?");
                } else if (token.isValue()) {
                    //serializeValue(context, currentFieldName, token);
                }
                token = parser.nextToken();
            }

        } finally {
            // only close the parser when its not provided externally
            if (source.parser() == null && parser != null) {
                parser.close();
            }
        }
    }


    class Doc {

        String index;
        String type;
        String id;

        Doc(String index, String type, String id) {
            this.index = index;
            this.type = type;
            this.id = id;
        }

    }
}
