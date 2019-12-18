/*
 *  Copyright © 2017 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package co.cask.directives.parser;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.format.RecordFormat;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.format.RecordFormats;
import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.ErrorRowException;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Categories;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.Text;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;

/**
 * A step to parse AVRO json or binary format.
 */
//@Plugin(type = Directive.Type)
@Name("parse-as-avro-schema")
@Categories(categories = { "parser", "avro"})
@Description("Parses column as AVRO generic record.")
public class ParseAvroSchema implements Directive {
  public static final String NAME = "parse-as-avro-schema";
  private static final Logger LOG = LoggerFactory.getLogger(ParseAvroSchema.class);
  private String column;
  private String schemaJson;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    builder.define("schema", TokenType.TEXT);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
    this.schemaJson = ((Text) args.value("schema")).value();
    LOG.info("Schema Json: {}", schemaJson);
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, final ExecutorContext context)
    throws DirectiveExecutionException, ErrorRowException {
    List<Row> results = new ArrayList<>();
    Schema schema;
    FormatSpecification spec;
    RecordFormat<StreamEvent, StructuredRecord> recordFormat;
    try {
    	schema = Schema.parseJson(schemaJson);
    	spec = new FormatSpecification(Formats.AVRO, schema, new HashMap<String, String>());
    	recordFormat = RecordFormats.createInitializedFormat(spec);
    } catch (Exception e) {
    	LOG.error("Exception in decoderInitialized.", e);
    	throw new DirectiveExecutionException(e);
    } 

    try {
        for (Row row : rows) {
          int idx = row.find(column);
          if (idx == -1) {
        	  continue;
          }
          Object object = row.getValue(idx);
          Row row1 = new Row();
          if (object instanceof byte[]) {
            byte[] bytes = (byte[]) object;
            StructuredRecord messageRecord = recordFormat.read(new StreamEvent(ByteBuffer.wrap(bytes)));
            for (Schema.Field field : messageRecord.getSchema().getFields()) {
                String fieldName = field.getName();
                row1.add(fieldName, messageRecord.get(fieldName));
            }
            results.add(row1);
          } else {
            LOG.error("Data in column: {} not byte array", column);
            throw new ErrorRowException(
              toString() + " : column " + column + " should be of type byte array", 1);
          }
        }
      } catch (Exception e) {
        LOG.error("Error decoding avro record");
        throw new ErrorRowException(toString() + " Issue decoding Avro record. " + e.getMessage(), 2);
      }
    return results;
  }
}
