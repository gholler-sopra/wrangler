# Parse AVRO Encoded Messages

The PARSE-AS-AVRO-SCHEMA directive parses messages encoded as binary or json AVRO
records or file. This directive requires the schema to be applied

## Syntax
```
parse-as-avro-schema <schema> <json|binary>
```

The `<column>` is the name of the column whoes values will be decoded using
the provided schema.

## Usage Notes

The PARSE-AS-AVRO-SCHEMA directive efficiently parses and represents an avro message using an
structure that can then be queried using other directives.
