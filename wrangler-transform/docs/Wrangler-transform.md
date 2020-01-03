# Data Prep Transform

This plugin applies data transformation directives on your data records. The directives
are generated either through an interactive user interface or by manual entry into the
plugin.

## Plugin Configuration

| Configuration     | Required | Default | Description                                                           |
| ----------------- | :------: | :-----: | --------------------------------------------------------------------- |
| Input Field       | No       | `*`     | The name of the input field (or `*` for all fields)                   |
| Precondition      | No       | `false` | A filter (containing JEXL condition) to be applied on any of the input columns before a record is passed to data prep for applying wrangling directives        |
| Directives        | Yes      | n/a     | The series of data prep directives to be applied on the input records |
| Failure Threshold | No       | `1`     | Maximum number of errors tolerated before exiting pipeline processing |

## Precondition filter

A `precondition` filter is useful to apply filtering on records before the records are
delivered for data prep. 

To filter a record, specify a condition that will result in boolean state of `true`.
The condition can be specified using `JEXL` expression which
when evaluated to `true` filters `out` i.e. remove such records.

For example, to filter out all records that are a header record from a CSV file where the
header record is at the start of the file, you could use this filter:

```
  offset == 0
```

This will filter out records that have an `offset` of zero.

Another example is where user may want to filter out input records containing a substring 
"YCES" in any of the records, in such case following filter may be used :

```
  body.contains("YCES")
```

This will filter out records that contains `YCES` anywhere in the body column from previous plugin stage.

## Directives

There are numerous directives and variations supported by this plugin. 
These directives are available as commands as mentioned below in [Available Directives](#available-directoves).


## `Wrangler` Button - Interractive Wrangling using DataPrep Service 

DataPrep service supports many row transformation directives which are actually executed by this plugin 
in the final pipeline.
For interrcatively applying these directives, click on the `Wrangler` button provided in plugin widget UI
which will open the DataPrep Service in your current Pipeline Canvas page context.
Select the file that will load  the desired data into DataPrep UI. Now, apply these directives to parse or 
transform the data until it looks good enough to proceed to attaching next plgin stage.

These directives can be applied as shell commands by entering them through provided console interpreter at bottom 
of DataPrep page. The result of directive is shown immediately after its executed by clicking enter. 
Also, the command gets registered on right side of page under directives tab for a user to review or revert later on.

Please note that many (and NOT all) of these directives can also be applied using drop-down option available before 
each column name from DataPrep UI itself. Wherever not clear, please apply directive using command console.

Please also find below the [DIRECTIVES CHEATSHEET](#directives-cheatsheet) to find mapping between the UI interractive drop-down option and 
corresponding directive command. However, note that these UI options and commands have very intuitive names to 
auto-infer the relation.


## Usage Notes

All input record fields are made available to the data prep directives when `*` is used as
the field to be data prepped. They are in the record in the same order as they appear.

Note that if the transform doesn't operate on all of the input record fields or a field is
not configured as part of the output schema, and you are using the `set columns`
directive, you may see inconsistent behavior. Use the `drop` directive to drop any fields
that are not used in the data prep.

This plugin uses the `emiterror` capability to emit records that fail parsing into a
separate error stream, allowing the aggregation of all errors. However, if the _Failure
Threshold_ is reached, then the pipeline will fail.


## Available Directives

These directives are currently available:

| Directive                                                              | Description                                                      |
| ---------------------------------------------------------------------- | ---------------------------------------------------------------- |
| **Parsers**                                                            |                                                                  |
| [JSON Path](wrangler-docs/directives/json-path.md)                              | Uses a DSL (a JSON path expression) for parsing JSON records     |
| [Parse as AVRO](wrangler-docs/directives/parse-as-avro.md)                      | Parsing an AVRO encoded message - either as binary or json       |
| [Parse as AVRO File](wrangler-docs/directives/parse-as-avro-file.md)            | Parsing an AVRO data file                                        |
| [Parse as CSV](wrangler-docs/directives/parse-as-csv.md)                        | Parsing an input record as comma-separated values                |
| [Parse as Date](wrangler-docs/directives/parse-as-date.md)                      | Parsing dates using natural language processing                  |
| [Parse as Excel](wrangler-docs/directives/parse-as-excel.md)                    | Parsing excel file.                                              |
| [Parse as Fixed Length](wrangler-docs/directives/parse-as-fixed-length.md)      | Parses as a fixed length record with specified widths            |
| [Parse as HL7](wrangler-docs/directives/parse-as-hl7.md)                        | Parsing Health Level 7 Version 2 (HL7 V2) messages               |
| [Parse as JSON](wrangler-docs/directives/parse-as-json.md)                      | Parsing a JSON object                                            |
| [Parse as Log](wrangler-docs/directives/parse-as-log.md)                        | Parses access log files as from Apache HTTPD and nginx servers   |
| [Parse as Protobuf](wrangler-docs/directives/parse-as-log.md)                   | Parses an Protobuf encoded in-memory message using descriptor    |
| [Parse as Simple Date](wrangler-docs/directives/parse-as-simple-date.md)        | Parses date strings                                              |
| [Parse XML To JSON](wrangler-docs/directives/parse-xml-to-json.md)              | Parses an XML document into a JSON structure                     |
| [Parse as Currency](wrangler-docs/directives/parse-as-currency.md)              | Parses a string representation of currency into a number.        |
| **Output Formatters**                                                  |                                                                  |
| [Write as CSV](wrangler-docs/directives/write-as-csv.md)                        | Converts a record into CSV format                                |
| [Write as JSON](wrangler-docs/directives/write-as-json-map.md)                  | Converts the record into a JSON map                              |
| [Write JSON Object](wrangler-docs/directives/write-as-json-object.md)           | Composes a JSON object based on the fields specified.            |
| [Format as Currency](wrangler-docs/directives/format-as-currency.md)            | Formats a number as currency as specified by locale.             |
| **Transformations**                                                    |                                                                  |
| [Changing Case](wrangler-docs/directives/changing-case.md)                      | Changes the case of column values                                |
| [Cut Character](wrangler-docs/directives/cut-character.md)                      | Selects parts of a string value                                  |
| [Set Column](wrangler-docs/directives/set-column.md)                            | Sets the column value to the result of an expression execution   |
| [Find and Replace](wrangler-docs/directives/find-and-replace.md)                | Transforms string column values using a "sed"-like expression    |
| [Index Split](wrangler-docs/directives/index-split.md)                          | (_Deprecated_)                                                   |
| [Invoke HTTP](wrangler-docs/directives/invoke-http.md)                          | Invokes an HTTP Service (_Experimental_, potentially slow)       |
| [Quantization](wrangler-docs/directives/quantize.md)                            | Quantizes a column based on specified ranges                     |
| [Regex Group Extractor](wrangler-docs/directives/extract-regex-groups.md)       | Extracts the data from a regex group into its own column         |
| [Setting Character Set](wrangler-docs/directives/set-charset.md)                | Sets the encoding and then converts the data to a UTF-8 String   |
| [Setting Record Delimiter](wrangler-docs/directives/set-record-delim.md)        | Sets the record delimiter                                        |
| [Split by Separator](wrangler-docs/directives/split-by-separator.md)            | Splits a column based on a separator into two columns            |
| [Split Email Address](wrangler-docs/directives/split-email.md)                  | Splits an email ID into an account and its domain                |
| [Split URL](wrangler-docs/directives/split-url.md)                              | Splits a URL into its constituents                               |
| [Text Distance (Fuzzy String Match)](wrangler-docs/directives/text-distance.md) | Measures the difference between two sequences of characters      |
| [Text Metric (Fuzzy String Match)](wrangler-docs/directives/text-metric.md)     | Measures the difference between two sequences of characters      |
| [URL Decode](wrangler-docs/directives/url-decode.md)                            | Decodes from the `application/x-www-form-urlencoded` MIME format |
| [URL Encode](wrangler-docs/directives/url-encode.md)                            | Encodes to the `application/x-www-form-urlencoded` MIME format   |
| [Trim](wrangler-docs/directives/trim.md)                                        | Functions for trimming white spaces around string data           |
| **Encoders and Decoders**                                              |                                                                  |
| [Decode](wrangler-docs/directives/decode.md)                                    | Decodes a column value as one of `base32`, `base64`, or `hex`    |
| [Encode](wrangler-docs/directives/encode.md)                                    | Encodes a column value as one of `base32`, `base64`, or `hex`    |
| **Unique ID**                                                          |                                                                  |
| [UUID Generation](wrangler-docs/directives/generate-uuid.md)                    | Generates a universally unique identifier (UUID)                 |
| **Date Transformations**                                               |                                                                  |
| [Diff Date](wrangler-docs/directives/diff-date.md)                              | Calculates the difference between two dates                      |
| [Format Date](wrangler-docs/directives/format-date.md)                          | Custom patterns for date-time formatting                         |
| [Format Unix Timestamp](wrangler-docs/directives/format-unix-timestamp.md)      | Formats a UNIX timestamp as a date                               |
| **Lookups**                                                            |                                                                  |
| [Catalog Lookup](wrangler-docs/directives/catalog-lookup.md)                    | Static catalog lookup of ICD-9, ICD-10-2016, ICD-10-2017 codes   |
| [Table Lookup](wrangler-docs/directives/table-lookup.md)                        | Performs lookups into Table datasets                             |
| **Hashing & Masking**                                                  |                                                                  |
| [Message Digest or Hash](wrangler-docs/directives/hash.md)                      | Generates a message digest                                       |
| [Mask Number](wrangler-docs/directives/mask-number.md)                          | Applies substitution masking on the column values                |
| [Mask Shuffle](wrangler-docs/directives/mask-shuffle.md)                        | Applies shuffle masking on the column values                     |
| **Row Operations**                                                     |                                                                  |
| [Filter Row if Matched](wrangler-docs/directives/filter-row-if-matched.md)      | Filters rows that match a pattern for a column                                         |
| [Filter Row if True](wrangler-docs/directives/filter-row-if-true.md)            | Filters rows if the condition is true.                                                  |
| [Filter Row Empty of Null](wrangler-docs/directives/filter-empty-or-null.md)    | Filters rows that are empty of null.                    |
| [Flatten](wrangler-docs/directives/flatten.md)                                  | Separates the elements in a repeated field                       |
| [Fail on condition](wrangler-docs/directives/fail.md)                           | Fails processing when the condition is evaluated to true.        |
| [Send to Error](wrangler-docs/directives/send-to-error.md)                      | Filtering of records to an error collector                       |
| [Send to Error And Continue](wrangler-docs/directives/send-to-error-and-continue.md) | Filtering of records to an error collector and continues processing                      |
| [Split to Rows](wrangler-docs/directives/split-to-rows.md)                      | Splits based on a separator into multiple records                |
| **Column Operations**                                                  |                                                                  |
| [Change Column Case](wrangler-docs/directives/change-column-case.md)            | Changes column names to either lowercase or uppercase            |
| [Changing Case](wrangler-docs/directives/changing-case.md)                      | Change the case of column values                                 |
| [Cleanse Column Names](wrangler-docs/directives/cleanse-column-names.md)        | Sanatizes column names, following specific rules                 |
| [Columns Replace](wrangler-docs/directives/columns-replace.md)                  | Alters column names in bulk                                      |
| [Copy](wrangler-docs/directives/copy.md)                                        | Copies values from a source column into a destination column     |
| [Drop Column](wrangler-docs/directives/drop.md)                                 | Drops a column in a record                                       |
| [Fill Null or Empty Columns](wrangler-docs/directives/fill-null-or-empty.md)    | Fills column value with a fixed value if null or empty           |
| [Keep Columns](wrangler-docs/directives/keep.md)                                | Keeps specified columns from the record                          |
| [Merge Columns](wrangler-docs/directives/merge.md)                              | Merges two columns by inserting a third column                   |
| [Rename Column](wrangler-docs/directives/rename.md)                             | Renames an existing column in the record                         |
| [Set Column Header](wrangler-docs/directives/set-headers.md)                     | Sets the names of columns, in the order they are specified       |
| [Split to Columns](wrangler-docs/directives/split-to-columns.md)                | Splits a column based on a separator into multiple columns       |
| [Swap Columns](wrangler-docs/directives/swap.md)                                | Swaps column names of two columns                                |
| [Set Column Data Type](wrangler-docs/directives/set-type.md)                    | Convert data type of a column                                    |
| **NLP**                                                                |                                                                  |
| [Stemming Tokenized Words](wrangler-docs/directives/stemming.md)                | Applies the Porter stemmer algorithm for English words           |
| **Transient Aggregators & Setters**                                    |                                                                  |
| [Increment Variable](wrangler-docs/directives/increment-variable.md)            | Increments a transient variable with a record of processing.     |
| [Set Variable](wrangler-docs/directives/set-variable.md)                        | Sets a transient variable with a record of processing.     |
| **Functions**                                                          |                                                                  |
| [Data Quality](wrangler-docs/functions/dq-functions.md)                         | Data quality check functions. Checks for date, time, etc.        |
| [Date Manipulations](wrangler-docs/functions/date-functions.md)                 | Functions that can manipulate date                               |
| [DDL](wrangler-docs/functions/ddl-functions.md)                                 | Functions that can manipulate definition of data                 |
| [JSON](wrangler-docs/functions/json-functions.md)                               | Functions that can be useful in transforming your data           |
| [Types](wrangler-docs/functions/type-functions.md)                              | Functions for detecting the type of data                         |



## Directives Cheatsheet

| Name                             | Usage                                                                              | Description                                                                                                                                                        |
| -------------------------------- | ---------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
|SWAP|swap &lt;column1&gt; &lt;column2&gt;|Swaps the column names of two columns.|
|ENCODE|encode &lt;base32|base64|hex&gt; &lt;column&gt;|Encodes column values using one of base32, base64, or hex.|
|XPATH|xpath &lt;column&gt; &lt;destination&gt; &lt;xpath&gt;|Extract a single XML element or attribute using XPath.|
|GENERATE-UUID|generate-uuid &lt;column&gt;|Populates a column with a universally unique identifier (UUID) of the record.|
|LOWERCASE|lowercase &lt;column&gt;|Changes the column values to lowercase.|
|WRITE-AS-CSV|write-as-csv &lt;column&gt;|Writes the records files as well-formatted CSV|
|PARSE-AS-PROTOBUF|parse-as-protobuf &lt;column&gt; &lt;schema-id&gt; &lt;record-name&gt; [version]|Parses column as protobuf encoded memory representations.|
|HASH|hash &lt;column&gt; &lt;algorithm&gt; [&lt;encode=true|false&gt;]|Creates a message digest for the column using algorithm, replacing the column value.|
|JSON-PATH|json-path &lt;source&gt; &lt;destination&gt; &lt;json-path-expression&gt;|Parses JSON elements using a DSL (a JSON path expression).|
|MASK-NUMBER|mask-number &lt;column&gt; &lt;pattern&gt;|Masks a column value using the specified masking pattern.|
|TEXT-DISTANCE|text-distance &lt;method&gt; &lt;column1&gt; &lt;column2&gt; &lt;destination&gt;|Calculates a text distance measure between two columns containing string.|
|PARSE-XML-TO-JSON|parse-xml-to-json &lt;column&gt; [&lt;depth&gt;]|Parses a XML document to JSON representation.|
|PARSE-AS-HL7|parse-as-hl7 &lt;column&gt; [&lt;depth&gt;]|Parses &lt;column&gt; for Health Level 7 Version 2 (HL7 V2) messages; &lt;depth&gt; indicates at which point JSON object enumeration terminates.|
|FIND-AND-REPLACE|find-and-replace &lt;column&gt; &lt;sed-expression&gt;|Finds and replaces text in column values using a sed-format expression.|
|RENAME|rename &lt;old&gt; &lt;new&gt;|Renames an existing column.|
|PARSE-AS-AVRO|parse-as-avro &lt;column&gt; &lt;schema-id&gt; &lt;json|binary&gt; [version]|Parses column as AVRO generic record.|
|FILL-NULL-OR-EMPTY|fill-null-or-empty &lt;column&gt; &lt;fixed-value&gt;|Fills a value of a column with a fixed value if it is either null or empty.|
|SET-TYPE|set-type &lt;column&gt; &lt;type&gt;|Converting data type of a column.|
|RTRIM|rtrim &lt;column&gt;|Trimming whitespace from right side of a string.|
|INVOKE-HTTP|invoke-http &lt;url&gt; &lt;column&gt;[,&lt;column&gt;*] &lt;header&gt;[,&lt;header&gt;*]|[EXPERIMENTAL] Invokes an HTTP endpoint, passing columns as a JSON map (potentially slow).|
|COLUMNS-REPLACE|columns-replace &lt;sed-expression&gt;|Modifies column names in bulk using a sed-format expression.|
|SEND-TO-ERROR|send-to-error &lt;condition&gt;|Send records that match condition to the error collector.|
|SET-RECORD-DELIM|set-record-delim &lt;column&gt; &lt;delimiter&gt; [&lt;limit&gt;]|Sets the record delimiter.|
|SET-VARIABLE|set-variable &lt;variable&gt; &lt;expression&gt;|Sets the value for a transient variable for the record being processed.|
|SET-CHARSET|set-charset &lt;column&gt; &lt;charset&gt;|Sets the character set decoding to UTF-8.|
|WRITE-AS-JSON-OBJECT|write-as-json-object &lt;dest-column&gt; [&lt;src-column&gt;[,&lt;src-column&gt;]|Creates a JSON object based on source columns specified. JSON object is written into dest-column.|
|KEEP|keep &lt;column&gt;[,&lt;column&gt;*]|Keeps the specified columns and drops all others.|
|CUT-CHARACTER|cut-character &lt;source&gt; &lt;destination&gt; &lt;type&gt; &lt;range|indexes&gt;|UNIX-like 'cut' directive for splitting text.|
|SPLIT-TO-ROWS|split-to-rows &lt;column&gt; &lt;separator&gt;|Splits a column into multiple rows, copies the rest of the columns.|
|XPATH-ARRAY|xpath-array &lt;column&gt; &lt;destination&gt; &lt;xpath&gt;|Extract XML element or attributes as JSON array using XPath.|
|FAIL|fail &lt;condition&gt;|Fails when the condition is evaluated to true.|
|INCREMENT-VARIABLE|increment-variable &lt;variable&gt; &lt;value&gt; &lt;expression&gt;|Wrangler - A interactive tool for data cleansing and transformation.|
|PARSE-AS-XML|parse-as-xml &lt;column&gt;|Parses a column as XML.|
|PARSE-AS-FIXED-LENGTH|parse-as-fixed-length &lt;column&gt; &lt;width&gt;[,&lt;width&gt;*] [&lt;padding-character&gt;]|Parses fixed-length records using the specified widths and padding-character.|
|CHANGE-COLUMN-CASE|change-column-case lower|upper|Changes the case of column names to either lowercase or uppercase.|
|SPLIT-EMAIL|split-email &lt;column&gt;|Split a email into account and domain.|
|URL-ENCODE|url-encode &lt;column&gt;|URL encode a column value.|
|WRITE-AS-JSON-MAP|write-as-json-map &lt;column&gt;|Writes all record columns as JSON map.|
|MASK-SHUFFLE|mask-shuffle &lt;column&gt;|Masks a column value by shuffling characters while maintaining the same length.|
|DROP|drop &lt;column&gt;[,&lt;column&gt;*]|Drop one or more columns.|
|DECODE|decode &lt;base32|base64|hex&gt; &lt;column&gt;|Decodes column values using one of base32, base64, or hex.|
|SPLIT|split &lt;source&gt; &lt;delimiter&gt; &lt;new-column-1&gt; &lt;new-column-2&gt;|[DEPRECATED] Use 'split-to-columns' or 'split-to-rows'.|
|PARSE-AS-SIMPLE-DATE|parse-as-simple-date &lt;column&gt; &lt;format&gt;|Parses a column as date using format.|
|DIFF-DATE|diff-date &lt;column1&gt; &lt;column2&gt; &lt;destination&gt;|Calculates the difference in milliseconds between two Date objects.Positive if &lt;column2&gt; earlier. Must use 'parse-as-date' or 'parse-as-simple-date' first.|
|INDEXSPLIT|indexsplit &lt;source&gt; &lt;start&gt; &lt;end&gt; &lt;destination&gt;|[DEPRECATED] Use the 'split-to-columns' or 'parse-as-fixed-length' directives instead.|
|PARSE-AS-AVRO-FILE|parse-as-avro-file &lt;column&gt;|parse-as-avro-file &lt;column&gt;.|
|FILTER-ROW-IF-TRUE|filter-row-if-true &lt;condition&gt;|[DEPRECATED] Filters rows if condition is evaluated to true. Use 'filter-rows-on' instead.|
|SPLIT-URL|split-url &lt;column&gt;|Split a url into it's components host,protocol,port,etc.|
|FORMAT-DATE|format-date &lt;column&gt; &lt;format&gt;|Formats a column using a date-time format. Use 'parse-as-date` beforehand.|
|QUANTIZE|quantize &lt;source&gt; &lt;destination&gt; &lt;[range1:range2)=value&gt;,[&lt;range1:range2=value&gt;]*|Quanitize the range of numbers into label values.|
|PARSE-AS-EXCEL|parse-as-excel &lt;column&gt; [&lt;sheet number | sheet name&gt;] [first-row-as-header]|Parses column as Excel file.|
|PARSE-AS-DATE|parse-as-date &lt;column&gt; [&lt;timezone&gt;]|Parses column values as dates using natural language processing and automatically identifying the format (expensive in terms of time consumed).|
|TABLE-LOOKUP|table-lookup &lt;column&gt; &lt;table&gt;|Uses the given column as a key to perform a lookup into the specified table.|
|FILTER-ROWS-ON|filter-rows-on empty-or-null-columns &lt;column&gt;[,&lt;column&gt;*]|Filters row that have empty or null columns.|
|TRIM|trim &lt;column&gt;|Trimming whitespace from both sides of a string.|
|URL-DECODE|url-decode &lt;column&gt;|URL decode a column value.|
|FLATTEN|flatten &lt;column&gt;[,&lt;column&gt;*]|Separates array elements of one or more columns into indvidual records, copying the other columns.|
|UPPERCASE|uppercase &lt;column&gt;|Changes the column values to uppercase.|
|CATALOG-LOOKUP|catalog-lookup &lt;catalog&gt; &lt;column&gt;|Looks-up values from pre-loaded (static) catalogs.|
|PARSE-AS-LOG|parse-as-log &lt;column&gt; &lt;format&gt;|Parses Apache HTTPD and NGINX logs.|
|LTRIM|ltrim &lt;column&gt;|Trimming whitespace from left side of a string.|
|EXTRACT-REGEX-GROUPS|extract-regex-groups &lt;column&gt; &lt;regex-with-groups&gt;|Extracts data from a regex group into its own column.|
|PARSE-AS-CSV|parse-as-csv &lt;column&gt; &lt;delimiter&gt; [&lt;header=true|false&gt;]|Parses a column as CSV (comma-separated values).|
|FILTER-ROW-IF-MATCHED|filter-row-if-matched &lt;column&gt; &lt;regex&gt;|[DEPRECATED] Filters rows if the regex is matched. Use 'filter-rows-on' instead.|
|PARSE-AS-JSON|parse-as-json &lt;column&gt; [&lt;depth&gt;]|Parses a column as JSON.|
|SET COLUMN|set column &lt;column&gt; &lt;jexl-expression&gt;|Sets a column by evaluating a JEXL expression.|
|STEMMING|stemming &lt;column&gt;|Apply Porter Stemming on the column value.|
|COPY|copy &lt;source&gt; &lt;destination&gt; [&lt;force=true|false&gt;]|Copies values from a source column into a destination column.|
|SET-COLUMN|set-column &lt;column&gt; &lt;expression&gt;|Sets a column the result of expression execution.|
|SPLIT-TO-COLUMNS|split-to-columns &lt;column&gt; &lt;regex&gt;|Splits a column into one or more columns around matches of the specified regular expression.|
|CLEANSE-COLUMN-NAME|cleanse-column-names|Sanatizes column names: trims, lowercases, and replaces all but [A-Z][a-z][0-9]_.with an underscore '_'.|
|SET COLUMNS|set columns &lt;columm&gt;[,&lt;column&gt;*]|Sets the name of columns, in the order they are specified.|
|TITLECASE|titlecase &lt;column&gt;|Changes the column values to title case.|
|MERGE|merge &lt;column1&gt; &lt;column2&gt; &lt;new-column&gt; &lt;separator&gt;|Merges values from two columns using a separator into a new column.|
|TEXT-METRIC|text-metric &lt;method&gt; &lt;column1&gt; &lt;column2&gt; &lt;destination&gt;|Calculates the metric for comparing two string values.|
|SET FORMAT|set format csv &lt;delimiter&gt; &lt;skip empty lines&gt;|[DEPRECATED] Parses the predefined column as CSV. Use 'parse-as-csv' instead.|
|FORMAT-UNIX-TIMESTAMP|format-unix-timestamp &lt;column&gt; &lt;format&gt;|Formats a UNIX timestamp using the specified format|
|FILTER-ROW-IF-NOT-MATCHED|filter-row-if-not-matched &lt;column&gt; &lt;regex&gt;|Filters rows if the regex does not match|
|FILTER-ROW-IF-FALSE|filter-row-if-false &lt;condition&gt;|Filters rows if the condition evaluates to false|


