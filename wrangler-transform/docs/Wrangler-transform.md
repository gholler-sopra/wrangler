# Wrangler

Wrangler accelerator is used to apply data transformation directives to your data records. The directives can be applied either through the Data Preparation visual interface or by manually configuring the Wrangler accelerator to perform the required data transformations.

## Configuration

The following table describes the fields as displayed in the accelerator properties dialog box.

| Configuration Field | Required | Default | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ------------------- | -------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Input Field         | No       | `*`     | Name of the input field(s) on which the directives must be run. The default value '*' indicates all fields.                                                                                                                                                                                                                                                                                                                                                                                                  |
| Precondition        | No       | `false` | Filter condition (written in Java Extended Expression Language or JEXL) which is evaluated before passing a data record to the Wrangler for processing. This condition can be applied to all or specific columns in a data record (as per the JEXL expression). If the precondition is evaluated to true for a record, the specific record is filtered by the Wrangler accelerator. For example, body.contains ("OCS") can be used to filter all records in which the body column contains the string "OCS". |
| Directives          | Yes      | N/A     | All data manipulation commands to be executed by Wrangler. The Wrangler applies the specified directives to the input records and outputs the processed records in a new dataset.                                                                                                                                                                                                                                                                                                                            |
| Failure Threshold   | No       | 1       | Maximum number of errors tolerated before exiting the pipeline processing                                                                                                                                                                                                                                                                                                                                                                                                                                    |

## Precondition filter

The `precondition` filter performs filtering on your data records before delivering these records for data preparation. 

You need to specify a filter condition as a `JEXL` expression. If this filter condition is evaluated to `true` for a record, the Wrangler removes the specific record from the list of data records to be transformed. 

For example, to filter all header records from a CSV file where the
header record is at the start of the file, you need to specify the following filter condition:

```
  offset == 0
```

The aforementioned filter condition filters all records that have an `offset` of zero.

Another example is where you want to filter input records containing a substring 
''YCES' in any of the records. To do so, you need to use the following filter condition:

```
  body.contains("YCES")
```

This will filter all records that contain `YCES` anywhere in their `body` column.

## Directives

The Wrangler accelerator supports numerous directives and their variations. 
These directives are available as commands as mentioned below in [Available Directives](#available-directoves).

## Interractive Wrangling using DataPrep Service

DataPrep service supports many row transformation directives which are actually executed by the Wrangler accelerator when you run your data pipeline. For interactively applying these directives, click `Pipelines` to display the DataPrep user interface (UI). Next, select the file that will load the desired data into DataPrep UI. After the data is loaded, you can see drop-downs for each column which provide options to perform simple data transformations, such as parse, data type, format, and calculate. Apply these directives to parse or transform the data until it looks good enough to proceed to attaching next stage. However, these drop-downs do not contain the complete list of transformation functions supported by Data Preparation. It is recommended to use the command console to avoid errors.

Data Preparation provides a command console to help you run data transformation directives as shell commands. You can specify the directives at the command line and the results are shown immediately in the Data Preparation UI. The command console is visible at the bottom of the Preparation page. You can view all directives applied to your data on the `Directives` tab in the right panel.

Please find below the [DIRECTIVES CHEATSHEET](#directives-cheatsheet) to find mapping between the UI interractive column drop-down option and the corresponding directive command. Note that these UI options and commands have intuitive names to easily infer this mapping.

## Usage Notes

All input record fields are made available to the Data Prep directives when `*` is used as the input field to be transformed. They are in the record in the same order as they appear.

Note that if the Wrangler accelerator does not operate on all input record fields or a field is not configured as part of the output schema and you are using the `set columns` directive, you may see inconsistent behavior. Use the `drop` directive to drop any fields that are not used in the data preparation.

This accelerator uses the `emiterror` capability to emit records that fail parsing into a
separate error stream, allowing the aggregation of all errors. However, if the _Failure
Threshold_ is reached, then the pipeline will fail.

## Available Directives

The following directives are currently available which can be applied to your data records using Wrangler:

| Directive                                                                            | Description                                                         |
| ------------------------------------------------------------------------------------ | ------------------------------------------------------------------- |
| **Parsers**                                                                          |                                                                     |
| [JSON Path](wrangler-docs/directives/json-path.md)                                   | Uses a DSL (a JSON path expression) for parsing JSON records        |
| [Parse as AVRO](wrangler-docs/directives/parse-as-avro.md)                           | Parsing an AVRO encoded message - either as binary or json          |
| [Parse as AVRO File](wrangler-docs/directives/parse-as-avro-file.md)                 | Parsing an AVRO data file                                           |
| [Parse as CSV](wrangler-docs/directives/parse-as-csv.md)                             | Parsing an input record as comma-separated values                   |
| [Parse as Date](wrangler-docs/directives/parse-as-date.md)                           | Parsing dates using natural language processing                     |
| [Parse as Excel](wrangler-docs/directives/parse-as-excel.md)                         | Parsing excel file.                                                 |
| [Parse as Fixed Length](wrangler-docs/directives/parse-as-fixed-length.md)           | Parses as a fixed length record with specified widths               |
| [Parse as HL7](wrangler-docs/directives/parse-as-hl7.md)                             | Parsing Health Level 7 Version 2 (HL7 V2) messages                  |
| [Parse as JSON](wrangler-docs/directives/parse-as-json.md)                           | Parsing a JSON object                                               |
| [Parse as Log](wrangler-docs/directives/parse-as-log.md)                             | Parses access log files as from Apache HTTPD and nginx servers      |
| [Parse as Protobuf](wrangler-docs/directives/parse-as-log.md)                        | Parses an Protobuf encoded in-memory message using descriptor       |
| [Parse as Simple Date](wrangler-docs/directives/parse-as-simple-date.md)             | Parses date strings                                                 |
| [Parse XML To JSON](wrangler-docs/directives/parse-xml-to-json.md)                   | Parses an XML document into a JSON structure                        |
| [Parse as Currency](wrangler-docs/directives/parse-as-currency.md)                   | Parses a string representation of currency into a number.           |
| **Output Formatters**                                                                |                                                                     |
| [Write as CSV](wrangler-docs/directives/write-as-csv.md)                             | Converts a record into CSV format                                   |
| [Write as JSON](wrangler-docs/directives/write-as-json-map.md)                       | Converts the record into a JSON map                                 |
| [Write JSON Object](wrangler-docs/directives/write-as-json-object.md)                | Composes a JSON object based on the fields specified.               |
| [Format as Currency](wrangler-docs/directives/format-as-currency.md)                 | Formats a number as currency as specified by locale.                |
| **Transformations**                                                                  |                                                                     |
| [Changing Case](wrangler-docs/directives/changing-case.md)                           | Changes the case of column values                                   |
| [Cut Character](wrangler-docs/directives/cut-character.md)                           | Selects parts of a string value                                     |
| [Set Column](wrangler-docs/directives/set-column.md)                                 | Sets the column value to the result of an expression execution      |
| [Find and Replace](wrangler-docs/directives/find-and-replace.md)                     | Transforms string column values using a "sed"-like expression       |
| [Index Split](wrangler-docs/directives/index-split.md)                               | (_Deprecated_)                                                      |
| [Invoke HTTP](wrangler-docs/directives/invoke-http.md)                               | Invokes an HTTP Service (_Experimental_, potentially slow)          |
| [Quantization](wrangler-docs/directives/quantize.md)                                 | Quantizes a column based on specified ranges                        |
| [Regex Group Extractor](wrangler-docs/directives/extract-regex-groups.md)            | Extracts the data from a regex group into its own column            |
| [Setting Character Set](wrangler-docs/directives/set-charset.md)                     | Sets the encoding and then converts the data to a UTF-8 String      |
| [Setting Record Delimiter](wrangler-docs/directives/set-record-delim.md)             | Sets the record delimiter                                           |
| [Split by Separator](wrangler-docs/directives/split-by-separator.md)                 | Splits a column based on a separator into two columns               |
| [Split Email Address](wrangler-docs/directives/split-email.md)                       | Splits an email ID into an account and its domain                   |
| [Split URL](wrangler-docs/directives/split-url.md)                                   | Splits a URL into its constituents                                  |
| [Text Distance (Fuzzy String Match)](wrangler-docs/directives/text-distance.md)      | Measures the difference between two sequences of characters         |
| [Text Metric (Fuzzy String Match)](wrangler-docs/directives/text-metric.md)          | Measures the difference between two sequences of characters         |
| [URL Decode](wrangler-docs/directives/url-decode.md)                                 | Decodes from the `application/x-www-form-urlencoded` MIME format    |
| [URL Encode](wrangler-docs/directives/url-encode.md)                                 | Encodes to the `application/x-www-form-urlencoded` MIME format      |
| [Trim](wrangler-docs/directives/trim.md)                                             | Functions for trimming white spaces around string data              |
| **Encoders and Decoders**                                                            |                                                                     |
| [Decode](wrangler-docs/directives/decode.md)                                         | Decodes a column value as one of `base32`, `base64`, or `hex`       |
| [Encode](wrangler-docs/directives/encode.md)                                         | Encodes a column value as one of `base32`, `base64`, or `hex`       |
| **Unique ID**                                                                        |                                                                     |
| [UUID Generation](wrangler-docs/directives/generate-uuid.md)                         | Generates a universally unique identifier (UUID)                    |
| **Date Transformations**                                                             |                                                                     |
| [Diff Date](wrangler-docs/directives/diff-date.md)                                   | Calculates the difference between two dates                         |
| [Format Date](wrangler-docs/directives/format-date.md)                               | Custom patterns for date-time formatting                            |
| [Format Unix Timestamp](wrangler-docs/directives/format-unix-timestamp.md)           | Formats a UNIX timestamp as a date                                  |
| **Lookups**                                                                          |                                                                     |
| [Catalog Lookup](wrangler-docs/directives/catalog-lookup.md)                         | Static catalog lookup of ICD-9, ICD-10-2016, ICD-10-2017 codes      |
| [Table Lookup](wrangler-docs/directives/table-lookup.md)                             | Performs lookups into Table datasets                                |
| **Hashing & Masking**                                                                |                                                                     |
| [Message Digest or Hash](wrangler-docs/directives/hash.md)                           | Generates a message digest                                          |
| [Mask Number](wrangler-docs/directives/mask-number.md)                               | Applies substitution masking on the column values                   |
| [Mask Shuffle](wrangler-docs/directives/mask-shuffle.md)                             | Applies shuffle masking on the column values                        |
| **Row Operations**                                                                   |                                                                     |
| [Filter Row if Matched](wrangler-docs/directives/filter-row-if-matched.md)           | Filters rows that match a pattern for a column                      |
| [Filter Row if True](wrangler-docs/directives/filter-row-if-true.md)                 | Filters rows if the condition is true.                              |
| [Filter Row Empty of Null](wrangler-docs/directives/filter-empty-or-null.md)         | Filters rows that are empty of null.                                |
| [Flatten](wrangler-docs/directives/flatten.md)                                       | Separates the elements in a repeated field                          |
| [Fail on condition](wrangler-docs/directives/fail.md)                                | Fails processing when the condition is evaluated to true.           |
| [Send to Error](wrangler-docs/directives/send-to-error.md)                           | Filtering of records to an error collector                          |
| [Send to Error And Continue](wrangler-docs/directives/send-to-error-and-continue.md) | Filtering of records to an error collector and continues processing |
| [Split to Rows](wrangler-docs/directives/split-to-rows.md)                           | Splits based on a separator into multiple records                   |
| **Column Operations**                                                                |                                                                     |
| [Change Column Case](wrangler-docs/directives/change-column-case.md)                 | Changes column names to either lowercase or uppercase               |
| [Changing Case](wrangler-docs/directives/changing-case.md)                           | Change the case of column values                                    |
| [Cleanse Column Names](wrangler-docs/directives/cleanse-column-names.md)             | Sanatizes column names, following specific rules                    |
| [Columns Replace](wrangler-docs/directives/columns-replace.md)                       | Alters column names in bulk                                         |
| [Copy](wrangler-docs/directives/copy.md)                                             | Copies values from a source column into a destination column        |
| [Drop Column](wrangler-docs/directives/drop.md)                                      | Drops a column in a record                                          |
| [Fill Null or Empty Columns](wrangler-docs/directives/fill-null-or-empty.md)         | Fills column value with a fixed value if null or empty              |
| [Keep Columns](wrangler-docs/directives/keep.md)                                     | Keeps specified columns from the record                             |
| [Merge Columns](wrangler-docs/directives/merge.md)                                   | Merges two columns by inserting a third column                      |
| [Rename Column](wrangler-docs/directives/rename.md)                                  | Renames an existing column in the record                            |
| [Set Column Header](wrangler-docs/directives/set-headers.md)                         | Sets the names of columns, in the order they are specified          |
| [Split to Columns](wrangler-docs/directives/split-to-columns.md)                     | Splits a column based on a separator into multiple columns          |
| [Swap Columns](wrangler-docs/directives/swap.md)                                     | Swaps column names of two columns                                   |
| [Set Column Data Type](wrangler-docs/directives/set-type.md)                         | Convert data type of a column                                       |
| **NLP**                                                                              |                                                                     |
| [Stemming Tokenized Words](wrangler-docs/directives/stemming.md)                     | Applies the Porter stemmer algorithm for English words              |
| **Transient Aggregators & Setters**                                                  |                                                                     |
| [Increment Variable](wrangler-docs/directives/increment-variable.md)                 | Increments a transient variable with a record of processing.        |
| [Set Variable](wrangler-docs/directives/set-variable.md)                             | Sets a transient variable with a record of processing.              |
| **Functions**                                                                        |                                                                     |
| [Data Quality](wrangler-docs/functions/dq-functions.md)                              | Data quality check functions. Checks for date, time, etc.           |
| [Date Manipulations](wrangler-docs/functions/date-functions.md)                      | Functions that can manipulate date                                  |
| [DDL](wrangler-docs/functions/ddl-functions.md)                                      | Functions that can manipulate definition of data                    |
| [JSON](wrangler-docs/functions/json-functions.md)                                    | Functions that can be useful in transforming your data              |
| [Types](wrangler-docs/functions/type-functions.md)                                   | Functions for detecting the type of data                            |

## Directives Cheatsheet

The following list displays the mapping between the column drop-down options and the underlying directive command:

| Name                      | Usage                                                                                                    | Description                                                                                                                                                       |
| ------------------------- | -------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| SWAP                      | swap &lt;column1&gt; &lt;column2&gt;                                                                     | Swaps the column names of two columns.                                                                                                                            |
| ENCODE                    | encode &lt;base32                                                                                        | base64                                                                                                                                                            |
| XPATH                     | xpath &lt;column&gt; &lt;destination&gt; &lt;xpath&gt;                                                   | Extract a single XML element or attribute using XPath.                                                                                                            |
| GENERATE-UUID             | generate-uuid &lt;column&gt;                                                                             | Populates a column with a universally unique identifier (UUID) of the record.                                                                                     |
| LOWERCASE                 | lowercase &lt;column&gt;                                                                                 | Changes the column values to lowercase.                                                                                                                           |
| WRITE-AS-CSV              | write-as-csv &lt;column&gt;                                                                              | Writes the records files as well-formatted CSV                                                                                                                    |
| PARSE-AS-PROTOBUF         | parse-as-protobuf &lt;column&gt; &lt;schema-id&gt; &lt;record-name&gt; [version]                         | Parses column as protobuf encoded memory representations.                                                                                                         |
| HASH                      | hash &lt;column&gt; &lt;algorithm&gt; [&lt;encode=true                                                   | false&gt;]                                                                                                                                                        |
| JSON-PATH                 | json-path &lt;source&gt; &lt;destination&gt; &lt;json-path-expression&gt;                                | Parses JSON elements using a DSL (a JSON path expression).                                                                                                        |
| MASK-NUMBER               | mask-number &lt;column&gt; &lt;pattern&gt;                                                               | Masks a column value using the specified masking pattern.                                                                                                         |
| TEXT-DISTANCE             | text-distance &lt;method&gt; &lt;column1&gt; &lt;column2&gt; &lt;destination&gt;                         | Calculates a text distance measure between two columns containing string.                                                                                         |
| PARSE-XML-TO-JSON         | parse-xml-to-json &lt;column&gt; [&lt;depth&gt;]                                                         | Parses a XML document to JSON representation.                                                                                                                     |
| PARSE-AS-HL7              | parse-as-hl7 &lt;column&gt; [&lt;depth&gt;]                                                              | Parses &lt;column&gt; for Health Level 7 Version 2 (HL7 V2) messages; &lt;depth&gt; indicates at which point JSON object enumeration terminates.                  |
| FIND-AND-REPLACE          | find-and-replace &lt;column&gt; &lt;sed-expression&gt;                                                   | Finds and replaces text in column values using a sed-format expression.                                                                                           |
| RENAME                    | rename &lt;old&gt; &lt;new&gt;                                                                           | Renames an existing column.                                                                                                                                       |
| PARSE-AS-AVRO             | parse-as-avro &lt;column&gt; &lt;schema-id&gt; &lt;json                                                  | binary&gt; [version]                                                                                                                                              |
| FILL-NULL-OR-EMPTY        | fill-null-or-empty &lt;column&gt; &lt;fixed-value&gt;                                                    | Fills a value of a column with a fixed value if it is either null or empty.                                                                                       |
| SET-TYPE                  | set-type &lt;column&gt; &lt;type&gt;                                                                     | Converting data type of a column.                                                                                                                                 |
| RTRIM                     | rtrim &lt;column&gt;                                                                                     | Trimming whitespace from right side of a string.                                                                                                                  |
| INVOKE-HTTP               | invoke-http &lt;url&gt; &lt;column&gt;[,&lt;column&gt;*] &lt;header&gt;[,&lt;header&gt;*]                | [EXPERIMENTAL] Invokes an HTTP endpoint, passing columns as a JSON map (potentially slow).                                                                        |
| COLUMNS-REPLACE           | columns-replace &lt;sed-expression&gt;                                                                   | Modifies column names in bulk using a sed-format expression.                                                                                                      |
| SEND-TO-ERROR             | send-to-error &lt;condition&gt;                                                                          | Send records that match condition to the error collector.                                                                                                         |
| SET-RECORD-DELIM          | set-record-delim &lt;column&gt; &lt;delimiter&gt; [&lt;limit&gt;]                                        | Sets the record delimiter.                                                                                                                                        |
| SET-VARIABLE              | set-variable &lt;variable&gt; &lt;expression&gt;                                                         | Sets the value for a transient variable for the record being processed.                                                                                           |
| SET-CHARSET               | set-charset &lt;column&gt; &lt;charset&gt;                                                               | Sets the character set decoding to UTF-8.                                                                                                                         |
| WRITE-AS-JSON-OBJECT      | write-as-json-object &lt;dest-column&gt; [&lt;src-column&gt;[,&lt;src-column&gt;]                        | Creates a JSON object based on source columns specified. JSON object is written into dest-column.                                                                 |
| KEEP                      | keep &lt;column&gt;[,&lt;column&gt;*]                                                                    | Keeps the specified columns and drops all others.                                                                                                                 |
| CUT-CHARACTER             | cut-character &lt;source&gt; &lt;destination&gt; &lt;type&gt; &lt;range                                  | indexes&gt;                                                                                                                                                       |
| SPLIT-TO-ROWS             | split-to-rows &lt;column&gt; &lt;separator&gt;                                                           | Splits a column into multiple rows, copies the rest of the columns.                                                                                               |
| XPATH-ARRAY               | xpath-array &lt;column&gt; &lt;destination&gt; &lt;xpath&gt;                                             | Extract XML element or attributes as JSON array using XPath.                                                                                                      |
| FAIL                      | fail &lt;condition&gt;                                                                                   | Fails when the condition is evaluated to true.                                                                                                                    |
| INCREMENT-VARIABLE        | increment-variable &lt;variable&gt; &lt;value&gt; &lt;expression&gt;                                     | Wrangler - A interactive tool for data cleansing and transformation.                                                                                              |
| PARSE-AS-XML              | parse-as-xml &lt;column&gt;                                                                              | Parses a column as XML.                                                                                                                                           |
| PARSE-AS-FIXED-LENGTH     | parse-as-fixed-length &lt;column&gt; &lt;width&gt;[,&lt;width&gt;*] [&lt;padding-character&gt;]          | Parses fixed-length records using the specified widths and padding-character.                                                                                     |
| CHANGE-COLUMN-CASE        | change-column-case lower                                                                                 | upper                                                                                                                                                             |
| SPLIT-EMAIL               | split-email &lt;column&gt;                                                                               | Split a email into account and domain.                                                                                                                            |
| URL-ENCODE                | url-encode &lt;column&gt;                                                                                | URL encode a column value.                                                                                                                                        |
| WRITE-AS-JSON-MAP         | write-as-json-map &lt;column&gt;                                                                         | Writes all record columns as JSON map.                                                                                                                            |
| MASK-SHUFFLE              | mask-shuffle &lt;column&gt;                                                                              | Masks a column value by shuffling characters while maintaining the same length.                                                                                   |
| DROP                      | drop &lt;column&gt;[,&lt;column&gt;*]                                                                    | Drop one or more columns.                                                                                                                                         |
| DECODE                    | decode &lt;base32                                                                                        | base64                                                                                                                                                            |
| SPLIT                     | split &lt;source&gt; &lt;delimiter&gt; &lt;new-column-1&gt; &lt;new-column-2&gt;                         | [DEPRECATED] Use 'split-to-columns' or 'split-to-rows'.                                                                                                           |
| PARSE-AS-SIMPLE-DATE      | parse-as-simple-date &lt;column&gt; &lt;format&gt;                                                       | Parses a column as date using format.                                                                                                                             |
| DIFF-DATE                 | diff-date &lt;column1&gt; &lt;column2&gt; &lt;destination&gt;                                            | Calculates the difference in milliseconds between two Date objects.Positive if &lt;column2&gt; earlier. Must use 'parse-as-date' or 'parse-as-simple-date' first. |
| INDEXSPLIT                | indexsplit &lt;source&gt; &lt;start&gt; &lt;end&gt; &lt;destination&gt;                                  | [DEPRECATED] Use the 'split-to-columns' or 'parse-as-fixed-length' directives instead.                                                                            |
| PARSE-AS-AVRO-FILE        | parse-as-avro-file &lt;column&gt;                                                                        | parse-as-avro-file &lt;column&gt;.                                                                                                                                |
| FILTER-ROW-IF-TRUE        | filter-row-if-true &lt;condition&gt;                                                                     | [DEPRECATED] Filters rows if condition is evaluated to true. Use 'filter-rows-on' instead.                                                                        |
| SPLIT-URL                 | split-url &lt;column&gt;                                                                                 | Split a url into it's components host,protocol,port,etc.                                                                                                          |
| FORMAT-DATE               | format-date &lt;column&gt; &lt;format&gt;                                                                | Formats a column using a date-time format. Use 'parse-as-date` beforehand.                                                                                        |
| QUANTIZE                  | quantize &lt;source&gt; &lt;destination&gt; &lt;[range1:range2)=value&gt;,[&lt;range1:range2=value&gt;]* | Quanitize the range of numbers into label values.                                                                                                                 |
| PARSE-AS-EXCEL            | parse-as-excel &lt;column&gt; [&lt;sheet number                                                          | sheet name&gt;]                                                                                                                                                   |
| PARSE-AS-DATE             | parse-as-date &lt;column&gt; [&lt;timezone&gt;]                                                          | Parses column values as dates using natural language processing and automatically identifying the format (expensive in terms of time consumed).                   |
| TABLE-LOOKUP              | table-lookup &lt;column&gt; &lt;table&gt;                                                                | Uses the given column as a key to perform a lookup into the specified table.                                                                                      |
| FILTER-ROWS-ON            | filter-rows-on empty-or-null-columns &lt;column&gt;[,&lt;column&gt;*]                                    | Filters row that have empty or null columns.                                                                                                                      |
| TRIM                      | trim &lt;column&gt;                                                                                      | Trimming whitespace from both sides of a string.                                                                                                                  |
| URL-DECODE                | url-decode &lt;column&gt;                                                                                | URL decode a column value.                                                                                                                                        |
| FLATTEN                   | flatten &lt;column&gt;[,&lt;column&gt;*]                                                                 | Separates array elements of one or more columns into indvidual records, copying the other columns.                                                                |
| UPPERCASE                 | uppercase &lt;column&gt;                                                                                 | Changes the column values to uppercase.                                                                                                                           |
| CATALOG-LOOKUP            | catalog-lookup &lt;catalog&gt; &lt;column&gt;                                                            | Looks-up values from pre-loaded (static) catalogs.                                                                                                                |
| PARSE-AS-LOG              | parse-as-log &lt;column&gt; &lt;format&gt;                                                               | Parses Apache HTTPD and NGINX logs.                                                                                                                               |
| LTRIM                     | ltrim &lt;column&gt;                                                                                     | Trimming whitespace from left side of a string.                                                                                                                   |
| EXTRACT-REGEX-GROUPS      | extract-regex-groups &lt;column&gt; &lt;regex-with-groups&gt;                                            | Extracts data from a regex group into its own column.                                                                                                             |
| PARSE-AS-CSV              | parse-as-csv &lt;column&gt; &lt;delimiter&gt; [&lt;header=true                                           | false&gt;]                                                                                                                                                        |
| FILTER-ROW-IF-MATCHED     | filter-row-if-matched &lt;column&gt; &lt;regex&gt;                                                       | [DEPRECATED] Filters rows if the regex is matched. Use 'filter-rows-on' instead.                                                                                  |
| PARSE-AS-JSON             | parse-as-json &lt;column&gt; [&lt;depth&gt;]                                                             | Parses a column as JSON.                                                                                                                                          |
| SET COLUMN                | set column &lt;column&gt; &lt;jexl-expression&gt;                                                        | Sets a column by evaluating a JEXL expression.                                                                                                                    |
| STEMMING                  | stemming &lt;column&gt;                                                                                  | Apply Porter Stemming on the column value.                                                                                                                        |
| COPY                      | copy &lt;source&gt; &lt;destination&gt; [&lt;force=true                                                  | false&gt;]                                                                                                                                                        |
| SET-COLUMN                | set-column &lt;column&gt; &lt;expression&gt;                                                             | Sets a column the result of expression execution.                                                                                                                 |
| SPLIT-TO-COLUMNS          | split-to-columns &lt;column&gt; &lt;regex&gt;                                                            | Splits a column into one or more columns around matches of the specified regular expression.                                                                      |
| CLEANSE-COLUMN-NAME       | cleanse-column-names                                                                                     | Sanatizes column names: trims, lowercases, and replaces all but [A-Z][a-z][0-9]_.with an underscore '_'.                                                          |
| SET COLUMNS               | set columns &lt;columm&gt;[,&lt;column&gt;*]                                                             | Sets the name of columns, in the order they are specified.                                                                                                        |
| TITLECASE                 | titlecase &lt;column&gt;                                                                                 | Changes the column values to title case.                                                                                                                          |
| MERGE                     | merge &lt;column1&gt; &lt;column2&gt; &lt;new-column&gt; &lt;separator&gt;                               | Merges values from two columns using a separator into a new column.                                                                                               |
| TEXT-METRIC               | text-metric &lt;method&gt; &lt;column1&gt; &lt;column2&gt; &lt;destination&gt;                           | Calculates the metric for comparing two string values.                                                                                                            |
| SET FORMAT                | set format csv &lt;delimiter&gt; &lt;skip empty lines&gt;                                                | [DEPRECATED] Parses the predefined column as CSV. Use 'parse-as-csv' instead.                                                                                     |
| FORMAT-UNIX-TIMESTAMP     | format-unix-timestamp &lt;column&gt; &lt;format&gt;                                                      | Formats a UNIX timestamp using the specified format                                                                                                               |
| FILTER-ROW-IF-NOT-MATCHED | filter-row-if-not-matched &lt;column&gt; &lt;regex&gt;                                                   | Filters rows if the regex does not match                                                                                                                          |
| FILTER-ROW-IF-FALSE       | filter-row-if-false &lt;condition&gt;                                                                    | Filters rows if the condition evaluates to false                                                                                                                  |
