# Pre-Processing of Taxonomy Data

There are a lot of data files in various different forms used to act as sources for 
the ALA taxonomy. These used to be handled by a massive swirl of Talend jobs.
However, the jobs were too complicated, too hard to repurposem hard to partition 
and version and Talend has difficulties with iterative processing.

So, this is a port of the various Talend jobs into Python, to make them  more accessible.
The Talend model, however, is quite useful, so the abstractions used bear
more than a passing resemblance.

* [Basics](#basics)
  * [Schemas](#schemas)
  * [Nodes, Ports, Contexts, Orchestrators](#nodes-ports-contexts-orchestrators)
  * [Records, Datasets, Keys, Indexes](#records-datasets-keys-indexes)
* [Nodes](#nodes)
  * [Sources](#sources)
  * [Sinks](#sinks)
  * [Transforms](#transforms)
  * [Darwin Core](#darwin-core)
  * [Predicates](#predicates)
  * [ALA Transforms](#ala-transforms)
* [Creating a Schema](#creating-a-schema)
* [Creating a Workflow](#creating-a-workflow)
* [Examples](example/README.md)  
* [The great big to-do list](#the-great-big-to-do-list)  

## Basics

All packages use the [attrs](https://www.attrs.org) package to
define typed attributes on the various elements.
This leads to a codeing style where you tend to use a class method
to create a new instance of the class, rather than just the constructor directly. 
So `CsvSource.create("foo", "bar.csv", "baz", schema)` rather than
`CsvSource("foo", "bar.csv", "baz", schema)`
This approach allows some arguments to be interpreted before being fed to
the constructor.

### Schemas

All records passing through the processing chains described below
conform to a schema.
The processing system uses [Marshmallow](https://marshmallow.readthedocs.io/en/stable/)
as a tool to define schemas.
Schemas are defined by writing a class with a number of named fields,
one for each column of data.
Instances of the class are passed about as defintions of what a record means.

Schemas can be created dynamically and a number of
the processing nodes described below often create a reduced or
enhanced schema for outputs.

Generally, schemas are ordered and that order is honoured when
constructing output schemas, writing to files, etc.

### Nodes, Ports, Contexts, Orchestrators

A [*Node*](processing/node.py) is an arbitrary processing component.
It has an identifier, a set of *inputs* (possibly none), a set of *outputs*
(again, possibly none) and some sort of execute method that takes the
inputs, does whatever is needed with them and produces outputs.
It may also have a set of *errors* which provide a way of managing and
saving problem data, so that any issues can be examined and corrected.

An input or output is called a [*Port*](processing/dataset.py).
A port has an identifier (usually a UUID) and a [schema](#Schemas).
Ports are named instance variables in nodes and can be used to
link nodes together. For example.

```python
names = CsvSource,create("name_source", "names.csv", "excel", name_schema)
valid_names = FilterTransform,create("valid_name", names.output, lambda r: r.isValid)
valid_output = CsvSink.create("valid_output", valid_names.output, "valid_names.csv")
```

creates three nodes.
The `names` node reads a CSV filem called `names.csv` in excel format.
This node has an output port.
The output port is linked to the next node, `valid_names` which filters the record
and only gets those records where isValid is true.
The output port from `valid_names` is then given to `valid_output` which writes
a CSV file containing the valid names.
Once the `name_schema` is specified, the schema flows through the
various other nodes.

Nodes also contain logging facilities and statisitics counters,
so that you can have have a sense of what is going on.

A [*ProcessingContext*](processing/node.py) is a configuration and
execution context.
Contexts hold information such as which directories to read data from,
where to store work information and where to output the final result.
Contexts also contain the datasets produced by each node and linked to each port.
In the above example, the actual location of `names.csv` and the final resting
place of `valid_names.csv` is determined by the context.
While the nodes are being executed, the data associated with the
`names.output` and `valid_names.output` ports are also stored in the context.
It is possible to run the same processing chain with multiple different contexts.

Processing data can, therefore, be done by presenting a ProcessingContext
to a collection of linked nodes and running the nodes in order.
Each node gets data from the context, transforms it and places the results
back into the context.
An [*Orchestrator*](processing/node.py) is a type of node takes a list of nodes
and executes the list in dependency order.
Basically, a node will not run until the input data that it expects
becomes available.

### Records, Datasets, Keys, Indexes

Note that you will probably not have to program using these classes
if all you are doing is stringing pre-built nodes.
However, it helps to know what is going on under the hood,
since the concepts here, particularly keys, help define some transformartions.

The basic unit of data is the [*Record*](processing/dataset.py).
A record contains a line number (generally, the line number of the orginal
source data) that can be used to trace problems.
A record also contains a list of issues attached to the record.
The main element of the record is a dictionary of column values that
match a [schema](#Schemas).

Record elements can be accessed using dot notation, eg.
`record.taxonID` will retrieve the `taxonID` column value in the record
or return `None` if the value is not present.

Records can be built by constructing a dictionary and then a
Record object. For example:

```python
dwc = {
    'taxonID': source.taxonID,
    'scientificName': assembleName(record.genus, record.specificEpithet),
    'nomenclaturalCode': self.nomenclaturalCode,
}
result = Record(record.line, dwc, record.issues)
```

A complete collection of records from an operation such as reading a
file or processing a node is called a [*Dataset*](processing/dataset.py).
Datasets contain an ordered list of rows, with each row a record that
conforms to the dataset's schema.
Datasets are, essentially, attached to ports in the context as the
esecution proceeds.

Datasets, by default, do not contain primary keys or any clever ways of
accessing the data.
If you want a lookup table, you need to build an indexed version of the
dataset.
To do that, you need a [*Keys*](processing/dataset.py) instance.
Keys are build against a schema and contain the fields in the schema that can
be used to construct a key tuple for each record.
An example key specification is

```python
keys = Keys.get(name_schema, ('PRIMARY_RANK', 'SECONDARY_RANK'))
```

An [*Index*](processing/dataset.py) is a dataset indexed by a key.
It can be use to provide fast lookup of a dataset.
Normally, indexes are expected to be unique but they can also be set to
have multple matches or first-found matches.

# Nodes

All nodes have a `create` class method that can be used to construct the node.
Use the create method in preference to a constructor.

All nodes contain the following common arguments:

* **id**: str The first item in all nodes is a unique string id that can be used to label
  the node during execution.
* **description**=str A description of the node
* **tags**=Dict[str, object] Any tags associated with the node
* **logger**=logger A specific logger for the node.
  If absent, a default logger is created.
* **no_errors**=bool Fail the execution if any errors are detected. True by default.
* **fail_on_exception**=bool If there is an exception while processing, raher
  than record the exception as an error and continue, propagate the exception.
  If false, exceptions are collected an reported as issues but the processing continues.
  False by default.
* **counts**=Dict[str, int] An initial dictionary of statistics.
  Empty by default.
* **break_begin**=bool If set to true, it will run a line of code in the [Node#begin](processing/node.py) 
  that you can place a breakpoint at and geta  per-node break befoire the node is executed.
  False by default.
* **break_begin**=bool If set to true, it will run a line of code in the [Node#commit](processing/node.py) 
  that you can place a breakpoint at and geta  per-node break after the node has been executed.

## Sources

Sources provide data.

### [CsvSource](processing/source.py)

Reads data from a CSV file.

`processing.source.CsvSink.create(id, file, dialect, schema)`

* **file**:str The name of the file to read.
  During processing, the context is used to provide a search path of
  directories to use to find the file.
* **dialect**:str The name of the CSV dialect to use when reading the file.
* **schema**:marshmallow.Schema The schema that the CSV data has to conform to.
  Each row is matched against the schema and errors are reported.
* **encoding**=str The file encoding. Defaults to `utf-8` but may need to be set to `utf-8-sig`
  to accomodate byte order marks at the start of the file.
* **predicate**=Callable A Record -> bool predicate for reading rows. The function is called
  with a single argument, the record representing the row, which can be used to eliminate
  unwanted rows from a large dataset before they become a memory problem.
  By default, all records are accepted.
  

### [ExcelSource](processing/source.py)

Reads data from an Excel spreadshet file.

`processing.source.CsvSink.create(id, file, sheet, schema)`

* **file**:str The name of the file to read.
  During processing, the context is used to provide a search path of
  directories to use to find the file.
* **sheet**:str The name of sheet within the workbook to read from.
* **schema**:marshmallow.Schema The schema that the CSV data has to conform to.
  Each row is matched against the schema and errors are reported.
* **predicate**=Callable A Record -> bool filter predicate for reading rows. The function is called
  with a single argument, the record representing the row, which can be used to eliminate
  unwanted rows from a large dataset before they become a memory problem.
  By default, all records are accepted.

## Sinks

Sinks write data to a permanent location

### [CsvSink](processing/sink.py)

Writes data to a CSV file. 

`processing.sink.CsvSink.create(id, input, file, dialect, work)`

* **input**:Port The port that produces the data.
* **file**:str The name of the file to write.
  The context determines the full path of the output file.
* **dialect**str The CSV dialect to use while writing
* **work**:bool Write to the work directory for a temporary file rather than the output directory. 
  False by default.
* **fieldnames**=List[str] The field names to use when accessing record data.
  By default, these are derived from the schema.
* **fieldkeys**=List[str] The column names to use as headers.
  By default, these are derived from the schema.
* **reduce**=bool If true, the dataset is examined to see which columns arfe all empty
  and can be left ou. False by default.
  
### [LogSink](processing/sink.py)

Writes data to the log.
This sink is useful for debugging or when you just want to have a
report of odd things.

`processing.sink.LogSink.create(id, input)`

* **input**:Port The port that produces the data.
* **fieldnames**=List[str] The field names to use when accessing record data.
  By default, these are derived from the schema.
* **fieldkeys**=List[str] The column names to use as headers.
  By default, these are derived from the schema.
  By default, these are derived from the schema.
* **reduce**=bool If true, the dataset is examined to see which columns arfe all empty
  and can be left ou. False by default.
* **limit**=int Only log the first *limit* rows. By default None, which means log everything. 

### [NullSink](processing/sink.py)

Writes data to nowhere. 
The null sink is a useful way of telling the orchestrator not to fret
about data outputs that are not needed.

`processing.sink.NullSink.create(cls, id, input1, input2, ...)`

* **input***n*:Port The list of ports that can be consigned to oblivion.

## Transforms

A basic toolkit of ways of filtering, transforming and linking data.
All transforms have an error port that can be used to report on unpleasentness
during transformation.

### [FilterTransform](processing/transform.py)

Filter rows based on a predicate.

`processing.transform.FilterTransform.create(id, input, pred)`

* **input**:Port The source of records
* **predicate**:Callable A Record -> bool predicate that takes a record and
  returns True/False depending on whether the record should be included
  in the output.
  
### [ProjectTransform](processing/transform.py)

Project records on the current schema onto a new schema.
Any fields that are part of the input schema are copied across
and any new fields are set to `None`

`processing.transform.ProjectTransform.create(id, input, schema)`

* **input**:Port The source of records.
* **schema**:marshmallow.Schema The schema to project on to

### [LookupTransform](processing/transform.py)

Generate a joined output from two inputs.
The schemas can be re-mapped to ensure that columns do not get
ignored or over-written.

Keys can be either a string giving a field name for a single key,
a tuple containing multuple field names or a keys object.
During lookup, a tuple is derived from the input record using
`input_keys` and the corresponding record is found in the lookup
dataset using `lookup_keys`.
For example, if you are looking up a nomenclatural code in a term -> value vocabulary
dictionary, then the `input_keys` would be `'nomenclaturalCode'` and the `lookup_keys`
would be `'term'`

Input and lookup schemas can be rewritten during the join.
There are three basic ways of doing this.
A map directly maps a source field name onto a new field name.
An include list maps a source field name onto the same field name but will
not include anything not in the include list.
An exclude list maps anything not in the list onto the same field name but
will not include anything in the exclude list.
The map takes precedence over the include list, which in turn takes precedence
over the exclude list.
There is also an option to simply prefix the lookup fields.

`processing.transform.LookupTransform(id, input, lookup, input_keys, lookup_keys)`

* **input**:Port The main input data source
* **lookup**:Port The lookup data source
* **input_keys** The key(s) to use
* **lookup_keys** The key(s) to the lookup table.
* **input_map**=Dict[str, str] The columns to take (and rename) from the input
* **lookup_map**=Dict[str, str] The columns to take (and rename) from the lookup
* **input_include**=List[str] The columns to include from the input
* **lookup_include**=List[str] The columns to include from the input
* **input_exclude**=List[str] The columns to exclude from the input
* **lookup_exclude**=List[str] The columns to exclude from the input
* **lookup_prefix**=str The prefix to append to lookup columns
* **lookup_type**=IndexTYpe The type of lookup to use when looking up values, defaults to `IndexType.UNIQUE` 
* **reject**=bool Reject unmatched input records.
  False by default
* **merge**=bool Merge schemas. If false, only the input schema is passed through,
  creating a lookup check.
  True by default
* **ignore_duplicates**=bool Ignore duplicate records with the same key in the lookup,
  only using the first one.
  If false, duplicate key values in the lookup will cause an error.
  False by default.
* **overwrite**=bool By default, a lookup will not overwrite fields with the same name
  as the input record when merging results. If overwrite is true, then the lookup
  result will overwrite the values in the input record.
* **record_unmatched**=bool Provide an `unmatched` output for records that are
  not found in the lookup. False by default. This flag is independent of the reject flag.

### [MergeTransform](processing/transform.py)

Merge multiple source datasets, with the same schema, into a
single output dataset.
Sources are merged in order.

`processing.transform.MergeTransform(id, source1, source2, ...)`

* **source***n*: Port a list of the sources to merge.

### [MapTransform](processing/transform.py)

Map data from a source to a destination.

`processing.transform.MapTransform(id, input, schena map)`

* **input**:Port The main input data source
* **schema**:Schema The output schema
* **map**:Dict[str, object] The mapping rules
* **auto**:bool Auto-map any unmapped fields. False by default.
  Specific mappings override these mappings.

The mapping rules can be used to map onto the output schema.
The key is the field name of the output schema.
The value is the transofrmation rule.
The rules can either be:

* A function that takes a Record as an argument and returns an appropriate
  mapping. For example `lambda r: r.datasetID` or the named `mapDatasetID`.
  Functions can have zero (constant), one (record), two (record and contaxt)
  or three (record, context and additional data)
* A string containing a field name. The contents of the field is copied
  onto the corresponding output field.
  Suitable conversions are done automatically.
  
There are a number of helpful functions available:

* `MapTransform.constant(value)` Provide a constant value.
* `MapTransform.dateparse(field, format1, format2, ...)` Parse a date found
  in the named field. Parsing is attempted on each format in order and `None` returned if
  parsing fails.

### [DenormaliseTransform](processing/transform.py)

Denormalise a row into several rows, by splitting the contents of a field.

`processing.transform.DenormaliseTransform(id, input, field, delimiter)`

* **input**:Port The main input data source
* **field**:str The name of the field to denormalise on
* **delimiter**:str The delimiter to use when splitting the contents of the field (eg '|')

The output record will have an "_index" field attached, giving the index number of
the resulting denormalisation, starting with 0 for the first value in the field.

## Darwin Core

Nodes that are useful for building Darwin Core Archives

### [MetaFile](dwc/meta.py)

Create a DwCA `meta.xml` file for the resulting data.

`dwc.meta.MetaFile.create(id, sink1, sink2, ...)`

* **sink***n*: Sink A sink node.
  The first sink is the core file.
  Subsequent sinks are the extension files.
  The identifier column in all files is assumed to be column 0.
  
To create an accurate `meta.xml` file, the sink schema needs to have
fields and the schema itself annotated with the field URI.
By default, if the `Meta` class contains a `namespace` entry,
the URI is constructed by adding the field name to the namespace.
Specific mappings are done by including a `metadata` dictionary with a `uri` entry
in the schema declaration.
For example:

```python
class TaxonSchema(Schema):
    taxonID = fields.String()
    parentNameUsageID = fields.String()
...
    source = fields.String(missing=None, metadata={'uri': 'http://purl.org/dc/terms/source'})

    class Meta:
        ordered = True
        uri = 'http://rs.tdwg.org/dwc/terms/Taxon'
        namespace = 'http://rs.tdwg.org/dwc/terms/'
```

In this case, the `source` column is associated with the 
`http://purl.org/dc/terms/source` URI and this is what will appear in the
`meta.xml` file.
The `taxonID` will appear as `http://rs.tdwg.org/dwc/terms/taxonID`

If there is no metadata, the bare field name will appear.

### [EmlFile](dwc/meta.py)

Create an Ecoinformatics Metadata Language file, giving provenance information
for the DwCA.
This is currently oriented towards 

`dwc.meta.EmlFile.create(id, source, publisher)`

* **source**: Port An input port containing metadata for the data source.
* **publisher**L Port An optional port containing details for the data publisher.
  
### [DwcTaxonValidate](dwc/transform.py)

Validate a DwC Taxon record for structural integrity.
Any records missing parents or accepted taxa are shunted off to the error port.

`dwc.transform.DwcTaxonValidate.create(id, input)`

* **input**:Port The main input data source

### [DwcTaxonClean](dwc/transform.py)

Remove any dangling parents and what-have you from a taxonomy.

`dwc.transform.DwcTaxonClean.create(id, input)`

* **input**:Port The main input data source

### [DwcTaxonTrail](dwc/transform.py)

Complete a taxonomic dataset by pulling records across from a reference dataset.
Any missing parent taxa or accepted taxa are, recursively, included in the output.
This transform can be used to compute a partial taxonomy and then include the extra
entries needed to make the result structurally valid.

`dwc.transform.DwcTaxonTrail(id, input, reference, reference_keys, parent_keys, accepted_keys)`

* **input**:Port The main input data source
* **reference**:Port The reference (complete) data source
* **reference_keys** The keys that identify the taxon identifier in both the input and reference data (eg 'taxonID')
* **parent_keys** The keys that provide the parent taxon identifier in the input dataset (eg. 'parentNameUsageID')
* **accepted_keys** The keys that provide the accepted taxon identifier in the input dataset (eg. 'acceptedNameUsageID')

### [DwcTaxonReidentify](dwc/transform.py)

Change the identifiers for the taxon, parent and accepted.
The output dataset is the same dataset, with the identifiers converted by a re-writing function.
An additional mapping dataset provides a lookup table of old to new identifier.

`dwc.transform.DwcTaxonReidentify(id, input, reference, reference_keys, parent_keys, accepted_keys)`

* **input**:Port The main input data source
* **identifier_keys** The keys that identify the in-use taxon identifier in the input (eg 'taxonID')
* **parent_keys** The keys that provide the parent taxon identifier in the input dataset (eg. 'parentNameUsageID')
* **accepted_keys** The keys that provide the accepted taxon identifier in the input dataset (eg. 'acceptedNameUsageID')
* **identifier**:Callable A Record -> str function that takes a record and returns a new identifier for the record
  (eg. `lambda r: r.taxonConceptID + "-" + r.taxonRank)`

## Predicates

Predicates are nodes that can be used as a filter predicate in other nodes.
They usually take the form of a lookup table that has been read from a file
or other simple dataset.

## ALA Transforms

### [SpeciesListSource](ala/transform.py)

Read a species list from the ALA species list server.
The species list to read is given by the `datasetID` default value set in the context.

`ala.transform.SpeciesListSource(id)`

* **service**=str The species list web service endpoint. Defaults to `https://lists.ala.org.au/ws`

### [CollectorySource](ala/transform.py)

Read a collectory metadata from the ALA collectory server.
The metadata to read is given by the `datasetID` default value set in the context.

`ala.transform.CollectorySource(id)`

* **service**=str The collectory web service endpoint. Defaults to `https://collections.ala.org.au/ws`

### [PublisherSource](ala/transform.py)

Pick up the publisher information from the configuration directory.

`ala.transform.PublisherSource(id)`

* **file**=str The file with the data. Defaults to `ala-metadata.csv`

## Orchestration

These nodes call other nodes, based on some sort of condition, loop, etc.

### [Orchestrator](processing/orchestrate.py)

Run a series of nodes, linking up the inputs of a waiting node with the
outputs of a node that has run.

`processing.orchestrate.Orchestrator(id, nodes)`

* **nodes**: List[Node] a list of nodes to orchestrate

### [Selector](processing/orchestrate.py)

Select a node to run, based on the data in a record.
The node that is being run is run in sub-context with defaults given by the data
in the input.

`processing.orchestrate.Selector(id, id, input, selector_key, directory_key, input_dir_key, output_dir_key, config_dir_key, work_dir_key, node1, node2, ...)`

* **input**: Port An input data source
* **selector_key** The column of the input which gives the id of the node to run
* **directory_key** The column of the input which gives the default subdirectory to use with input, configuration, etc.
* **input_dir_key** The column of the input which gives the input subdirectory to use. 
  If None then there is no key.
  If the data contains None, then the directory key is used.
* **output_dir_key** The column of the input which gives the output subdirectory to use. 
  If None then there is no key.
  If the data contains None, then the directory key is used.
* **config_dir_key** The column of the input which gives the configuration subdirectory(s) to use. 
  If None then there is no key.
  If the data contains None, then the directory key is used.
  Multiple configuration subdirectories can be specified, separated by commas.
* **work_dir_key** The column of the input which gives the work subdirectory to use. 
  If None then there is no key.
  If the data contains None, then the directory key is used.
* **node***n* The nodes to select.
  These nodes are keyed by id.

# Creating a Schema

Any input file (and many output files) will require a schema.
This is a class that inherits from `marshmallow.Schema` and contains a list of 
named fields.
Since Marshmallow fields do not interpret the sort of empty strings one
gets in a CSV file very well, there is a shadow set in the package
`processing.fields` that converts empty strings into None for you.
The following is a simple schema declaration:

```python
from marshmallow import Schema
from processing import fields

class ExampleSchema(Schema):
  key = fields.Integer()
  identifier = fields.String()
  description = fields.String(missing=None)
  modified = fields.Date(missing=None, format='%d/%b/%y', uri='http://purl.org/dc/terms/modified')
  category = fields.String(missing=None)

  class Meta:
    ordered = True
    uri = 'http://id.ala.org.au/terms/Vocabulary'
    namespace = 'http://id.ala.org.au/terms/'
```

This gives five four fields.
The `key` and `identifier` fields are required and, if missing, will flag the
record with an error.
The `description`, `modified` and `category` fields can be empty and, if missing the data will
be set to `None`.
The modified field is a date and comes in the format specified, eg. "20/03/21".

The `Meta` interior class contains schema metadata.
Setting `ordered = True` means that input and output of data will follow the
ordering specified.
Generally, you will want to use ordered schemas.

If a `uri` is supplied, then the output is tagged as an instance of this class.

If a field has a `uri` parameter, then this is used to provide the URI for the field n
a [metafile](#metafile).
If a `namespace` is supplied, and not set in the field metadata, URIs for these fields are 
constructed by adding the field name to the namespace.

# Creating a Workflow

Creating a workflow involves creating instances of nodes, all linked together.
The workflow can then be run by creating a context, which contains the onfiguration
information for the workflow and then creating and orchestrator to make everything
happen in the right order.
The following is an example workflow:

```python
from processing.node import ProcessingContext
from processing.orchestrate import Orchestrator
from processing.sink import CsvSink
from processing.source import CsvSource
from processing.transform import FilterTransform, LookupTransform

# First create the directories where we will do everything
config_dirs = ['/data/config/common', '/data/config/dr123']
input_dir = '/data/resources/dr123'
output_dir = '/data/processed/dr123'
work_dir = '/data/tmp/dr123'

# Create schemas
ex_schema = ExampleSchema()
vocab_schema = VocabularySchema()

# Read a file containing example data and some category information
source = CsvSource.create("source", "example.csv", "excel", ex_schema)
category_vocab = CsvSource("category_vocab", "categories.csv", "excel", vocab_schema)

# Only process those examples with categories
categorised = FilterTransform.create("categorised", source.output, lambda record: record.category is not None)

# Link with the vocabulary
linked = LookupTransform.create("link_category", categorised.output, category_vocab.output, 'category', 'term',
                                lookup_map={'description': 'categoryDescription'})

# Write out the result
sink = CsvSink.create("sink", linked.output, "example_categories.csv", "excel")

# Create an orchestrator for all these nodes
orchestrator = Orchestrator("orch", [source, category_vocab, categorised, linked, sink])
# Create a context for it to run in. The CsvSink is how to handle "dangling" outputs
context = ProcessingContext("ctx", CsvSink, work_dir=work_dir, config_dirs=config_dirs, input_dir=input_dir,
                            output_dir=output_dir)

# And Ta-Da!
orchestrator.run(context)
```

# The great big to do list

* Looping dataflows are not present but can be made possible.
* Datasets are not cut loose and garbage collected efficiently.
  Right now, this is not a problem but will be for larger processing chains.
* Datasets are processed in total, rather than as streams.
  Right now, this is not important.
* Cache indexes on the same dataset using the same keys
* Generate schemas by examining a file and seeing what's there














































































