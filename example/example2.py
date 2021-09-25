from marshmallow import Schema

from dwc.meta import MetaFile
from processing import fields
from processing.node import ProcessingContext
from processing.orchestrate import Orchestrator
from processing.sink import LogSink
from processing.source import CsvSource
from processing.transform import MapTransform, LookupTransform


class InputSchema(Schema):
    """
    Example input schema
    """
    ID = fields.String()
    DATE = fields.String()
    SPECIES = fields.String()
    SITEID = fields.Integer()

    class Meta:
        ordered = True

class SiteSchema(Schema):
    ID = fields.Integer()
    NAME = fields.String(missing=None)
    LAT = fields.Float()
    LON = fields.Float()
    STATE = fields.String(missing=None)


class OccurenceSchema(Schema):
    """
    Schema for the output of a simple Darwin Core Occurrence
    """
    catalogNumber = fields.String()
    basisOfRecord = fields.String()
    eventDate = fields.Date(format="%Y-%m-%d")
    verbatimEventDate = fields.String()
    decimalLatitude = fields.Float()
    decimalLongitude = fields.Float()
    stateProvince = fields.String(missing=None)
    locationID = fields.String(missing=None)
    locality = fields.String(missing=None)
    scientificName = fields.String()

    class Meta:
        ordered = True
        uri = 'http://rs.tdwg.org/dwc/terms/Occurrence'
        namespace = 'http://rs.tdwg.org/dwc/terms/'

# Construct a processing context that wiill read from the example directory and put results in
# to the log. If there are errors, they will be sent to the log output
# Setting the output_dir to None sends any output to the work directory
context = ProcessingContext("ctx", dangling_sink_class=LogSink, config_dirs=['./config'], input_dir='.', output_dir=None)

# Read data from the input file
input = CsvSource.create("input", "example2.csv", "excel", InputSchema())
# Read data from the sites list
sites = CsvSource.create("sites", "sites.csv", "excel", SiteSchema())
# Join the input and site data. Remp name from the site to SITE_NAME to make it easier to remember and include LAT, LON and STATE
lookup_sites = LookupTransform.create("lookup_sites", input.output, sites.output, 'SITEID', 'ID', lookup_map={'NAME': 'SITE_NAME'}, lookup_include=['LAT', 'LON', 'STATE'])
# Map the input data onto the output
transform = MapTransform.create("transform", lookup_sites.output, OccurenceSchema(), {
    'catalogNumber': 'ID',
    'basisOfRecord': MapTransform.constant('MachineObservation'), # Constant value
    'eventDate': MapTransform.dateparse('DATE', '%d/%m/%y', '%d-%b-%y'), # Parse a date into this format
    'verbatimEventDate': 'DATE',
    'decimalLatitude': 'LAT',
    'decimalLongitude': 'LON',
    'stateProvince': 'STATE',
    'locality': 'SITE_NAME',
    'scientificName': 'SPECIES'
})
# Printe the resulting converted file to the output
# Note that locationID will not be included in the output because there is no input
output = LogSink.create("output", transform.output, reduce=True)
# Create a meta.xml file for the result
meta = MetaFile.create("meta", output)

orchestrator = Orchestrator("orchestrator", [ input, sites, lookup_sites, transform, output, meta ])
orchestrator.run(context)