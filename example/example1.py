from marshmallow import Schema

from dwc.meta import MetaFile
from processing import fields
from processing.node import ProcessingContext
from processing.orchestrate import Orchestrator
from processing.sink import LogSink
from processing.source import CsvSource
from processing.transform import MapTransform


class InputSchema(Schema):
    """
    Example input schema
    """
    ID = fields.String()
    OBSERVER = fields.String(missing=None)
    DATE = fields.String()
    LAT = fields.String()
    LON = fields.String()
    SPECIES = fields.String()

    class Meta:
        ordered = True

class OccurenceSchema(Schema):
    """
    Schema for the output of a simple Darwin Core Occurrence
    """
    catalogNumber = fields.String()
    basisOfRecord = fields.String()
    recordedBy = fields.String(missing=None)
    eventDate = fields.Date(format="%Y-%m-%d")
    verbatimEventDate = fields.String()
    decimalLatitude = fields.Float()
    decimalLongitude = fields.Float()
    verbatimLatitude = fields.String()
    verbatimLongitude = fields.String()
    scientificName = fields.String()

    class Meta:
        ordered = True
        uri = 'http://rs.tdwg.org/dwc/terms/Occurrence'
        namespace = 'http://rs.tdwg.org/dwc/terms/'

# Construct a processing context that wiill read from the example directory and put results in
# to the log. If there are errors, they will be sent to the log output
# Setting the output_dir to None sends any output to the work directory
context = ProcessingContext("ctx", dangling_sink_class=LogSink, input_dir='.', output_dir=None)

# Read data from the input file
input = CsvSource.create("input", "example1.csv", "excel", InputSchema())
# Map the input data onto the output
transform = MapTransform.create("transform", input.output, OccurenceSchema(), {
    'catalogNumber': 'ID',
    'basisOfRecord': MapTransform.constant('HumanObservation'), # Constant value
    'recordedBy': 'OBSERVER',
    'eventDate': MapTransform.dateparse('DATE', '%d/%m/%y'), # Parse a date into this format
    'verbatimEventDate': 'DATE',
    'decimalLatitude': 'LAT', # Auto-converted to a float
    'decimalLongitude': 'LON',
    'verbatimLatitude': 'LAT',
    'verbatimLongitude': 'LON',
    'scientificName': 'SPECIES'
}, fail_on_exception=True)
# Printe the resulting converted file to the output
output = LogSink.create("output", transform.output, reduce=True)
# Create a meta.xml file for the result
meta = MetaFile.create("meta", output)

orchestrator = Orchestrator("orchestrator", [ input, transform, output, meta ])
orchestrator.run(context)