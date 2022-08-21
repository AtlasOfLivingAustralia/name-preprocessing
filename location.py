#  Copyright (c) 2021-2022.  Atlas of Living Australia
#   All Rights Reserved.
#
#   The contents of this file are subject to the Mozilla Public
#   License Version 1.1 (the "License"); you may not use this file
#   except in compliance with the License. You may obtain a copy of
#   the License at http://www.mozilla.org/MPL/
#
#   Software distributed under the License is distributed on an "AS  IS" basis,
#   WITHOUT WARRANTY OF ANY KIND, either express or
#   implied. See the License for the specific language governing
#   rights and limitations under the License.
#
#   The contents of this file are subject to the Mozilla Public
#   License Version 1.1 (the "License"); you may not use this file
#   except in compliance with the License. You may obtain a copy of
#   the License at http://www.mozilla.org/MPL/
#
#   Software distributed under the License is distributed on an "AS  IS" basis,
#   WITHOUT WARRANTY OF ANY KIND, either express or
#   implied. See the License for the specific language governing
#   rights and limitations under the License.
import argparse
import logging
import os
from typing import Set, Dict

import attr
from marshmallow import Schema

from ala.transform import PublisherSource, CollectorySource
from dwc.meta import MetaFile, EmlFile
from dwc.schema import LocationMapSchema, LocationSchema
from processing import fields
from processing.dataset import Port, Keys, Index, Record, IndexType
from processing.node import ProcessingContext
from processing.orchestrate import Orchestrator
from processing.sink import LogSink, CsvSink
from processing.source import CsvSource
from processing.transform import MapTransform, Predicate, LookupTransform, DenormaliseTransform, FilterTransform, \
    MergeTransform, DeduplicateTransform, ProjectTransform, TrailTransform


class InputSchema(Schema):
    """
    Pre-processed locations
    """
    locationID = fields.String()
    parentLocationID = fields.String(missing=None)
    name = fields.String(missing=None)
    preferredName = fields.String(missing=None)
    otherNames = fields.String(missing=None)
    iso2 = fields.String(missing=None)
    iso3 = fields.String(missing=None)
    currency = fields.String(missing=None)
    type = fields.String(missing=None)
    decimalLatitude = fields.Float(missing=None)
    decimalLongitude = fields.Float(missing=None)

    class Meta:
        ordered = True

class GeographyTypeMap(Schema):
    """
    Map TGN geography type onto
    """
    type = fields.String()
    include = fields.Boolean()
    geographyType = fields.String(missing=None)

class AreaSchema(Schema):
    """
    Area lookup table
    """
    name = fields.String()
    area = fields.Float(missing=None)
    landArea = fields.Float(missing=None)


def location_uri(r: Record):
    return 'http://vocab.getty.edu/tgn/' + r.locationID

def parent_location_uri(r: Record):
    if not r.parentLocationID or r.parentLocationID == r.locationID:
        return None
    return 'http://vocab.getty.edu/tgn/' + r.parentLocationID

@attr.s
class LocationUsePredicate(Predicate):
    """Select based on geography type and currency"""
    geography_type: Port = attr.ib()
    name_mappings: Port = attr.ib()
    currency: Set[str] = attr.ib()
    exclude: Set[str] = attr.ib()

    def inputs(self) -> Dict[str, Port]:
        inputs = super().inputs()
        inputs['geographyType'] = self.geography_type
        inputs['name_mappings'] = self.name_mappings
        return inputs

    def begin(self, context: ProcessingContext):
        super().begin(context)
        geographyTypes = context.acquire(self.geography_type)
        self.type_keys = Keys.make_keys(self.geography_type.schema, 'type')
        self.type_index = Index.create(geographyTypes, self.type_keys)
        nameMappings = context.acquire(self.name_mappings)
        self.name_keys = Keys.make_keys(self.name_mappings.schema, 'locationID')
        self.name_index = Index.create(nameMappings, self.name_keys, IndexType.FIRST)

    def execute(self, context: ProcessingContext):
        pass

    def test(self, record: Record):
        c = record.currency
        if c not in self.currency:
            return False
        id = record.locationID
        if id in self.exclude:
            return False
        locationID = location_uri(record)
        if self.name_index.findByKey(locationID) is not None:
            return True
        type = self.type_index.findByKey(record.type)
        if type is None:
            return False
        return type.include

parser = argparse.ArgumentParser(description='Convert TGN data into primary location data and mappings')
parser.add_argument('-d', '--directory', type=str, help='Base directory', default='.')
parser.add_argument('-i', '--input', type=str, help='Input directory (if relative, then relative to the base directory)', default='input')
parser.add_argument('-o', '--output', type=str, help='Output directory (if relative, then relative to the base directory)', default='output')
parser.add_argument('-w', '--work', type=str, help='Work directory (if relative, then relative to the base directory)', default='work')
parser.add_argument('-c', '--config', type=str, help='Configuration directory (if relative, then relative to the base directory)', default='config')
parser.add_argument('--exclude', type=str, help='Exclude these identifiers from the final output. Separated by commas')
parser.add_argument('--currency', type=str, help='Include only those with specified currencies. Separated by commas', default='Current')
parser.add_argument('-v', '--verbose', help='Verbose logging', action='store_true', default=False)

args = parser.parse_args()

base_dir = args.directory
config_dirs = [os.path.join(base_dir, args.config, 'Location'), os.path.join(base_dir, args.config)]
work_dir = os.path.join(base_dir, args.work, 'Location')
input_dir = os.path.join(base_dir, args.input, 'Location')
output_dir = os.path.join(base_dir, args.output, 'Location')
exclude = set(args.exclude.split(',') if args.exclude else [])
currency = set(args.currency.split(','))
log_level = logging.DEBUG if args.verbose else logging.INFO


# Construct a processing context that wiill read from the example directory and put results in
# to the log. If there are errors, they will be sent to the log output
# Setting the output_dir to None sends any output to the work directory
location_map_schema = LocationMapSchema()

# Read data from the input file
type_map = CsvSource.create("types", "Geography_Types.csv", "ala", GeographyTypeMap())
other_mappings = CsvSource.create('other_mappings', 'Other_Location_Mappings.csv', 'ala', location_map_schema)
areas = CsvSource.create('areas', "Areas.csv", 'ala', AreaSchema())
location_use = LocationUsePredicate('location_use', type_map.output, other_mappings.output, currency, exclude)
input = CsvSource.create("input", "locations.csv", "excel", InputSchema())
used = FilterTransform.create('used', input.output, location_use)
trailed = TrailTransform.create('trailed', used.output, input.output, 'locationID', 'parentLocationID', None, location_use)
typed = LookupTransform.create('typed', trailed.output, type_map.output, 'type', 'type', reject=True)
with_area = LookupTransform.create('with_area', typed.output, areas.output, 'name', 'name', lookup_include=['area'])
# Map the input data onto the output
transform = MapTransform.create("transform", with_area.output, LocationSchema(), {
    'locationID': location_uri,
    'parentLocationID': parent_location_uri,
    'datasetID': MapTransform.default('datasetID'),
    'geographyType': MapTransform.orDefault(lambda r: r.geographyType, 'geographyType'),
    'locality': 'name',
    'countryCode': lambda r: r.iso2 if r.iso2 else r.iso3,
    'decimalLatitude': 'decimalLatitude',
    'decimalLongitude': 'decimalLongitude',
    'area': 'area'
}, fail_on_exception=True)
output = CsvSink.create("output", transform.output, 'Location.csv', 'excel', reduce=True)
# Build the name map
names = MapTransform.create('names', typed.output, location_map_schema, {
    'locality': 'name',
    'locationID': location_uri
})
preferred_names = MapTransform.create('preferred_names', typed.output, location_map_schema, {
    'locality': 'preferredName',
    'locationID': location_uri
})
iso_codes_2 = FilterTransform.create('iso_codes_2', typed.output, lambda r: r.iso2 is not None)
iso_codes_2_mapped = MapTransform.create('iso_codes_2_mapped', iso_codes_2.output, location_map_schema, {
    'locality': 'iso2',
    'locationID': location_uri
})
iso_codes_3 = FilterTransform.create('iso_codes_3', typed.output, lambda r: r.iso3 is not None)
iso_codes_3_mapped = MapTransform.create('iso_codes_3_mapped', iso_codes_3.output, location_map_schema, {
    'locality': 'iso3',
    'locationID': location_uri
})
other_names = DenormaliseTransform.create('other_names', typed.output, 'otherNames', '|')
other_names_cleaned = LookupTransform.create('other_names_cleaned', other_names.output, preferred_names.output, 'otherNames', 'locality', reject=True, record_unmatched=True, merge=False, lookup_type=IndexType.FIRST)
other_names_mapped = MapTransform.create('other_names_mapped', other_names_cleaned.unmatched, location_map_schema, {
    'locality': 'otherNames',
    'locationID': location_uri
})
# Put other mappings first so that they override other on IndexType.FIRST lookups
name_map = MergeTransform.create('name_map', other_mappings.output, names.output, preferred_names.output, other_names_mapped.output, iso_codes_2_mapped.output, iso_codes_3_mapped.output)
name_map_unique = DeduplicateTransform.create('name_map_unique', name_map.output, ('locationID', 'locality'))
name_map_output = CsvSink.create("name_map_output", name_map_unique.output, 'Location_Map.csv', 'excel', reduce=True)
publisher = PublisherSource.create('publisher')
metadata = CollectorySource.create('metadata')
dwc_eml = EmlFile.create('dwc_eml', metadata.output, publisher.output)
meta = MetaFile.create('meta', output, name_map_output)
# Create some analytics on parent/child relationships so that we can detect odd cases
parent_types = LookupTransform.create('parent_types', transform.output, transform.output, 'parentLocationID', 'locationID', lookup_prefix='parent_', lookup_include=['geographyType'])
parent_types_reduced = ProjectTransform.create_from('parent_types_reduced', parent_types.output, 'locationID', 'parentLocationID', 'geographyType', 'parent_geographyType', 'locality')
parent_types_output = CsvSink.create('parent_types_output', parent_types_reduced.output, 'ParentTypes.csv', 'excel', work=True)

defaults = {
    'datasetID': 'dr19606',
    'geographyType': 'other'
}
context = ProcessingContext.create('all', dangling_sink_class=CsvSink, config_dirs=config_dirs, input_dir=input_dir, work_dir=work_dir, output_dir=output_dir, log_level=log_level, defaults=defaults)
orchestrator = Orchestrator("orchestrator", [
    type_map,
    other_mappings,
    areas,
    location_use,
    input,
    used,
    trailed,
    typed,
    with_area,
    transform,
    output,
    names,
    preferred_names,
    iso_codes_2,
    iso_codes_2_mapped,
    iso_codes_3,
    iso_codes_3_mapped,
    other_names,
    other_names_cleaned,
    other_names_mapped,
    name_map,
    name_map_unique,
    name_map_output,
    publisher,
    metadata,
    dwc_eml,
    meta,
    parent_types,
    parent_types_reduced,
    parent_types_output
])
orchestrator.run(context)