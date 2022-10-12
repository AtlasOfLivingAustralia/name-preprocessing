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
import re
from math import sin, cos, atan2, sqrt, pi
from re import Pattern
from typing import Set, Dict, Tuple, List

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
    MergeTransform, DeduplicateTransform, ProjectTransform, TrailTransform, VariantTransform, SortTransform, \
    AcceptTransform, ClusterTransform


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
    include = fields.String()
    parent = fields.String()
    geographyType = fields.String(missing=None)

class AreaSchema(Schema):
    """
    Area lookup table
    """
    name = fields.String()
    area = fields.Float(missing=None)
    landArea = fields.Float(missing=None)

class TypeMapSchema(Schema):
    """
    Geography type lookup table
    """
    locationID = fields.String()
    name = fields.String()
    geographyType = fields.String()

class NameSchema(Schema):
    """
    (Invalid) name lookup table
    """
    name = fields.String()

def location_uri(r: Record):
    return 'http://vocab.getty.edu/tgn/' + r.locationID

def parent_location_uri(r: Record):
    if not r.parentLocationID or r.parentLocationID == r.locationID:
        return None
    return 'http://vocab.getty.edu/tgn/' + r.parentLocationID

COMMA_LOCATION = re.compile(r"\s*(.+?)\s*,\s+(.+?)\s*")
def comma_location_1(value: str):
    match = COMMA_LOCATION.fullmatch(value)
    if not match:
        return None
    return match.group(2) + " " + match.group(1)

def comma_location_2(value: str):
    match = COMMA_LOCATION.fullmatch(value)
    if not match:
        return None
    return match.group(1) + " (" + match.group(2) + ")"

OF_LOCATION = re.compile(r"\s*(.+?)\s+(of(?: the)?)\s+(.+?)\s*")
def of_location_1(value: str):
    match = OF_LOCATION.fullmatch(value)
    if not match:
        return None
    return match.group(3) + ", " + match.group(1) + " " + match.group(2)

def of_location_2(value: str):
    match = OF_LOCATION.fullmatch(value)
    if not match:
        return None
    return match.group(3) + " (" + match.group(1) + " " + match.group(2) + ")"

THE_LOCATION = re.compile(r"\s*([Tt]he)\s+(.+?)\s*")
def the_location_1(value: str):
    match = THE_LOCATION.fullmatch(value)
    if not match:
        return None
    return match.group(2) + ", The"

def the_location_2(value: str):
    match = THE_LOCATION.fullmatch(value)
    if not match:
        return None
    return match.group(2)

STATE_LOCATION = re.compile(r"\s*(.+?)\s+(?:[Ss]tate|[Pp]rovince|[Pp]refecture|[Oo]blast|[Dd]istrict|[Tt]erritory|[Rr]egion)\s*")
PROVINCE_LOCATION = re.compile(r"\s*(?:[Ss]tate|[Pp]rovince|[Pp]refecture|[Oo]blast|[Dd]istrict|[Tt]erritory|[Rr]egion)\s+(of|de la|du|de)\s+(.+?)\s*")
def state_location_1(value: str, record: Record):
    match = STATE_LOCATION.fullmatch(value)
    if match:
        return match.group(1)
    match = PROVINCE_LOCATION.fullmatch(value)
    if match:
        return match.group(1)
    return None

ISLAND_LOCATION = re.compile(r"\s*(.+?)\s+(?:[Is]land [Gg]roup|[Ii]lands|[Ii]sland|[Gg]group)\s*")
def island_location_1(value: str, record: Record):
    match = ISLAND_LOCATION.fullmatch(value)
    if match:
        return match.group(1)
    return None


SEA_LOCATION = re.compile(r"\s*(.+?)\s+(?:[Ss]sea|[Oo]cean)\s*")
def sea_location_1(value: str, record: Record):
    match = SEA_LOCATION.fullmatch(value)
    if match:
        return match.group(1)
    return None

def annotate_variant(value: str, record: Record):
    record.data['locationRemarks'] = f"Variant of {record.locality}"

@attr.s
class LocationUsePredicate(Predicate):
    """Select based on geography type and currency"""
    geography_type: Port = attr.ib()
    name_mappings: Port = attr.ib()
    currency: Set[str] = attr.ib()
    exclude: Set[str] = attr.ib()
    bbox: Tuple[float] = attr.ib()
    parent: bool = attr.ib(kw_only=True, default=False)

    def inputs(self) -> Dict[str, Port]:
        inputs = super().inputs()
        if self.geography_type is not None:
            inputs['geographyType'] = self.geography_type
        if self.name_mappings is not None:
            inputs['name_mappings'] = self.name_mappings
        return inputs

    def begin(self, context: ProcessingContext):
        super().begin(context)
        self.type_index = None
        self.name_index = None
        if self.geography_type is not None:
            geographyTypes = context.acquire(self.geography_type)
            self.type_keys = Keys.make_keys(self.geography_type.schema, 'type')
            self.type_index = Index.create(geographyTypes, self.type_keys)
        if self.name_mappings is not None:
            nameMappings = context.acquire(self.name_mappings)
            self.name_keys = Keys.make_keys(self.name_mappings.schema, 'locationID')
            self.name_index = Index.create(nameMappings, self.name_keys, IndexType.FIRST)

    def execute(self, context: ProcessingContext):
        pass

    def test(self, record: Record):
        c = record.currency
        if self.currency is not None and c not in self.currency:
            return False
        id = record.locationID
        if self.exclude is not None and id in self.exclude:
            return False
        locationID = location_uri(record)
        if self.name_index is not None and self.name_index.findByKey(locationID) is not None:
            return True
        if self.type_index is None:
            return True
        type = self.type_index.findByKey(record.type)
        if type is None:
            return False
        include = type.parent if self.parent else type.include
        if include == 'true':
            return True
        if include == 'false':
            return False
        if include == 'bbox' and self.bbox:
            latitude = record.decimalLatitude
            longitude = record.decimalLongitude
            if latitude is None or longitude is None:
                return False
            if latitude < self.bbox[0]:
                return False
            if latitude > self.bbox[2]:
                return False
            if longitude < self.bbox[1]:
                return False
            if longitude > self.bbox[3]:
                return False
            return True
        return True

# Units degrees and km, Haversine formula
RADIUS_OF_EARTH = 6373.0
def distance(r: Record, lat: float, lon: float) -> float:
    rlat = r.decimalLatitude
    rlon = r.decimalLongitude
    if rlat is None or rlon is None:
        return RADIUS_OF_EARTH * pi
    rlat = rlat * pi / 180.0
    rlon = rlon * pi / 180.0
    lat = lat * pi / 180.0
    lon = lon * pi / 180.0
    dlon = rlon - lon
    dlat = rlat - lat
    a = (sin(dlat/2))**2 + cos(rlat) * cos(lat) * (sin(dlon/2))**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    distance = RADIUS_OF_EARTH * c
    return distance

# Signature for clustering elements
LAT_LONG_ROUND = 1.0
def cluster_signature(r: Record) -> Tuple:
    lat = round(r.decimalLatitude * LAT_LONG_ROUND) if r.decimalLatitude else None
    lon = round(r.decimalLongitude * LAT_LONG_ROUND) if r.decimalLongitude else None
    return (r.name, lat, lon, r.parentLocationID)

# Choose the preferred record from a cluster
GEOGRAPHY_ORDER = {
    'continent': 0,
    'ocean': 1,
    'country': 2,
    'sea': 3,
    'stateProvince': 4,
    'waterBody': 4,
    'gulf': 4,
    'bay': 4,
    'county': 5,
    'municipality': 6,
    'other': 7
}
def cluster_selector(sig: Tuple, cluster: List[Record]) -> List[Record]:
    if sig[1] is None or sig[2] is None:
        return cluster
    if len(cluster) < 2:
        return cluster
    cluster.sort(key=lambda r: GEOGRAPHY_ORDER.get(r.geographyType, 100))
    return cluster[0:1]

parser = argparse.ArgumentParser(description='Convert TGN data into primary location data and mappings')
parser.add_argument('-d', '--directory', type=str, help='Base directory', default='.')
parser.add_argument('-i', '--input', type=str, help='Input directory (if relative, then relative to the base directory)', default='input')
parser.add_argument('-o', '--output', type=str, help='Output directory (if relative, then relative to the base directory)', default='output')
parser.add_argument('-w', '--work', type=str, help='Work directory (if relative, then relative to the base directory)', default='work')
parser.add_argument('-c', '--config', type=str, help='Configuration directory (if relative, then relative to the base directory)', default='config')
parser.add_argument('--exclude', type=str, help='Exclude these identifiers from the final output. Separated by commas')
parser.add_argument('--currency', type=str, help='Include only those with specified currencies. Separated by commas', default='Current,Both')
parser.add_argument('--center', type=str, help='The lat,long of the central location for sorting locations. ', default='-25.27,133.78')
parser.add_argument('--bbox', type=str, help='The llat,llong,ulat,rlong of the box of interest for location selection. ', default='-90,71.0,10.0,179.9')
parser.add_argument('-v', '--verbose', help='Verbose logging', action='store_true', default=False)
parser.add_argument('--debug', help='Use a small sample dataset', action='store_true', default=False)

args = parser.parse_args()

base_dir = args.directory
config_dirs = [os.path.join(base_dir, args.config, 'Location'), os.path.join(base_dir, args.config)]
work_dir = os.path.join(base_dir, args.work, 'Location')
input_dir = os.path.join(base_dir, args.input, 'Location')
output_dir = os.path.join(base_dir, args.output, 'Location')
exclude = set(args.exclude.split(',') if args.exclude else [])
currency = set(args.currency.split(','))
center_lat = float(args.center.split(',')[0])
center_long = float(args.center.split(',')[1])
bbox = tuple((float(a) for a in args.bbox.split(','))) if args.bbox else None
log_level = logging.DEBUG if args.verbose else logging.INFO
source_file = 'locations_sample.csv' if args.debug else 'locations.csv'

# Sorter for locations
sorter = lambda r: distance(r, center_lat, center_long)

# Construct a processing context that wiill read from the example directory and put results in
# to the log. If there are errors, they will be sent to the log output
# Setting the output_dir to None sends any output to the work directory
location_schema = LocationSchema()
location_map_schema = LocationMapSchema()
type_map_schema = TypeMapSchema()
name_schema = NameSchema()

# Read data from the input file
type_map = CsvSource.create("types", "Geography_Types.csv", "ala", GeographyTypeMap())
other_mappings = CsvSource.create('other_mappings', 'Other_Location_Mappings.csv', 'ala', location_map_schema)
type_mappings = CsvSource.create('type_mappings', 'Location_Types.csv', 'ala', type_map_schema)
additional_locations = CsvSource.create('additional_locations', 'Additional_Locations.csv', 'ala', location_schema)
invalid_names = CsvSource.create('invalid_names', 'Invalid_Names.csv', 'ala', name_schema)
areas = CsvSource.create('areas', "Areas.csv", 'ala', AreaSchema())
primary_use = LocationUsePredicate('primary_use', type_map.output, other_mappings.output, currency, exclude, bbox)
trailed_use = LocationUsePredicate('trailed_use', type_map.output, other_mappings.output, currency, exclude, bbox, parent=True)
input = CsvSource.create("input", source_file, "excel", InputSchema())
used = FilterTransform.create('used', input.output, primary_use)
trailed = TrailTransform.create('trailed', used.output, input.output, 'locationID', 'parentLocationID', None, trailed_use, fail_on_exception=True)
typed = LookupTransform.create('typed', trailed.output, type_map.output, 'type', 'type', reject=False)
retyped = LookupTransform.create('retyped', typed.output, type_mappings.output, 'locationID', 'locationID', lookup_prefix='retype_', reject=False)
with_area = LookupTransform.create('with_area', retyped.output, areas.output, 'name', 'name', lookup_include=['area'])
clustered = ClusterTransform.create('clustered', with_area.output, cluster_signature, cluster_selector, 'locationID', 'parentLocationID', None, record_rejects=True)
sorted = SortTransform.create('sorted', clustered.output, sorter)
# Map the input data onto the output
transform = MapTransform.create("transform", sorted.output, location_schema, {
    'locationID': location_uri,
    'parentLocationID': parent_location_uri,
    'datasetID': MapTransform.default('datasetID'),
    'geographyType': MapTransform.choose('retype_geographyType', 'geographyType', (lambda r: 'other')),
    'locality': MapTransform.choose('name', 'prferredName'),
    'countryCode': lambda r: r.iso2 if r.iso2 else r.iso3,
    'decimalLatitude': 'decimalLatitude',
    'decimalLongitude': 'decimalLongitude',
    'area': 'area',
    'locationRemarks': 'type'
}, fail_on_exception=True)
merged_locations = MergeTransform.create('merged_locations', transform.output, additional_locations.output)
output = CsvSink.create("output", merged_locations.output, 'Location.csv', 'excel', reduce=True)
# Build the name map
names = MapTransform.create('names', sorted.output, location_map_schema, {
    'locality': 'name',
    'locationID': location_uri,
    'locationRemarks': MapTransform.constant('Base name')
})
preferred_names = MapTransform.create('preferred_names', sorted.output, location_map_schema, {
    'locality': 'preferredName',
    'locationID': location_uri,
    'locationRemarks': lambda r: 'Preferred name for ' + r.name
})
iso_codes_2 = FilterTransform.create('iso_codes_2', sorted.output, lambda r: r.iso2 is not None)
iso_codes_2_mapped = MapTransform.create('iso_codes_2_mapped', iso_codes_2.output, location_map_schema, {
    'locality': 'iso2',
    'locationID': location_uri,
    'locationRemarks': lambda r: 'ISO2 code for ' + r.name
})
iso_codes_3 = FilterTransform.create('iso_codes_3', sorted.output, lambda r: r.iso3 is not None)
iso_codes_3_mapped = MapTransform.create('iso_codes_3_mapped', iso_codes_3.output, location_map_schema, {
    'locality': 'iso3',
    'locationID': location_uri,
    'locationRemarks': lambda r: 'ISO3 code for ' + r.name
})
other_names = DenormaliseTransform.create('other_names', sorted.output, 'otherNames', '|')
other_names_cleaned = LookupTransform.create('other_names_cleaned', other_names.output, preferred_names.output, 'otherNames', 'locality', reject=True, record_unmatched=True, merge=False, lookup_type=IndexType.FIRST)
other_names_mapped = MapTransform.create('other_names_mapped', other_names_cleaned.unmatched, location_map_schema, {
    'locality': 'otherNames',
    'locationID': location_uri,
    'locationRemarks': lambda r: 'Alternative name for ' + r.name
})
variant_source = MergeTransform.create('variant_source', preferred_names.output, names.output, other_names_mapped.output)
names_variant = VariantTransform.create('names_variant', variant_source.output, 'locality', comma_location_1, comma_location_2, of_location_1, of_location_1, the_location_1, the_location_2, state_location_1, island_location_1, sea_location_1, annotate=annotate_variant)
# Put other mappings first so that they override other on IndexType.FIRST lookups
name_map = MergeTransform.create('name_map', other_mappings.output, names.output, preferred_names.output, other_names_mapped.output, iso_codes_2_mapped.output, iso_codes_3_mapped.output, names_variant.output)
name_map_accepted = AcceptTransform.create('name_map_accepted', name_map.output, invalid_names.output, 'locality', 'name', exclude=True, case_insensitive=True)
name_map_unique = DeduplicateTransform.create('name_map_unique', name_map_accepted.output, ('locationID', 'locality'))
name_map_output = CsvSink.create("name_map_output", name_map_unique.output, 'Location_Map.csv', 'excel', reduce=True)
publisher = PublisherSource.create('publisher')
metadata = CollectorySource.create('metadata')
dwc_eml = EmlFile.create('dwc_eml', metadata.output, publisher.output)
meta = MetaFile.create('meta', output, name_map_output)
# Create some analytics on parent/child relationships so that we can detect odd cases
parent_types = LookupTransform.create('parent_types', transform.output, transform.output, 'parentLocationID', 'locationID', lookup_prefix='parent_', lookup_include=['geographyType'])
parent_types_reduced = ProjectTransform.create_from('parent_types_reduced', parent_types.output, 'locationID', 'parentLocationID', 'geographyType', 'parent_geographyType', 'locality')
parent_types_output = CsvSink.create('parent_types_output', parent_types_reduced.output, 'ParentTypes.csv', 'excel', work=True)
retyped_output = CsvSink.create('retyped_output', retyped.output, 'Retyped.csv', 'excel', work=True)

defaults = {
    'datasetID': 'dr19606',
    'geographyType': 'other'
}
context = ProcessingContext.create('all', dangling_sink_class=CsvSink, config_dirs=config_dirs, input_dir=input_dir, work_dir=work_dir, output_dir=output_dir, log_level=log_level, defaults=defaults)
orchestrator = Orchestrator("orchestrator", [
    type_map,
    other_mappings,
    type_mappings,
    additional_locations,
    invalid_names,
    areas,
    primary_use,
    trailed_use,
    input,
    used,
    trailed,
    typed,
    retyped,
    with_area,
    clustered,
    sorted,
    transform,
    merged_locations,
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
    variant_source,
    names_variant,
    name_map,
    name_map_accepted,
    name_map_unique,
    name_map_output,
    publisher,
    metadata,
    dwc_eml,
    meta,
    parent_types,
    parent_types_reduced,
    parent_types_output,
    retyped_output
])
orchestrator.run(context)