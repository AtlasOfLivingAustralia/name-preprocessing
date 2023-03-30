#  Copyright (c) 2022.  Atlas of Living Australia
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
import math
import re
from math import cos, atan2, sqrt, pi, sin
from re import Pattern
from typing import Set, Tuple, Dict, List, Callable

import attr

from ala.transform import PublisherSource, CollectorySource
from dwc.meta import EmlFile, MetaFile
from dwc.schema import LocationSchema, LocationMapSchema, LocationIdentifierMapSchema
from location.schema import GeographyTypeMap, InputSchema, TypeMapSchema, NameSchema, AreaSchema, DefaultAreaSchema, \
    LocationWeightSchema
from processing.dataset import Record, Port, Keys, Index, IndexType
from processing.node import ProcessingContext
from processing.orchestrate import Orchestrator
from processing.sink import CsvSink
from processing.source import CsvSource
from processing.transform import Predicate, MapTransform, LookupTransform, DenormaliseTransform, MergeTransform, \
    FilterTransform, TrailTransform, ClusterTransform, SortTransform, VariantTransform, AcceptTransform, \
    DeduplicateTransform, ProjectTransform, ParentLookupTransform

# Expected geography levels
# First level is sort order, other levels are matches
GEOGRAPHY_ORDER = {
    'world': [0],
    'continent': [1],
    'ocean': [1],
    'region': [2, 1, 3, 4],
    'country': [3, 2, 4],
    'sea': [3, 1, 2],
    'stateProvince': [4, 3],
    'waterBody': [4, 1, 2, 3],
    'gulf': [5, 4, 3],
    'bay': [5, 4, 3],
    'islandGroup': [6, 5, 4],
    'island': [7, 6],
    'county': [8],
    'municipality': [9, 8],
    'other': [10, 1, 2, 3, 4, 5, 6, 7, 8, 9]
}


def tgn_location_uri(r: Record):
    return 'http://vocab.getty.edu/tgn/' + r.locationID


def tgn_parent_location_uri(r: Record):
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


STATE_LOCATION = re.compile(
    r"\s*(.+?)\s+(?:[Ss]tate|[Pp]rovince|[Pp]refecture|[Oo]blast|[Dd]istrict|[Tt]erritory|[Rr]egion)\s*")
PROVINCE_LOCATION = re.compile(
    r"\s*(?:[Ss]tate|[Pp]rovince|[Pp]refecture|[Oo]blast|[Dd]istrict|[Tt]erritory|[Rr]egion)\s+(of|de la|du|de)\s+("
    r".+?)\s*")


def state_location_1(value: str, record: Record):
    match = STATE_LOCATION.fullmatch(value)
    if match:
        return match.group(1)
    match = PROVINCE_LOCATION.fullmatch(value)
    if match:
        return match.group(1)
    return None


ISLAND_LOCATION = re.compile(r"\s*(.+?)\s+[Ii]sland\s*")
ISLAND_ABBREV = re.compile(r"\s*(.+?)\s+I\.\s*")
ISLAND_GROUP = re.compile(r"\s*(.+?)\s+(?:[Ii]sland [Gg]roup|[Ii]slands)\s*")
ISLAND_GROUP_ABBREV = re.compile(r"\s*(.+?)\s+Is\.\s*")


def island_location_1(value: str, record: Record):
    match = ISLAND_LOCATION.fullmatch(value)
    if not match:
        match = ISLAND_GROUP.fullmatch(value)
    return None


def island_location_2(value: str, record: Record):
    match = ISLAND_GROUP.fullmatch(value)
    if match:
        return match.group(1) + ' Is.'
    return None


def island_location_3(value: str, record: Record):
    match = ISLAND_LOCATION.fullmatch(value)
    if match:
        return match.group(1) + ' I.'
    return None


def island_location_4(value: str, record: Record):
    match = ISLAND_ABBREV.fullmatch(value)
    if match:
        return match.group(1) + ' Island'
    return None


def island_location_5(value: str, record: Record):
    match = ISLAND_GROUP_ABBREV.fullmatch(value)
    if match:
        return match.group(1) + ' Islands'
    return None


SEA_LOCATION = re.compile(r"\s*(.+?)\s+(?:[Ss]sea|[Oo]cean)\s*")


def sea_location_1(value: str, record: Record):
    match = SEA_LOCATION.fullmatch(value)
    if match:
        return match.group(1)
    return None


def annotate_variant(value: str, record: Record):
    record.data['locationRemarks'] = f"Variant of {record.locality}"


def name_expander(r: Record):
    names = set()
    names.add(r.name)
    if r.preferredName:
        names.add(r.preferredName)
    if r.otherNames:
        names.update(r.otherNames.split('|'))
    if r.iso2:
        names.add(r.iso2)
    if r.iso3:
        names.add(r.iso3)
    return list(names)


@attr.s
class LocationUsePredicate(Predicate):
    """Select based on geography type and currency"""
    geography_type: Port = attr.ib()
    required: Port = attr.ib()
    names: Port = attr.ib()
    exclude: Port = attr.ib()
    parent: bool = attr.ib()
    location_uri: Callable = attr.ib()
    parent_location_uri: Callable = attr.ib()

    def inputs(self) -> Dict[str, Port]:
        inputs = super().inputs()
        if self.geography_type is not None:
            inputs['geographyType'] = self.geography_type
        if self.required is not None:
            inputs['required'] = self.required
        if self.names is not None:
            inputs['names'] = self.names
        if self.exclude is not None:
            inputs['exclude'] = self.exclude
        return inputs

    def begin(self, context: ProcessingContext):
        super().begin(context)
        self.type_index = None
        self.name_index = None
        self.exclude_index = None
        if self.geography_type is not None:
            geography_types = context.acquire(self.geography_type)
            self.type_keys = Keys.make_keys(self.geography_type.schema, 'type')
            self.type_index = Index.create(geography_types, self.type_keys)
        if self.required is not None:
            name_mappings = context.acquire(self.required)
            self.required_keys = Keys.make_keys(self.required.schema, 'locationID')
            self.required_index = Index.create(name_mappings, self.required_keys, IndexType.FIRST)
        if self.names is not None:
            extra_names = context.acquire(self.names)
            self.name_keys = Keys.make_keys(self.names.schema, 'name')
            self.name_index = Index.create(extra_names, self.name_keys, IndexType.MULTI)
        if self.exclude is not None:
            exclusions = context.acquire(self.exclude)
            self.exclude_keys = Keys.make_keys(self.exclude.schema, 'locationID')
            self.exclude_index = Index.create(exclusions, self.exclude_keys)
        self.currency = set(context.get_default('currency', 'Current').split(','))
        bbox = context.get_default('bbox', None)
        if bbox:
            bbox = bbox.split('|')
            self.bbox = [tuple((float(a) for a in bb.split(','))) for bb in bbox]
        else:
            self.bbox = None

    def execute(self, context: ProcessingContext):
        pass

    def same_location(self, record: Record, required: Record):
        """Check to see if the record/required value are more-or-less in the same spot"""
        req_dec_lat = required.decimalLatitude
        req_dec_lon = required.decimalLongitude
        rec_dec_lat = record.decimalLatitude
        rec_dec_lon = record.decimalLongitude
        if rec_dec_lat is None and req_dec_lat is not None:
            return False
        if rec_dec_lon is None and req_dec_lon is not None:
            return False
        if req_dec_lat is not None and abs(rec_dec_lat - req_dec_lat) > 10.0:  # Sameish region
            return False
        if req_dec_lon is not None and abs(rec_dec_lon - req_dec_lon) > 10.0:
            return False
        return True

    def same_geography_type(self, record: Record, required: Record):
        rec_type = record.geographyType
        req_type = required.geographyType
        if req_type is None:
            return True
        if rec_type is None:
            return False
        rec_val = GEOGRAPHY_ORDER.get(rec_type, None)
        if rec_val is None:
            return False
        rec_val = set(rec_val)
        req_val = GEOGRAPHY_ORDER.get(req_type, None)
        if req_val is None:
            return False
        req_val = set(req_val)
        return not rec_val.isdisjoint(req_val)

    def test(self, record: Record):
        c = record.currency
        if self.currency is not None and c not in self.currency:
            return False
        id = record.locationID
        if self.exclude_index is not None and self.exclude_index.findByKey(id) is not None:
            return False
        location_id = self.location_uri(record)
        if self.required_index is not None:
            required = self.required_index.findByKey(location_id)
            if required is not None:
                return True
        if self.name_index is not None:
            names = name_expander(record)
            for name in names:
                required = self.name_index.findByKey(name)
                if required is not None:
                    for req in required:
                        if self.same_location(record, req) and self.same_geography_type(record, req):
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
            for bbox in self.bbox:
                within = True
                if latitude < bbox[0]:
                    within = False
                if latitude > bbox[2]:
                    within = False
                if longitude < bbox[1]:
                    within = False
                if longitude > bbox[3]:
                    within = False
                if within:
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
    a = (sin(dlat / 2)) ** 2 + cos(rlat) * cos(lat) * (sin(dlon / 2)) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = RADIUS_OF_EARTH * c
    return distance


# Signature for clustering elements
LAT_LONG_ROUND = 1.0


def cluster_signature(r: Record) -> Tuple:
    lat = round(r.decimalLatitude * LAT_LONG_ROUND) if r.decimalLatitude else None
    lon = round(r.decimalLongitude * LAT_LONG_ROUND) if r.decimalLongitude else None
    return (r.name, lat, lon, r.parentLocationID)


def cluster_selector(sig: Tuple, cluster: List[Record]) -> List[Record]:
    if sig[1] is None or sig[2] is None:
        return cluster
    if len(cluster) < 2:
        return cluster
    cluster.sort(key=lambda r: GEOGRAPHY_ORDER.get(r.geographyType, 100))
    return cluster[0:1]


# Sorter for locations, based on a central position
def sorter(r: Record, c: ProcessingContext):
    clat = c.get_default('centreLatitude', 0.0)
    clon = c.get_default('centreLongitude', 0.0)
    if clat == 0.0 or clon == 0.0:
        return 0
    return distance(r, clat, clon)


# Default weight for names, based on area and central position
def location_weight(r: Record, c: ProcessingContext):
    clat = c.get_default('centreLatitude', 0.0)
    clon = c.get_default('centreLongitude', 0.0)
    if clat == 0.0 or clon == 0.0:
        d = 1.0
    else:
        d = max(1.0, distance(r, clat, clon))
    area = r.area
    if not area:
        area = 1.0
    return round(area / (math.log(d) + 1.0))


# Name sort weight
def name_sort_weight(r: Record):
    weight = r.weight
    return weight if weight else 0.0


ISO_NUMBER = re.compile(r'([A-Z][A-Z][A-Z]?)\d+')


# Remove iso codes and weird variants in other names
def non_iso_other_name(r: Record):
    name: str = r.otherNames
    if name is None or len(name) == 0:
        return False
    name = name.strip().upper()
    iso2 = r.iso2
    if iso2 is not None:
        iso2 = iso2.upper()
        if name == iso2:
            return False
    iso3 = r.iso3
    if iso3 is not None:
        iso3 = iso3.upper()
        if name == iso3:
            return False
    match = ISO_NUMBER.fullmatch(name)
    if match:
        return False
    return True


def generic_reader(source: str, location_uri: Callable, parent_location_uri: Callable) -> Orchestrator:
    input_schema = InputSchema()
    location_schema = LocationSchema()
    location_map_schema = LocationMapSchema()
    type_map_schema = TypeMapSchema()
    name_schema = NameSchema()
    weight_schema = LocationWeightSchema()
    identifier_map_schema = LocationIdentifierMapSchema()

    with Orchestrator("tgn") as orchestrator:
        # Read data from the input file
        type_map = CsvSource.create("types", "Geography_Types.csv", "ala", GeographyTypeMap())
        other_mappings = CsvSource.create('other_mappings', 'Other_Location_Mappings.csv', 'ala', location_map_schema)
        type_mappings = CsvSource.create('type_mappings', 'Location_Types.csv', 'ala', type_map_schema)
        additional_locations = CsvSource.create('additional_locations', 'Additional_Locations.csv', 'ala',
                                                location_schema)
        exclusions = CsvSource.create('exclusions', 'Exclude_Locations.csv', 'ala', location_map_schema)
        additional_required = ProjectTransform.create('required', additional_locations.output, location_map_schema)
        invalid_names = CsvSource.create('invalid_names', 'Invalid_Names.csv', 'ala', name_schema)
        location_weights = CsvSource.create('location_weights', 'Location_Weights.csv', 'ala', weight_schema)
        required_identifier_map = CsvSource.create('required_identifier_map', 'Required_Identifier_Map.csv', 'ala',
                                                   identifier_map_schema)
        areas = CsvSource.create('areas', "Areas.csv", 'ala', AreaSchema())
        default_areas = CsvSource.create('default_areas', 'Default_Areas.csv', 'ala', DefaultAreaSchema())
        input = CsvSource.create("input", source, "excel", input_schema)
        typed = LookupTransform.create('typed', input.output, type_map.output, 'type', 'type', reject=False)
        retyped = LookupTransform.create('retyped', typed.output, type_mappings.output, 'locationID', 'locationID',
                                         lookup_prefix='retype_', reject=False)

        # Get TDWG and marine region names for inclusion
        tdwg = CsvSource.create('tdwg', 'TDWG.csv', 'ala', input_schema)
        tdwg_typed = LookupTransform.create('tdwg_typed', tdwg.output, type_map.output, 'type', 'type', reject=False)
        marineregions = CsvSource.create('marineregions', 'marineregions.csv', 'ala', input_schema)
        marineregions_typed = LookupTransform.create('marineregions_typed', marineregions.output, type_map.output,
                                                     'type', 'type', reject=False)

        required = MergeTransform.create('required', other_mappings.output, additional_required.output,
                                         required_identifier_map.output)
        required_names = MergeTransform.create('required_names', tdwg_typed.output, marineregions_typed.output)

        primary_use = LocationUsePredicate('primary_use', type_map.output, required.output, required_names.output,
                                           exclusions.output, False, location_uri, parent_location_uri)
        trailed_use = LocationUsePredicate('trailed_use', type_map.output, required.output, required_names.output,
                                           exclusions.output, True, location_uri, parent_location_uri)
        used = FilterTransform.create('used', retyped.output, primary_use)
        trailed = TrailTransform.create('trailed', used.output, retyped.output, 'locationID', 'parentLocationID', None,
                                        trailed_use, fail_on_exception=True)
        with_default_area = LookupTransform.create('with_default_area', trailed.output, default_areas.output,
                                                   'geographyType', 'geographyType', lookup_include=['area'])
        with_area = LookupTransform.create('with_area', with_default_area.output, areas.output,
                                           'name', 'name', lookup_include=['area'], overwrite=True)
        with_default_weight = MapTransform.create('with_default_weight', with_area.output, None, {
            'weight': location_weight
        }, auto=True)
        with_weight = LookupTransform.create('with_weight', with_default_weight.output, location_weights.output,
                                             'locationID', 'locationID', lookup_include=['weight'], overwrite=True)
        clustered = ClusterTransform.create('clustered', with_weight.output, cluster_signature, cluster_selector,
                                            'locationID', 'parentLocationID', None, record_rejects=True)
        sorted = SortTransform.create('sorted', clustered.output, sorter)
        # Map the input data onto the output
        transform = MapTransform.create("transform", sorted.output, location_schema, {
            'locationID': location_uri,
            'parentLocationID': parent_location_uri,
            'datasetID': MapTransform.default('datasetID'),
            'geographyType': MapTransform.choose('retype_geographyType', 'geographyType', (lambda r: 'other')),
            'locality': MapTransform.choose('name', 'preferredName'),
            'countryCode': lambda r: r.iso2 if r.iso2 else r.iso3,
            'decimalLatitude': 'decimalLatitude',
            'decimalLongitude': 'decimalLongitude',
            'area': 'area',
            'weight': 'weight',
            'locationRemarks': 'type'
        }, fail_on_exception=True)
        merged_locations = MergeTransform.create('merged_locations', transform.output, additional_locations.output)
        output = CsvSink.create("output", merged_locations.output, 'Location.csv', 'excel', reduce=True)
        # Build the name map
        base_names = MapTransform.create('names', sorted.output, location_map_schema, {
            'locality': 'name',
            'locationID': location_uri,
            'locationRemarks': MapTransform.constant('Base name')
        })
        additional_names = MapTransform.create('additional_names', additional_locations.output, location_map_schema, {
            'locality': 'locality',
            'locationID': 'locationID',
            'locationRemarks': MapTransform.constant('Base name')
        })
        names = MergeTransform.create('names', base_names.output, additional_names.output)
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
        other_names = DenormaliseTransform.delimiter('other_names', sorted.output, 'otherNames', '|')
        other_names_noniso = FilterTransform.create('other_names_noniso', other_names.output, non_iso_other_name)
        other_names_cleaned = LookupTransform.create('other_names_cleaned', other_names_noniso.output,
                                                     preferred_names.output, 'otherNames', 'locality', reject=True,
                                                     record_unmatched=True, merge=False, lookup_type=IndexType.FIRST)
        other_names_mapped = MapTransform.create('other_names_mapped', other_names_cleaned.unmatched,
                                                 location_map_schema, {
                                                     'locality': 'otherNames',
                                                     'locationID': location_uri,
                                                     'locationRemarks': lambda r: 'Alternative name for ' + r.name
                                                 })
        variant_source = MergeTransform.create('variant_source', preferred_names.output, names.output,
                                               other_names_mapped.output)
        names_variant = VariantTransform.create('names_variant', variant_source.output, 'locality', comma_location_1,
                                                comma_location_2, of_location_1, of_location_1, the_location_1,
                                                the_location_2, state_location_1,
                                                island_location_1, island_location_2, island_location_3,
                                                island_location_4, island_location_5,
                                                sea_location_1,
                                                annotate=annotate_variant)
        # Put other mappings first so that they override other on IndexType.FIRST lookups
        name_map = MergeTransform.create('name_map', other_mappings.output, names.output, preferred_names.output,
                                         other_names_mapped.output, iso_codes_2_mapped.output,
                                         iso_codes_3_mapped.output, names_variant.output)
        name_map_accepted = AcceptTransform.create('name_map_accepted', name_map.output, invalid_names.output,
                                                   'locality', 'name', exclude=True, case_insensitive=True)
        name_map_unique = DeduplicateTransform.create('name_map_unique', name_map_accepted.output,
                                                      ('locationID', 'locality'))
        name_map_output = CsvSink.create("name_map_output", name_map_unique.output, 'Location_Names.csv', 'excel',
                                         reduce=True)

        # Make a lookup name map that only uses the most significant version of the name
        # Significant is a moving target
        name_map_with_weight = LookupTransform.create('name_map_with_weight', name_map_unique.output,
                                                      merged_locations.output, 'locationID', 'locationID')
        name_map_sorted = SortTransform.create('name_map_sorted', name_map_with_weight.output,
                                               name_sort_weight, reverse=True)
        name_map_lookup = DeduplicateTransform.create('name_map_lookup', name_map_sorted.output, 'locality')
        name_map_lookup_reduced = ProjectTransform.create('name_map_lookup_reduced', name_map_lookup.output,
                                                          LocationMapSchema())
        name_map_lookup_output = CsvSink.create('name_map_lookup_output', name_map_lookup_reduced.output,
                                                'Location_Lookup.csv', 'excel')

        # Map identifiers for marine regions/tdwg
        id_provided = AcceptTransform.create('id_provided', required_names.output, required_identifier_map.output,
                                             'locationID', 'identifier', exclude=True)
        id_matches = ParentLookupTransform.create('id_matches', id_provided.output, name_map_unique.output, 'name',
                                                  'locality', 'locationID', 'parentLocationID', lookup_prefix='m_',
                                                  lookup_type=IndexType.FIRST)
        id_map = MapTransform.create("id_map", id_matches.output, identifier_map_schema, {
            'locationID': 'm_locationID',
            'identifier': 'locationID',
            'locality': 'name',
            'mappedLocality': 'm_locality'
        }, fail_on_exception=True)
        id_map_all = MergeTransform.create('id_map_all', required_identifier_map.output, id_map.output)
        id_map_output = CsvSink.create('id_map_output', id_map_all.output, 'Location_Identifiers.csv', 'excel',
                                       reduce=True)

        publisher = PublisherSource.create('publisher')
        metadata = CollectorySource.create('metadata')
        EmlFile.create('dwc_eml', metadata.output, publisher.output)
        MetaFile.create('meta', output, name_map_output, id_map_output)
        # Create some analytics on parent/child relationships so that we can detect odd cases
        parent_types = LookupTransform.create('parent_types', transform.output, transform.output, 'parentLocationID',
                                              'locationID', lookup_prefix='parent_', lookup_include=['geographyType'])
        parent_types_reduced = ProjectTransform.create_from('parent_types_reduced', parent_types.output, 'locationID',
                                                            'parentLocationID', 'geographyType', 'parent_geographyType',
                                                            'locality')
        CsvSink.create('parent_types_output', parent_types_reduced.output, 'ParentTypes.csv',
                       'excel', work=True)
        CsvSink.create('retyped_output', retyped.output, 'Retyped.csv', 'excel', work=True)
    return orchestrator


def getty_reader(debug: bool) -> Orchestrator:
    """
    Orchestrator for Getty Thesaurus of Geographic Names (TGN)
    """
    return generic_reader('getty_sample.csv' if debug else 'getty.csv', tgn_location_uri, tgn_parent_location_uri)
