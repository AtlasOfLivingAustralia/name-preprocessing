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
import hashlib
import json
import logging
import os
import re

import requests
import attr
from marshmallow import Schema

import processing.fields as fields
from col.schema import ColDistributionSchema
from location.schema import InputSchema
from processing.dataset import Port, Record, Dataset
from processing.node import ProcessingContext
from processing.orchestrate import Orchestrator
from processing.sink import CsvSink
from processing.source import CsvSource
from processing.transform import MapTransform, ClusterTransform, ThroughTransform

logger = logging.getLogger("marineregions")
logger.setLevel(logging.INFO)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logger.addHandler(console)

class RegionsSchema(Schema):
    locationID = fields.String(missing = None, data_key='dwc:locationID')
    locality = fields.String(missing = None)

MR_RECORD = re.compile("mrgid:(\\d+)")

@attr.s
class RetrieveTransform(ThroughTransform):
    """
    Recursively retrieve values and parent values from the marine regions web services.
    """

    cache: str = attr.ib(kw_only=True, default='/data/tmp/marineregions/cache')

    @classmethod
    def create(cls, id: str, input: Port):
        output = Port.port(InputSchema())
        reject = Port.port(input.schema)
        return RetrieveTransform(id, input, output, reject)

    def get_json(self, url: str):
        m = hashlib.sha256()
        m.update(url.encode(encoding = 'UTF-8'))
        store = os.path.join(self.cache, m.hexdigest() + ".json")
        if os.path.exists(store):
            try:
                self.logger.debug("Loading " + url + " at " + store)
                with open(store) as st:
                    r = json.load(st)
            except:
                self.logger.error("Unable to read " + store)
                raise ValueError(store)
        else:
            self.logger.debug("Getting " + url)
            r = requests.get(url).json()
            with open(store, "w") as st:
                json.dump(r, st)
        return r

    def get_mrid(self, mrid: int):
        url = f"https://marineregions.org/rest/getGazetteerRecordByMRGID.json/{mrid}/"
        try:
            return self.get_json(url)
        except Exception as err:
            self.logger.error(f"Unable to retrieve {url}", err)
            return None

    def get_names(self, mrid: int):
        url = f"https://marineregions.org/rest/getGazetteerNamesByMRGID.json/{mrid}/"
        try:
            return self.get_json(url)
        except Exception as err:
            self.logger.error(f"Unable to retrieve {url}", err)
            return None

    def get_parent(self, mrid: int):
        if mrid is None:
            return None
        url = f"https://marineregions.org/rest/getGazetteerRelationsByMRGID.json/{mrid}/?direction=upper&type=partof/"
        try:
            return self.get_json(url)
        except Exception as err:
            self.logger.error(f"Unable to retrieve {url}", err)
            return None

    def retrieve(self, row: Record, result: Dataset, rejected: Dataset, seen: dict):
        mrid = row.locationID
        mrid_match = MR_RECORD.fullmatch(mrid)
        if mrid_match:
            mrid = int(mrid_match.group(1))
            if mrid in seen:
                return seen[mrid]
            other_names = set()
            if row.locality:
                other_names.add(row.locality)
            record = self.get_mrid(mrid)
            collect = True
            loop = set()
            while collect and record is not None:
                id = record.get('MRGID')
                name_record = self.get_names(id)
                if name_record is not None:
                    other_names.update(name_record)
                if record.get('accepted') == id:
                    collect = False
                elif id in loop:
                    collect = False
                else:
                    loop.add(id)
                    record = self.get_mrid(record.get('accepted'))
            if record is None:
                self.count(self.REJECTED_COUNT, row, context)
                rejected.add(row)
                seen[mrid] = None
                return None
            name = record.get('preferredGazetteerName')
            if name in other_names:
                other_names.remove(name)
            data = {
                'locationID': f"mrgid:{mrid}",
                'parentLocationID': None,
                'name': name,
                'preferredName': name,
                'otherNames': '|'.join(other_names) if len(other_names) > 0 else None,
                'iso2': None,
                'iso3': None,
                'currency': 'Current',
                'type': record.get('placeType'),
                'decimalLatitude': record.get('latitude'),
                'decimalLongitude': record.get('longitude'),
            }
            lr = Record(row.line, data)
            seen[mrid] = lr
            parents = self.get_parent(record.get('MRGID'))
            if parents is not None and len(parents) > 0:
                pr = Record(row.line, {
                    'locationID': f"mrgid:{parents[0].get('MRGID')}",
                    'locality': parents[0].get('preferredGazetteerName')
                })
                parent = self.retrieve(pr, result, rejected, seen)
                data['parentLocationID'] = parent.locationID
            self.count(self.ACCEPTED_COUNT, row, context)
            result.add(lr)
            return lr
        self.count(self.REJCTED_COUNT, row, context)
        rejected.add(row)
        return None

    def execute(self, context: ProcessingContext):
        old_li = context.log_interval
        context.log_interval = 100
        base = context.acquire(self.input)
        result = Dataset.for_port(self.output)
        rejected = Dataset.for_port(self.reject)
        seen = dict()
        for row in base.rows:
            self.count(self.PROCESSED_COUNT, row, context)
            self.retrieve(row, result, rejected, seen)
        context.save(self.output, result)
        context.save(self.reject, rejected)
        context.log_interval = old_li

MR_RECORD = re.compile("mrgid:(\\d+)")
def id_records(r: Record) -> bool:
    locationID = r.locationID
    return locationID is not None and MR_RECORD.fullmatch(locationID) is not None

def reader() -> Orchestrator:
    distribution_file = "CoL/Distribution.tsv"
    locations_file = "Location/col_locations.csv"

    col_distribution_schema = ColDistributionSchema()

    distribution_source = CsvSource.create("distribution_source", distribution_file, 'col', col_distribution_schema, no_errors=False, encoding='utf-8-sig', predicate=id_records)
    distribution_keys = MapTransform.create("distribution_keys", distribution_source.output, None, {
        'taxonID': 'taxonID',
        'locationID': 'locationID',
    })
    cluster_keys = ClusterTransform.create('cluster_keys', distribution_keys.output, lambda r: (r.locationID, ), lambda s, c: c[0:1], None, None, None)
    cluster_sink = CsvSink.create('cluster_sink', cluster_keys.output, 'keys.csv', 'ala', work=True)
    key_retrieve = RetrieveTransform.create('key_retrieve', cluster_keys.output)
    retrieve_sink = CsvSink.create('retrieve_sink', key_retrieve.output, locations_file, 'ala')

    orchestrator = Orchestrator("marineregions",
                                [
                                    distribution_source,
                                    distribution_keys,
                                    cluster_keys,
                                    cluster_sink,
                                    key_retrieve,
                                    retrieve_sink
                                ])
    return orchestrator

parser = argparse.ArgumentParser(description='Import natureshare data and convert into a DwC file')
parser.add_argument('-d', '--directory', type=str, help='Base directory', default='.')
parser.add_argument('-i', '--input', type=str, help='Input directory (if relative, then relative to the base directory)', default='input')
parser.add_argument('-o', '--output', type=str, help='Output directory (if relative, then relative to the base directory)', default='output')
parser.add_argument('-w', '--work', type=str, help='Work directory (if relative, then relative to the base directory)', default='work')
parser.add_argument('-c', '--config', type=str, help='Configuration directory (if relative, then relative to the base directory)', default='config')
parser.add_argument('-s', '--sources', type=str, help='File containing the source list', default='sources.csv')
parser.add_argument('-v', '--verbose', help='Verbose logging', action='store_true', default=False)
parser.add_argument('--dump', help='Dump datasets to the ', action='store_true', default=False)
parser.add_argument('-x', '--clear', help='Clear the work directory before execution', action='store_true', default=False)

args = parser.parse_args()

base_dir = args.directory
config_dirs = [os.path.join(base_dir, args.config)]
work_dir = os.path.join(base_dir, args.work)
input_dir = os.path.join(base_dir, args.input)
output_dir = os.path.join(base_dir, args.output)
log_level = logging.DEBUG if args.verbose else logging.INFO
clear = args.clear
source_file = args.sources
dump = args.dump

orchestrator = reader()
defaults = {

}
context = ProcessingContext.create('marineregions', dangling_sink_class=CsvSink, config_dirs=config_dirs, input_dir=input_dir, work_dir=work_dir, output_dir=output_dir, log_level=log_level, clear_work_dir=clear, defaults=defaults, dump=dump)
orchestrator.run(context)