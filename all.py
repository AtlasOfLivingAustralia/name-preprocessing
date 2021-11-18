import argparse
import logging
import os.path

from marshmallow import Schema

import afd.read
import ala.read
import ausfungi.read
import caab.read
import col.read
import nsl.read
import nzor.read
from processing import fields
from processing.node import ProcessingContext
from processing.orchestrate import Selector, Orchestrator
from processing.sink import NullSink, CsvSink
from processing.source import CsvSource


class SourceSchema(Schema):
    id = fields.String()
    job = fields.String()
    dir = fields.String()
    inputDir = fields.String(missing=None)
    configDir = fields.String(missing=None)
    datasetID = fields.String()
    nomenclaturalCode = fields.String(missing=None)
    defaultOrganisation = fields.String(missing=None)
    geographicCoverage = fields.String(missing=None)
    taxonomicCoverage = fields.String(missing=None)
    sourceUrl = fields.String(missing=None)
    vernacularStatus = fields.String(missing=None)

parser = argparse.ArgumentParser(description='Import natureshare data and convert into a DwC file')
parser.add_argument('-d', '--directory', type=str, help='Base directory', default='.')
parser.add_argument('-i', '--input', type=str, help='Input directory (if relative, then relative to the base directory)', default='input')
parser.add_argument('-o', '--output', type=str, help='Output directory (if relative, then relative to the base directory)', default='output')
parser.add_argument('-w', '--work', type=str, help='Work directory (if relative, then relative to the base directory)', default='work')
parser.add_argument('-c', '--config', type=str, help='Configuration directory (if relative, then relative to the base directory)', default='config')
parser.add_argument('-s', '--sources', type=str, help='File containing the source list', default='sources.csv')
parser.add_argument('-v', '--verbose', help='Verbose logging', action='store_true', default=False)
parser.add_argument('-x', '--clear', help='Clear the work directory before execution', action='store_true', default=False)
parser.add_argument('--only', type=str, help='Comma separated list of source ids to execute. If absent, all are executed')

args = parser.parse_args()

base_dir = args.directory
config_dirs = [os.path.join(base_dir, args.config)]
work_dir = os.path.join(base_dir, args.work)
input_dir = os.path.join(base_dir, args.input)
output_dir = os.path.join(base_dir, args.output)
log_level = logging.DEBUG if args.verbose else logging.INFO
clear = args.clear
source_file = args.sources
if args.only is None:
    source_filter = lambda r: True
else:
    only_ids = set(map(lambda id: id.strip(), args.only.split(',')))
    source_filter = lambda r: r.id in only_ids

sources = CsvSource.create("sources", source_file, "ala", SourceSchema(), predicate=source_filter)
dummy = NullSink.create("dummy")
selector = Selector.create(
    "selector",
    sources.output,
    'job',
    'dir',
    'inputDir',
    None,
    'configDir',
    None,
    afd.read.reader(),
    ala.read.reader(),
    ala.read.vernacular_reader(),
    ala.read.vernacular_list_reader(),
    ausfungi.read.reader(),
    caab.read.reader(),
    col.read.reader(),
    nsl.read.reader(),
    nsl.read.additional_reader(),
    nzor.read.reader(),
    dummy
)

orchestator = Orchestrator('all', [sources, selector])
defaults = {
    'language': 'en',
    'countryCode': 'AU',
    'status': 'common',
    'isPreferredName': False,
    'locationID': 'AUS'
}

context = ProcessingContext.create('all', dangling_sink_class=CsvSink, config_dirs=config_dirs, input_dir=input_dir, work_dir=work_dir, output_dir=output_dir, log_level=log_level, clear_work_dir=clear, defaults=defaults)
orchestator.run(context)