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

from marshmallow import Schema

import location.read
from processing import fields
from processing.node import ProcessingContext
from processing.orchestrate import Orchestrator, Selector
from processing.sink import CsvSink, NullSink
from processing.source import CsvSource


class SourceSchema(Schema):
    id = fields.String()
    job = fields.String()
    dir = fields.String()
    inputDir = fields.String(missing=None)
    configDir = fields.String(missing=None)
    datasetID = fields.String()
    centreLatitude = fields.Float(missing=None)
    centreLongitude = fields.Float(missing=None)
    bbox = fields.String(missing=None)
    currency = fields.String(missing=None)


parser = argparse.ArgumentParser(description='Convert TGN data into primary location data and mappings')
parser.add_argument('-d', '--directory', type=str, help='Base directory', default='.')
parser.add_argument('-i', '--input', type=str,
                    help='Input directory (if relative, then relative to the base directory)', default='input')
parser.add_argument('-o', '--output', type=str,
                    help='Output directory (if relative, then relative to the base directory)', default='output')
parser.add_argument('-w', '--work', type=str, help='Work directory (if relative, then relative to the base directory)',
                    default='work')
parser.add_argument('-c', '--config', type=str,
                    help='Configuration directory (if relative, then relative to the base directory)', default='config')
parser.add_argument('-s', '--sources', type=str, help='File containing the source list', default='location_sources.csv')
parser.add_argument('-v', '--verbose', help='Verbose logging', action='store_true', default=False)
parser.add_argument('--debug', help='Use a small sample dataset', action='store_true', default=False)
parser.add_argument('-x', '--clear', help='Clear the work directory before execution', action='store_true',
                    default=False)
parser.add_argument('--only', type=str,
                    help='Comma separated list of source ids to execute. If absent, all are executed')

args = parser.parse_args()

base_dir = args.directory
config_dirs = [os.path.join(base_dir, args.config)]
work_dir = os.path.join(base_dir, args.work)
input_dir = os.path.join(base_dir, args.input)
output_dir = os.path.join(base_dir, args.output)
log_level = logging.DEBUG if args.verbose else logging.INFO
clear = args.clear
source_file = args.sources
debug = args.debug
if args.only is None:
    source_filter = lambda r: True
else:
    only_ids = set(map(lambda id: id.strip(), args.only.split(',')))
    source_filter = lambda r: r.id in only_ids or r.id == 'default'

sources = CsvSource.create("sources", source_file, "ala", SourceSchema(), predicate=source_filter,
                           fail_on_exception=True)
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
    'default',
    location.read.getty_reader(debug),
    dummy
)

orchestator = Orchestrator('locations', [sources, selector])
defaults = {}

context = ProcessingContext.create('all', dangling_sink_class=CsvSink, config_dirs=config_dirs, input_dir=input_dir,
                                   work_dir=work_dir, output_dir=output_dir, log_level=log_level, clear_work_dir=clear,
                                   defaults=defaults)
orchestator.run(context)
