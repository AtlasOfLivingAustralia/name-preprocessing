#  Copyright (c) 2021.  Atlas of Living Australia
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

from ala.transform import SpeciesListSource, CollectorySource, PublisherSource, VernacularListSource
from dwc.meta import MetaFile, EmlFile
from dwc.schema import NomenclaturalCodeMapSchema, VernacularNameSchema
from dwc.transform import DwcTaxonValidate
from processing.orchestrate import Orchestrator
from processing.sink import CsvSink
from processing.source import CsvSource
from processing.transform import LookupTransform, MapTransform


def reader() -> Orchestrator:
    species_list = SpeciesListSource.create('species_list')
    species_metadata = CollectorySource.create('collectory_source')
    nomenclatural_code_map = CsvSource.create('nomenclatural_code_map', 'Nomenclatural_Code_Map.csv', 'ala', NomenclaturalCodeMapSchema())
    default_codes = LookupTransform.create('default_nomenclaural_codes', species_list.output, nomenclatural_code_map.output, 'kingdom', 'kingdom', overwrite=True)
    list_validate = DwcTaxonValidate.create("species_validate", default_codes.output)
    list_output = CsvSink.create("species_output", list_validate.output, "taxon.csv", "excel", reduce=True)
    dwc_meta = MetaFile.create('dwc_meta', list_output)
    publisher = PublisherSource.create("publisher")
    dwc_eml = EmlFile.create('dwc_eml', species_metadata.output, publisher.output)

    orchestrator = Orchestrator("ala",
                                [
                                    species_list,
                                    species_metadata,
                                    nomenclatural_code_map,
                                    default_codes,
                                    list_validate,
                                    list_output,
                                    dwc_meta,
                                    publisher,
                                    dwc_eml
                                ])
    return orchestrator


def vernacular_reader() -> Orchestrator:
    name_file = "vernacularName.csv"
    name_source = CsvSource.create("name_source", name_file, 'excel', VernacularNameSchema())
    name_transform = MapTransform.create("name_transform", name_source.output, VernacularNameSchema(), {
       'datasetID': MapTransform.orDefault(MapTransform.choose('datasetID'), 'datasetID'),
       'status': MapTransform.orDefault(MapTransform.choose('status'), 'vernacularStatus')
    }, auto=True)
    name_output = CsvSink.create("name_output", name_transform.output, "vernacularName.csv", "excel", reduce=True)
    dwc_meta = MetaFile.create("dwc_meta", name_output)
    publisher = PublisherSource.create("publisher")
    dwc_eml = EmlFile.create('dwc_eml', publisher.output, publisher.output)
    orchestrator = Orchestrator("ala_vernacular",
                                [
                                    name_source,
                                    name_transform,
                                    name_output,
                                    dwc_meta,
                                    publisher,
                                    dwc_eml
                                ])
    return orchestrator

def vernacular_list_reader() -> Orchestrator:
    vernacular_list = VernacularListSource.create('vernacular_list', aliases={
        'vernacular name': 'vernacularName',
        'Notes': 'taxonRemarks'
    })
    species_metadata = CollectorySource.create('collectory_source')
    name_transform = MapTransform.create("name_transform", vernacular_list.output, VernacularNameSchema(), {
       'datasetID': MapTransform.orDefault(MapTransform.choose('datasetID'), 'datasetID'),
       'status': MapTransform.orDefault(MapTransform.choose('status'), 'vernacularStatus')
    }, auto=True)
    name_output = CsvSink.create("name_output", name_transform.output, "vernacularName.csv", "excel", reduce=True)
    dwc_meta = MetaFile.create("dwc_meta", name_output)
    publisher = PublisherSource.create("publisher")
    dwc_eml = EmlFile.create('dwc_eml', species_metadata.output, publisher.output)

    orchestrator = Orchestrator("ala_vernacular_list",
                                [
                                    vernacular_list,
                                    species_metadata,
                                    name_transform,
                                    name_output,
                                    dwc_meta,
                                    publisher,
                                    dwc_eml
                                ])
    return orchestrator

