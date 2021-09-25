from ala.transform import SpeciesListSource, CollectorySource, PublisherSource
from dwc.meta import MetaFile, EmlFile
from dwc.schema import NomenclaturalCodeMapSchema, VernacularNameSchema
from dwc.transform import DwcTaxonValidate
from processing.orchestrate import Orchestrator
from processing.sink import CsvSink
from processing.source import CsvSource
from processing.transform import LookupTransform


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
