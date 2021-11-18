from ala.transform import PublisherSource, CollectorySource
from dwc.meta import MetaFile, EmlFile
from dwc.schema import TaxonSchema, VernacularSchema, NomenclaturalCodeMapSchema
from nzor.schema import NzorTaxonSchema, NzorRankMapSchema, NzorVernacularSchema, NzorLanguageMapSchema
from processing.dataset import IndexType
from processing.orchestrate import Orchestrator
from processing.sink import CsvSink
from processing.source import CsvSource
from processing.transform import MapTransform, normalise_spaces, LookupTransform, choose


def clean_author(name: str, author: str):
    index = name.find(author)
    if index > 0:
        name = name[:index] + ' ' + name[index + len(author):]
    return name

def clean_scientific(name: str, author: str, year: str):
    if author is None:
        return name
    if year is not None:
        name = clean_author(name, '(' + author + ', ' + year + ')')
        name = clean_author(name, ' ' + author + ', ' + year)
    name = clean_author(name, '(' + author + ')')
    name = clean_author(name, ' ' + author)
    return normalise_spaces(name)

def clean_uninomial(name: str):
    if name is None:
        return None
    name = name.strip();
    if len(name) == 0:
        return None
    try:
        sp = name.index(' ')
        return name[:sp]
    except ValueError:
        return name


def reader() -> Orchestrator:
    taxon_file = "taxon.txt"
    vernacular_file = "vernacularname.txt"
    rank_file = "NZOR_Rank_Map.csv"
    language_file = "Language_Map.csv"
    nomenclatural_code_file = "Nomenclatural_Code_Map.csv"
    nzor_taxon_schema = NzorTaxonSchema()
    nzor_vernacular_schema = NzorVernacularSchema()
    nzor_rank_map_schema = NzorRankMapSchema()
    nzor_language_map_schema = NzorLanguageMapSchema()
    nomenclatural_code_schema = NomenclaturalCodeMapSchema()

    rank_map = CsvSource.create("rank_map", rank_file, "ala", nzor_rank_map_schema)
    language_map = CsvSource.create("language_map", language_file, "ala", nzor_language_map_schema)
    nomenclatural_code_map = CsvSource.create("nomenclatual_code_map", nomenclatural_code_file, "ala", nomenclatural_code_schema)
    taxon_source = CsvSource.create("taxon_source", taxon_file, 'excel-tab', nzor_taxon_schema, no_errors=False)
    taxon_coded = LookupTransform.create("taxon_coded", taxon_source.output, nomenclatural_code_map.output, 'kingdom', 'kingdom', lookup_map= { 'nomenclaturalCode': 'kingdomNomenclaturalCode' })
    taxon_recoded = MapTransform.create("taxon_recoded", taxon_coded.output, nzor_taxon_schema, {
       'nomenclaturalCode': (lambda r: choose(r.kingdomNomenclaturalCode, r.nomenclaturalCode))
    }, auto=True)
    taxon_ranked = LookupTransform.create("taxon_ranked",taxon_recoded.output, rank_map.output, ('taxonRank', 'nomenclaturalCode'), ('rank', 'nomenclaturalCode'), lookup_map={ 'taxonRank': 'taxonRank1'})
    taxon_ranked_2 = LookupTransform.create("taxon_ranked_2", taxon_ranked.output, rank_map.output, 'taxonRank', 'rank', lookup_type=IndexType.FIRST, lookup_map={ 'taxonRank': 'taxonRank2'})
    taxon_rewrite = MapTransform.create("taxon_rewrite", taxon_ranked_2.output, TaxonSchema(), {
       'datasetID': MapTransform.default('datasetID'),
       'parentNameUsageID': (lambda r: r.parentNameUsageID if r.taxonID == r.acceptedNameUsageID else None),
       'acceptedNameUsageID': (lambda r: r.acceptedNameUsageID if r.taxonID != r.acceptedNameUsageID else None),
       'taxonomicStatus': (lambda r: choose(r.taxonomicStatus, 'accepted' if r.taxonID == r.acceptedNameUsageID else 'synonym')),
       'taxonRank': (lambda r: choose(r.taxonRank1, r.taxonRank2, r.taxonRank)),
       'scientificName': (lambda r: clean_scientific(r.scientificName, r.scientificNameAuthorship, r.namePublishedInYear)),
       'nameComplete': 'scientificName',
       'genus': (lambda r: clean_uninomial(r.genus)),
       'family': (lambda r: clean_uninomial(r.family)),
       'order': (lambda r: clean_uninomial(r.order)),
       'class_': (lambda r: clean_uninomial(r.class_)),
       'phylum': (lambda r: clean_uninomial(r.phylum)),
       'kingdom': (lambda r: clean_uninomial(r.kingdom)),
       'source': 'scientificNameID'
    }, auto=True)
    taxon_output = CsvSink.create("taxon_output", taxon_rewrite.output, "taxon.csv", "excel", reduce=True)
    vernacular_source = CsvSource.create("vernacular_source", vernacular_file, 'excel-tab', nzor_vernacular_schema, no_errors=False)
    vernacular_mapped = LookupTransform.create('vernacular_mapped', vernacular_source.output, language_map.output, 'language', 'Name')
    vernacular_linked = LookupTransform.create('vernacular_linked', vernacular_mapped.output, taxon_rewrite.output, 'id', 'taxonID', lookup_include=['scientificNameID'], reject=True)
    vernacular_rewrite = MapTransform.create('vernacular_rewrite', vernacular_linked.output, VernacularSchema(), {
       'datasetID': MapTransform.default('datasetID'),
       'taxonID': 'id',
       'nameID': (lambda r: 'NZOR_V_' + str(r.line)),
       'status': (lambda r: 'preferred' if r.isPreferredName else 'common'),
       'language': (lambda r, c: choose(r.Code, r.language, c.get_default('language'))),
       'countryCode': MapTransform.default('countryCode'),
       'nameAccordingTo': 'source',
       'source': 'scientificNameID'
    }, auto=True)
    vernacular_output = CsvSink.create("vernacular_output", vernacular_rewrite.output, "vernacularName.csv", "excel", reduce=True)
    dwc_meta = MetaFile.create('dwc_meta', taxon_output, vernacular_output)
    publisher = PublisherSource.create('publisher')
    metadata = CollectorySource.create('metadata')
    dwc_eml = EmlFile.create('dwc_eml', metadata.output, publisher.output)

    orchestrator = Orchestrator("nzor",
                                [
                                    rank_map,
                                    language_map,
                                    nomenclatural_code_map,
                                    taxon_source,
                                    taxon_coded,
                                    taxon_recoded,
                                    taxon_ranked,
                                    taxon_ranked_2,
                                    taxon_rewrite,
                                    taxon_output,
                                    vernacular_source,
                                    vernacular_mapped,
                                    vernacular_linked,
                                    vernacular_rewrite,
                                    vernacular_output,
                                    dwc_meta,
                                    metadata,
                                    publisher,
                                    dwc_eml
                                ])
    return orchestrator
