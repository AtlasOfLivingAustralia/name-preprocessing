from marshmallow import Schema

import processing.fields as fields


class TaxonSchema(Schema):
    taxonID = fields.String(metadata={ 'export': True })
    parentNameUsageID = fields.String(missing=None, metadata={ 'export': True })
    parentNameUsage = fields.String(missing=None)
    acceptedNameUsageID = fields.String(missing=None, metadata={ 'export': True })
    acceptedNameUsage = fields.String(missing=None)
    datasetID = fields.String(missing=None, metadata={ 'export': True })
    nomenclaturalCode = fields.String(missing=None, metadata={ 'export': True })
    scientificName = fields.String(metadata={ 'export': True })
    scientificNameAuthorship = fields.String(missing=None, metadata={ 'export': True })
    taxonRank = fields.String(metadata={ 'export': True })
    taxonConceptID = fields.String(missing=None)
    scientificNameID = fields.String(missing=None)
    taxonomicStatus = fields.String(metadata={ 'export': True })
    nomenclaturalStatus = fields.String(missing=None)
    kingdom = fields.String(missing=None)
    phylum = fields.String(missing=None)
    subphylum = fields.String(missing=None, uri='http://ala.org.au/terms/1.0/subphylum')
    class_ = fields.String(missing=None, data_key='class', uri='http://rs.tdwg.org/dwc/terms/class')
    subclass = fields.String(missing=None, uri='http://ala.org.au/terms/1.0/subclass')
    order = fields.String(missing=None)
    suborder = fields.String(missing=None, uri='http://ala.org.au/terms/1.0/suborder')
    infraorder = fields.String(missing=None, uri='http://ala.org.au/terms/1.0/infraorder')
    family = fields.String(missing=None)
    genus = fields.String(missing=None)
    subgenus = fields.String(missing=None)
    specificEpithet = fields.String(missing=None)
    infraspecificEpithet = fields.String(missing=None)
    establishmentMeans = fields.String(missing=None)
    nameAccordingToID = fields.String(missing=None)
    nameAccordingTo = fields.String(missing=None)
    namePublishedInID = fields.String(missing=None)
    namePublishedIn = fields.String(missing=None)
    namePublishedInYear = fields.String(missing=None)
    nameComplete = fields.String(missing=None, uri='http://ala.org.au/terms/1.0/nameComplete')
    nameFormatted = fields.String(missing=None, uri='http://ala.org.au/terms/1.0/nameFormatted')
    taxonRemarks = fields.String(missing=None)
    provenance = fields.String(missing=None, uri='http://purl.org/dc/terms/provenance')
    source = fields.String(missing=None, uri='http://purl.org/dc/terms/source')

    class Meta:
        ordered = True
        uri = 'Taxon'
        namespace = 'http://rs.tdwg.org/dwc/terms/'


class VernacularSchema(Schema):
    taxonID = fields.String(metadata={ 'export': True })
    nameID = fields.String(missing=None, uri='http://ala.org.au/terms/1.0/nameID')
    datasetID = fields.String(missing=None, metadata={ 'export': True })
    vernacularName = fields.String(metadata={ 'export': True })
    status = fields.String(missing=None, uri='http://ala.org.au/terms/1.0/status')
    language = fields.String(missing=None, uri='http://purl.org/dc/terms/language')
    temporal = fields.String(missing=None, uri='http://purl.org/dc/terms/temporal')
    locationID = fields.String(missing=None)
    locality = fields.String(missing=None)
    countryCode = fields.String(missing=None)
    sex = fields.String(missing=None)
    lifeStage = fields.String(missing=None)
    isPlural = fields.Boolean(missing=None, uri='http://rs.gbif.org/terms/1.0/isPlural')
    isPreferredName = fields.Boolean(missing=None, uri='http://rs.gbif.org/terms/1.0/isPreferredName')
    organismPart = fields.String(missing=None, uri='http://rs.gbif.org/terms/1.0/organismPart')
    labels = fields.String(missing=None, uri='http://ala.org.au/terms/1.0/labels')
    nameAccordingTo = fields.String(missing=None)
    taxonRemarks = fields.String(missing=None)
    provenance = fields.String(missing=None, uri='http://purl.org/dc/terms/provenance')
    source = fields.String(missing=None, uri='http://purl.org/dc/terms/source')

    class Meta:
        ordered = True
        uri = 'http://rs.gbif.org/terms/1.0/VernacularName'
        namespace = 'http://rs.tdwg.org/dwc/terms/'

class VernacularNameSchema(Schema):
    """
    Schema for vernacular names with only a scientific name to match, rather than a taxonId
    """
    scientificName = fields.String(metadata={ 'export': True })
    kingdom = fields.String(missing=None)
    phylum = fields.String(missing=None)
    class_ = fields.String(missing=None, data_key='class', uri='http://rs.tdwg.org/dwc/terms/class')
    order = fields.String(missing=None)
    family = fields.String(missing=None)
    vernacularName = fields.String(metadata={ 'export': True })
    nameID = fields.String(missing=None, uri='http://ala.org.au/terms/1.0/nameID')
    datasetID = fields.String(missing=None, metadata={ 'export': True })
    status = fields.String(missing=None, uri='http://ala.org.au/terms/1.0/status')
    language = fields.String(missing=None, uri='http://purl.org/dc/terms/language')
    temporal = fields.String(missing=None, uri='http://purl.org/dc/terms/temporal')
    locationID = fields.String(missing=None)
    locality = fields.String(missing=None)
    stateProvince = fields.String(missing=None)
    countryCode = fields.String(missing=None)
    sex = fields.String(missing=None)
    lifeStage = fields.String(missing=None)
    isPlural = fields.Boolean(missing=None, uri='http://rs.gbif.org/terms/1.0/isPlural')
    isPreferredName = fields.Boolean(missing=None, uri='http://rs.gbif.org/terms/1.0/isPreferredName')
    organismPart = fields.String(missing=None, uri='http://rs.gbif.org/terms/1.0/organismPart')
    labels = fields.String(missing=None, uri='http://ala.org.au/terms/1.0/labels')
    nameAccordingTo = fields.String(missing=None)
    namePublishedIn = fields.String(missing=None)
    taxonRemarks = fields.String(missing=None)
    provenance = fields.String(missing=None, uri='http://purl.org/dc/terms/provenance')
    source = fields.String(missing=None, uri='http://purl.org/dc/terms/source')

    class Meta:
        ordered = True
        uri = 'http://rs.gbif.org/terms/1.0/VernacularName'
        namespace = 'http://rs.tdwg.org/dwc/terms/'

class MappingSchema(Schema):
    term = fields.String()
    mapping = fields.String(missing=None)

class TaxonomicStatusMapSchema(Schema):
    Term = fields.String()
    DwC = fields.String()
    Accepted = fields.Boolean()
    Synonym = fields.Boolean()
    Misapplied = fields.Boolean()

class NomenclaturalCodeMapSchema(Schema):
    kingdom = fields.String()
    nomenclaturalCode = fields.String()
