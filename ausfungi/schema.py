from marshmallow import Schema

import processing.fields as fields


class AusFungiTaxonSchema(Schema):
    taxonID = fields.String(missing=None)
    scientificNameID = fields.String(missing=None)
    acceptedNameUsageID = fields.String(missing=None)
    parentNameUsageID = fields.String(missing=None)
    originalNameUsageID = fields.String(missing=None)
    namePublishedInID = fields.String(missing=None)
    taxonConceptID = fields.String(missing=None)
    scientificName = fields.String(missing=None)
    acceptedNameUsage = fields.String(missing=None)
    parentNameUsage = fields.String(missing=None)
    originalNameUsage = fields.String(missing=None)
    namePublishedIn = fields.String(missing=None)
    namePublishedInYear = fields.String(missing=None)
    nameAccordingTo = fields.String(missing=None)
    higherClassification = fields.String(missing=None)
    kingdom = fields.String(missing=None)
    phylum = fields.String(missing=None)
    subphylum = fields.String(missing=None)
    class_ = fields.String(missing=None, data_key='class')
    subclass = fields.String(missing=None)
    order = fields.String(missing=None)
    family = fields.String(missing=None)
    genus = fields.String(missing=None)
    specificEpithet = fields.String(missing=None)
    infraspecificEpithet = fields.String(missing=None)
    taxonRank = fields.String(missing=None)
    scientificNameAuthorship = fields.String(missing=None)
    nomenclaturalCode = fields.String(missing=None)
    taxonomicStatus = fields.String(missing=None)
    nomenclaturalStatus = fields.String(missing=None)
    occurrenceStatus = fields.String(missing=None)

    class Meta:
        ordered = True
        uri = '"http://rs.tdwg.org/dwc/terms/Taxon'
        namespace = 'http://rs.tdwg.org/dwc/terms/'

class AusFungiIdentifierSchema(Schema):
    coreid = fields.String()
    identifier = fields.String()
    title = fields.String(missing=None)
    format = fields.String(missing=None)

    class Meta:
        ordered = True
        uri = 'http://rs.gbif.org/terms/1.0/Identifier'
        namespace = 'http://purl.org/dc/terms/'
