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
    taxonomicFlags = fields.String(missing=None, uri='http://ala.org.au/terms/1.0/taxonomicFlags')

    class Meta:
        ordered = True
        uri = 'Taxon'
        namespace = 'http://rs.tdwg.org/dwc/terms/'

class ExtendedTaxonSchema(TaxonSchema):
    vernacularName = fields.String(missing=None)

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
    scientificName = fields.String(metadata={'export': True})
    scientificNameAuthorship = fields.String()
    kingdom = fields.String(missing=None)
    phylum = fields.String(missing=None)
    class_ = fields.String(missing=None, data_key='class', uri='http://rs.tdwg.org/dwc/terms/class')
    order = fields.String(missing=None)
    family = fields.String(missing=None)
    vernacularName = fields.String(metadata={'export': True})
    nameID = fields.String(missing=None, uri='http://ala.org.au/terms/1.0/nameID')
    datasetID = fields.String(missing=None, metadata={'export': True})
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


class IdentifierSchema(Schema):
    """
    Schema for additional identifiers
    """
    taxonID = fields.String(required=True, metadata={ 'export': True })
    identifier = fields.String(required=True, uri="http://purl.org/dc/terms/identifier")
    datasetID = fields.String(missing=None)
    title = fields.String(missing=None, uri='http://purl.org/dc/terms/title')
    status = fields.String(missing=None, uri='http://ala.org.au/terms/1.0/status')
    format = fields.String(missing=None, uri='http://purl.org/dc/terms/format')
    source = fields.String(missing=None, uri='http://purl.org/dc/terms/source')
    taxonRemarks = fields.String(missing=None)
    provenance = fields.String(missing=None, uri='http://purl.org/dc/terms/provenance')

    class Meta:
        ordered = True
        uri = 'http://rs.gbif.org/terms/1.0/Identifier'
        namespace = 'http://rs.tdwg.org/dwc/terms/'


class IdentifierNameSchema(Schema):
    """
    Schema for additional identifiers matched by scientific name
    """
    scientificName = fields.String(metadata={'export': True})
    scientificNameAuthorship = fields.String()
    kingdom = fields.String(missing=None)
    phylum = fields.String(missing=None)
    class_ = fields.String(missing=None, data_key='class', uri='http://rs.tdwg.org/dwc/terms/class')
    order = fields.String(missing=None)
    family = fields.String(missing=None)
    identifier = fields.String(required=True, uri="http://purl.org/dc/terms/identifier")
    datasetID = fields.String(missing=None)
    title = fields.String(missing=None, uri='http://purl.org/dc/terms/title')
    status = fields.String(missing=None, uri='http://ala.org.au/terms/1.0/status')
    format = fields.String(missing=None, uri='http://purl.org/dc/terms/format')
    source = fields.String(missing=None, uri='http://purl.org/dc/terms/source')
    taxonRemarks = fields.String(missing=None)
    provenance = fields.String(missing=None, uri='http://purl.org/dc/terms/provenance')

    class Meta:
        ordered = True
        uri = 'http://rs.gbif.org/terms/1.0/Identifier'
        namespace = 'http://rs.tdwg.org/dwc/terms/'


class DistributionSchema(Schema):
    """
    Schema for distributions
    """
    taxonID = fields.String(required=True, metadata={ 'export': True })
    locationID = fields.String(required=True)
    locality = fields.String(required=True)
    country = fields.String(required=True)
    countryCode = fields.String(missing=None)
    continent = fields.String(required=True)
    island = fields.String(required=True)
    islandGroup = fields.String(required=True)
    waterBody = fields.String(required=True)
    lifeStage = fields.String(missing=None, vocabulary='http://rs.gbif.org/vocabulary/gbif/life_stage.xml')
    occurrenceStatus = fields.String(missing=None, vocabulary='http://rs.gbif.org/vocabulary/gbif/distribution_status_2020-07-15.xml')
    establishmentMeans = fields.String(missing=None, vocabulary='http://rs.gbif.org/vocabulary/dwc/establishment_means_2022-02-02.xml')
    degreeOfEstablishment = fields.String(missing=None, vocabulary='http://rs.gbif.org/vocabulary/dwc/degree_of_establishment_2022-02-02.xml')
    pathway = fields.String(missing=None, vocabulary='http://rs.gbif.org/vocabulary/dwc/pathway_2022-02-02.xml')
    threatStatus = fields.String(missing=None, uri='http://iucn.org/terms/threatStatus', vocabulary='http://rs.gbif.org/vocabulary/iucn/threat_status.xml')
    appendixCITES = fields.String(missing=None, uri='http://rs.gbif.org/terms/1.0/appendixCITES', vocabulary='http://rs.gbif.org/vocabulary/un/cites_appendix.xml')
    eventDate = fields.String(missing=None)
    source = fields.String(missing=None, uri='http://purl.org/dc/terms/source')
    occurrenceRemarks = fields.String(missing=None)
    datasetID = fields.String(missing=None)
    provenance = fields.String(missing=None, uri='http://purl.org/dc/terms/provenance')
    locationRemarks = fields.String(missing=None)

    class Meta:
        ordered = True
        uri = 'http://rs.gbif.org/terms/1.0/Distribution'
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
    taxonomicFlags = fields.String()

class NameMapSchema(Schema):
    original = fields.String()
    replacement = fields.String(missing=None)
    rank = fields.String(missing=None)
    comment = fields.String(missing=None)

class EstablishmentMeansMapSchema(Schema):
    term = fields.String()
    establishmentMeans = fields.String(missing=None)
    occurrenceStatus = fields.String(missing=None)
    degreeOfEstablishment = fields.String(missing=None)
    pathway = fields.String(missing=None)
    comment = fields.String(missing=None)


class LocationMapSchema(Schema):
    locationID = fields.String()
    locality = fields.String()
    locationRemarks = fields.String(missing=None)

    class Meta:
        ordered = True
        uri = 'http://ala.org.au/terms/1.0/LocationName'
        namespace = 'http://rs.tdwg.org/dwc/terms/'

class LocationIdentifierMapSchema(Schema):
    locationID = fields.String()
    identifier = fields.String(uri='http://purl.org/dc/terms/title')
    locality = fields.String(missing=None)
    mappedLocality = fields.String(missing=None, uri='http://ala.org.au/terms/1.0/mappedLocality')
    locationRemarks = fields.String(missing=None)

    class Meta:
        ordered = True
        uri = 'http://ala.org.au/terms/1.0/LocationIdentifier'
        namespace = 'http://rs.tdwg.org/dwc/terms/'

class LocationSchema(Schema):
    """
    Schema for the output of a simple Darwin Core Occurrence
    """
    locationID = fields.String()
    parentLocationID = fields.String(missing=None, uri='http://ala.org.au/terms/1.0/parentLocationID')
    acceptedLocationID = fields.String(missing=None, uri='http://ala.org.au/terms/1.0/acceptedLocationID')
    datasetID = fields.String(missing=None)
    geographyType = fields.String(missing=None, uri='http://ala.org.au/terms/1.0/geographyType')
    locality = fields.String()
    countryCode = fields.String(missing=None)
    decimalLatitude = fields.Float(missing=None)
    decimalLongitude = fields.Float(missing=None)
    area = fields.Float(missing=None, uri='http://ala.org.au/terms/1.0/area')
    weight = fields.Float(missing=None, uri='http://ala.org.au/bayesian/1.0/weight')
    locationRemarks = fields.String(missing=None)

    class Meta:
        ordered = True
        uri = 'http://rs.tdwg.org/dwc/terms/Location'
        namespace = 'http://rs.tdwg.org/dwc/terms/'

class VernacularStatusSchema(Schema):
    pattern = fields.String()
    include = fields.Boolean()
    status = fields.String(missing=None)
    taxonRemarks = fields.String(missing=None)

class ScientificNameStatusSchema(Schema):
    pattern = fields.String()
    replace = fields.String()
    include = fields.Boolean(missing=True)
    taxonomicStatus = fields.String(missing=None)
    nomenclaturalStatus = fields.String(missing=None)
    taxonRemarks = fields.String(missing=None)

class ClassificationSchema(Schema):
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

    class Meta:
        ordered = True
        uri = 'Classification'
        namespace = 'http://rs.tdwg.org/dwc/terms/'
