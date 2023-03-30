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

import csv

from marshmallow import Schema, post_load

import processing.fields as fields


class col_dialect(csv.Dialect):
    """Describe the usual properties of CoL tsv files."""
    delimiter = '\t'
    quotechar = None
    doublequote = False
    skipinitialspace = True
    lineterminator = '\n'
    quoting = csv.QUOTE_NONE
    strict = True
csv.register_dialect("col", col_dialect)

_MAX_REFERENCE = 1024

class ColTaxonSchema(Schema):
    taxonID = fields.String(data_key = 'dwc:taxonID')
    acceptedNameUsageID = fields.String(missing=None, data_key = 'dwc:acceptedNameUsageID')
    parentNameUsageID = fields.String(missing=None, data_key = 'dwc:parentNameUsageID')
    originalNameUsageID = fields.String(missing=None, data_key = 'dwc:originalNameUsageID')
    scientificNameID = fields.String(missing=None, data_key = 'dwc:scientificNameID')
    datasetID = fields.String(missing=None, data_key = 'dwc:datasetID')
    taxonomicStatus = fields.String(missing=None, data_key = 'dwc:taxonomicStatus')
    taxonRank = fields.String(missing=None, data_key = 'dwc:taxonRank')
    scientificName = fields.String(data_key = 'dwc:scientificName')
    scientificNameAuthorship = fields.String(missing=None, data_key = 'dwc:scientificNameAuthorship')
    notho = fields.String(missing=None, data_key = 'col:notho')
    genericName = fields.String(missing=None, data_key = 'dwc:genericName')
    infragenericEpithet = fields.String(missing=None, data_key = 'dwc:infragenericEpithet')
    specificEpithet = fields.String(missing=None, data_key = 'dwc:specificEpithet')
    infraspecificEpithet = fields.String(missing=None, data_key = 'dwc:infraspecificEpithet')
    cultivarEpithet = fields.String(missing=None, data_key = 'dwc:cultivarEpithet')
    nameAccordingTo = fields.String(missing=None, data_key = 'dwc:nameAccordingTo')
    namePublishedIn = fields.String(missing=None, data_key = 'dwc:namePublishedIn')
    nomeclaturalCode = fields.String(missing=None, data_key = 'dwc:nomenclaturalCode')
    nomenclaturalStatus = fields.String(missing=None, data_key = 'dwc:nomenclaturalStatus')
    taxonRemarks = fields.String(missing=None, data_key = 'dwc:taxonRemarks')
    references = fields.String(missing=None, data_key = 'dcterms:references')

    class Meta:
        ordered = True

    def drop_long(self, value):
        if value is not None and len(value) > _MAX_REFERENCE:
            return None
        return value

    # Prevent ludicrously long references
    @post_load
    def handle_long_references(self, data, **kwargs):
        data['namePublishedIn'] = self.drop_long(data['namePublishedIn'])
        data['nameAccordingTo'] = self.drop_long(data['nameAccordingTo'])
        data['references'] = self.drop_long(data['references'])
        return data

class ColTaxonWithClassificationSchema(ColTaxonSchema):
    kingdom = fields.String(missing=None)
    phylum = fields.String(missing=None)
    subphylum = fields.String(missing=None)
    class_ = fields.String(missing=None, data_key='class')
    subclass = fields.String(missing=None)
    order = fields.String(missing=None)
    suborder = fields.String(missing=None)
    infraorder = fields.String(missing=None)
    family = fields.String(missing=None)
    genus = fields.String(missing=None)
    subgenus = fields.String(missing=None)

class ColDistributionSchema(Schema):
    taxonID = fields.String(data_key = 'dwc:taxonID')
    occurrenceStatus = fields.String(missing=None, data_key = 'dwc:occurrenceStatus')
    locationID = fields.String(missing=None, data_key = 'dwc:locationID')
    locality = fields.String(missing=None, data_key = 'dwc:locality')
    countryCode = fields.String(missing=None, data_key = 'dwc:countryCode')
    source = fields.String(missing=None, data_key = 'dcterms:source')

    class Meta:
        ordered = True

class ColVernacularSchema(Schema):
    taxonID = fields.String(data_key = 'dwc:taxonID')
    language = fields.String(missing=None, data_key = 'dcterms:language')
    vernacularName = fields.String(missing=None, data_key = 'dwc:vernacularName')

    class Meta:
        ordered = True

class ColAcceptedKingdomSchema(Schema):
    kingdom = fields.String()
    taxonID = fields.String()
    useDistribution = fields.Boolean()
    useDataset = fields.Boolean()
    useRank = fields.Boolean()

class ColAcceptedDatasetSchema(Schema):
    datasetID = fields.String()
    datasetName = fields.String()

class ColAcceptedLocationSchema(Schema):
    locationID = fields.String()
    locality = fields.String()
    country = fields.String()

class ColAcceptedLanguageSchema(Schema):
    language = fields.String()
    code = fields.String()

class ColAcceptedRankSchema(Schema):
    taxonRank = fields.String()

class MarineRegionsLocationMapSchema(Schema):
    locationID = fields.String()
    acceptedLocationID = fields.String()
    locality = fields.String()
    countryCode = fields.String()
    originalLocality = fields.String()

    class Meta:
        ordered = True
