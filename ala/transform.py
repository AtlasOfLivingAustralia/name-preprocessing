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
import urllib.parse
from typing import Dict

import attr
import marshmallow
import requests

from ala.schema import CollectorySchema
from dwc.schema import VernacularNameSchema, ExtendedTaxonSchema
from processing.dataset import Port, Dataset, Record
from processing.node import ProcessingContext
from processing.source import Source, CsvSource
from processing.transform import extract_href


@attr.s
class SpeciesListSource(Source):
    """Read a species list from the ALA species list service"""
    service: str = attr.ib()
    link: str = attr.ib()
    batchsize: int = attr.ib()

    @classmethod
    def create(cls, id: str, service="https://lists.ala.org.au/ws", link="https://lists.ala.org.au", batchsize=100):
        schema = ExtendedTaxonSchema()
        output = Port.port(schema)
        error = Port.error_port(schema)
        return SpeciesListSource(id, output, error, service, link, batchsize)

    def execute(self, context: ProcessingContext):
        output = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        fieldmap = dict()
        if 'vernacularName' in self.output.schema.fields:
            fieldmap['commonName'] = 'vernacularName'
            fieldmap['vernacular'] = 'vernacularName'
            fieldmap['common'] = 'vernacularName'
        fieldmap['rawkingdom'] = 'kingdom'
        fieldmap['rawfamily'] = 'family'
        fieldmap['raworder'] = 'order'
        fieldmap['rawclass'] = 'class_'
        fieldmap['rawphylum'] = 'phylum'
        fieldmap['rawrank'] = 'taxonRank'
        fieldmap.update({(field.data_key if field.data_key is not None else field.name).lower(): field.name for field in
                         self.output.schema.fields.values()})
        dr = context.get_default('datasetID')
        idstem = "ALA_" + dr.upper() + "_"
        default_source = self.link + "/speciesListItem/list/" + dr
        # If we ever get batch size working
        cont = True
        line = 1
        offset = 0
        while cont:
            request = requests.request('GET', self.service + "/speciesListItems/" + dr,
                                       params={'q': '', 'includeKVP': 'true', 'max': 1000000})
            list = request.json()
            for item in list:
                data = dict()
                for kv in item.get('kvpValues', []):
                    key: str = kv.get('key', None)
                    value = kv.get('value', None)
                    # if value == '0':
                    #     value = ''
                    if key is not None:
                        key = key.lower().replace(' ', '')
                    if key is not None and value is not None and key in fieldmap:
                        data[fieldmap[key]] = value
                # theory - commonName used to be in key value pairs - now appears to be top level property like name
                if 'commonName' in item:
                    data["vernacularName"] = item['commonName']
                data['taxonID'] = idstem + str(line)
                data['scientificName'] = item['name']
                data['datasetID'] = item['dataResourceUid']
                if 'source' in data:
                    data['source'] = extract_href(data['source'])
                else:
                    data['source'] = default_source + "?q=" + urllib.parse.quote_plus(item['name'])
                if 'taxonomicStatus' not in data:
                    status = 'inferredUnplaced'
                    if 'kingdom' in data or 'phylum' in data or 'class' in data or 'order' in data or 'family' in data:
                        status = context.get_default('defaultAcceptedStatus', 'inferredAccepted')
                    if 'acceptedNameUsage' in data:
                        status = context.get_default('defaultSynonymStatus', 'inferredSynonym')
                    data['taxonomicStatus'] = status
                record = Record(line, data, None)
                if data['scientificName'] is None:
                    errors.add(Record.error(record, None, "No scientific name"))
                    self.count(self.ERROR_COUNT, record, context)
                else:
                    output.add(record)
                    self.count(self.ACCEPTED_COUNT, record, context)
                self.count(self.PROCESSED_COUNT, record, context)
                line += 1
            offset += len(list)
            # cont = len(list) > 0
            cont = False
        context.save(self.output, output)
        context.save(self.error, errors)


@attr.s
class VernacularListSource(Source):
    """Read a vernacular list from the ALA species list service"""
    service: str = attr.ib()
    link: str = attr.ib()
    aliases: Dict[str, str] = attr.ib(factory=dict)

    @classmethod
    def create(cls, id: str, aliases={}, service="https://lists.ala.org.au/ws", link="https://lists.ala.org.au"):
        schema = VernacularNameSchema()
        output = Port.port(schema)
        error = Port.error_port(schema)
        return VernacularListSource(id, output, error, service, link, aliases)

    def execute(self, context: ProcessingContext):
        output = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        fieldmap = {(field.data_key if field.data_key is not None else field.name): field.name for field in
                    self.output.schema.fields.values()}
        vernacular = self.output.schema.fields.get('vernacularName')
        fieldmap['commonName'] = vernacular
        fieldmap['common name'] = vernacular
        fieldmap['vernacular name'] = vernacular
        additional = {(alias, fieldmap.get(field)) for (alias, field) in self.aliases.items()}
        fieldmap.update(additional)
        dr = context.get_default('datasetID')
        idstem = "ALA_" + dr.upper() + "_V"
        defaultSource = self.link + "/speciesListItem/list/" + dr
        list = requests.get(self.service + "/speciesListItems/" + dr, params={'includeKVP': True, 'max': 10000}).json()
        line = 1
        for item in list:
            data = dict()
            for kv in item.get('kvpValues', []):
                key = kv.get('key', None)
                value = kv.get('value', None)
                if key is not None and value is not None and key in fieldmap:
                    data[fieldmap[key]] = value
            data['taxonID'] = idstem + str(line)
            data['scientificName'] = item['name']
            data['datasetID'] = item['dataResourceUid']
            if 'source' in data:
                data['source'] = extract_href(data['source'])
            else:
                data['source'] = defaultSource + "?q=" + urllib.parse.quote_plus(item['name'])
            record = Record(line, data, None)
            if data.get('vernacularName') is None:
                errors.add(
                    Record.error(record, None, "No vernacular name for " + dr + " " + str(line) + " " + item['name']))
                self.count(self.ERROR_COUNT, record, context)
            else:
                output.add(record)
                self.count(self.ACCEPTED_COUNT, record, context)
            self.count(self.PROCESSED_COUNT, record, context)
            line += 1
        context.save(self.output, output)
        context.save(self.error, errors)


@attr.s
class CollectorySource(Source):
    """Read a collectory metadata from the ALA collectory service"""
    service: str = attr.ib()

    @classmethod
    def create(cls, id: str, service="https://collections.ala.org.au/ws"):
        schema = CollectorySchema()
        output = Port.port(schema)
        error = Port.error_port(schema)
        return CollectorySource(id, output, error, service)

    def execute(self, context: ProcessingContext):
        output = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        record = None
        try:
            dr = context.get_default('datasetID')
            url = self.service + "/dataResource/" + dr
            self.logger.info("Retrieving metadata from " + url)
            collection = {'uid': dr}
            try:
                collection = requests.get(url).json()
            except Exception as err:
                self.logger.error(f"Unable to retrieve {url}: {err}")
                if self.fail_on_exception:
                    raise err
                self.logger.warn(f"Using defaults for {dr}")
            metadata = dict()
            metadata['uid'] = collection.get('uid', None)
            metadata['name'] = collection.get('name', None)
            metadata['acronym'] = collection.get('acronym', None)
            metadata['pubShortDescription'] = collection.get('pubShortDescription', None)
            metadata['pubDescription'] = collection.get('pubDescription', None)
            metadata['techDescription'] = collection.get('techDescription', None)
            metadata['websiteUrl'] = collection.get('websiteUrl', None)
            metadata['alaPublicUrl'] = collection.get('alaPublicUrl', None)
            if collection.get('address', None) is not None:
                address = collection['address']
                metadata['street'] = address.get('street', None)
                metadata['city'] = address.get('city', None)
                metadata['state'] = address.get('state', None)
                metadata['postcode'] = address.get('postcode', None)
                metadata['country'] = address.get('country', None)
                metadata['postBox'] = address.get('postBox', None)
            if context.get_default('defaultOrganisation') is not None:
                metadata['organisation'] = context.get_default('defaultOrganisation')
            if collection.get('provider', None) is not None:
                metadata['organisation'] = collection['provider'].get('name', None)
            if collection.get('institution', None) is not None:
                metadata['organisation'] = collection['institution'].get('name', None)
            metadata['phone'] = collection.get('phone', None)
            metadata['email'] = collection.get('email', None)
            metadata['rights'] = collection.get('rights', None)
            metadata['license'] = collection.get('license', None)
            metadata['citation'] = collection.get('citation', None)
            metadata['lastUpdated'] = collection.get('lastUpdated', None)
            metadata['doi'] = collection.get('doi', None)
            record = Record(1, self.output.schema.load(metadata), None)
            output.add(record)
            self.count(self.ACCEPTED_COUNT, record, context)
        except marshmallow.ValidationError as err:
            err.data['_line'] = 1
            err.data['_messages'] = err.messages
            error = Record(1, err.data, err.messages)
            errors.add(error)
            self.count(self.ERROR_COUNT, error, context)
        self.count(self.PROCESSED_COUNT, record, context)
        context.save(self.output, output)
        context.save(self.error, errors)


class PublisherSource(CsvSource):
    """Default source for publisher metadata"""

    @classmethod
    def create(cls, id: str, file='ala-metadata.csv', dialect='ala', **kwargs):
        output = Port.port(CollectorySchema())
        error = Port.error_port(output.schema)
        return PublisherSource(id, output, error, file, dialect, **kwargs)
