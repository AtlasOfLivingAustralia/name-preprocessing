from typing import Dict

import attr
import marshmallow
import requests

from ala.schema import CollectorySchema
from dwc.schema import TaxonSchema, VernacularSchema, VernacularNameSchema
from processing.dataset import Port, Dataset, Record
from processing.node import ProcessingContext
from processing.source import Source, CsvSource

@attr.s
class SpeciesListSource(Source):
    """Read a species list from the ALA species list service"""
    service: str = attr.ib()

    @classmethod
    def create(cls, id:str, service="https://lists.ala.org.au/ws"):
        schema = TaxonSchema()
        output = Port.port(schema)
        error = Port.error_port(schema)
        return SpeciesListSource(id, output, error, service)

    def execute(self, context: ProcessingContext):
        output = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        fieldmap = { (field.data_key if field.data_key is not None else field.name): field.name for field in self.output.schema.fields.values()}
        dr = context.get_default('datasetID')
        list = requests.get(self.service + "/speciesListItems/" + dr, params={'includeKVP': True}).json()
        line = 0
        for item in list:
            data = dict()
            for kv in item.get('kvpValues', []):
                key = kv.get('key', None)
                value = kv.get('value', None)
                if key is not None and value is not None and key in fieldmap:
                    data[fieldmap[key]] = value
            data['taxonID'] = 'ALA_' + str(item['id'])
            data['scientificName'] = item['name']
            data['datasetID'] = item['dataResourceUid']
            if 'taxonomicStatus' not in data:
                status = 'inferredUnplaced'
                if 'kingdom' in data or 'phylum' in data or 'class' in data or 'order' in data or 'family' in data:
                    status = 'inferredAccepted'
                if 'acceptedNameUsage' in data:
                    status = 'inferredSynonym'
                data['taxonomicStatus'] = status
            record = Record(line, data, None)
            if data['scientificName'] is None:
                errors.add(Record.error(record, None, "No scientific name"))
                self.count(self.ERROR_COUNT, record, context)
            else:
                output.add(record)
                self.count(self.ACCEPTED_COUNT, record, context)
            self.count(self.PROCESSED_COUNT, record, context)
        context.save(self.output, output)
        context.save(self.error, errors)

@attr.s
class VernacularListSource(Source):
    """Read a vernacular list from the ALA species list service"""
    service: str = attr.ib()
    aliases: Dict[str, str] = attr.ib(factory=dict)

    @classmethod
    def create(cls, id:str, aliases={}, service="https://lists.ala.org.au/ws"):
        schema = VernacularNameSchema()
        output = Port.port(schema)
        error = Port.error_port(schema)
        return VernacularListSource(id, output, error, service, aliases)

    def execute(self, context: ProcessingContext):
        output = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        fieldmap = { (field.data_key if field.data_key is not None else field.name): field.name for field in self.output.schema.fields.values()}
        additional = { (alias, fieldmap.get(field)) for (alias, field)  in self.aliases.items() }
        fieldmap.update(additional)
        dr = context.get_default('datasetID')
        list = requests.get(self.service + "/speciesListItems/" + dr, params={'includeKVP': True}).json()
        line = 0
        for item in list:
            data = dict()
            for kv in item.get('kvpValues', []):
                key = kv.get('key', None)
                value = kv.get('value', None)
                if key is not None and value is not None and key in fieldmap:
                    data[fieldmap[key]] = value
            data['taxonID'] = 'ALA_' + str(item['id'])
            data['scientificName'] = item['name']
            data['datasetID'] = item['dataResourceUid']
            record = Record(line, data, None)
            if data['vernacularName'] is None:
                errors.add(Record.error(record, None, "No scientific name"))
                self.count(self.ERROR_COUNT, record, context)
            else:
                output.add(record)
                self.count(self.ACCEPTED_COUNT, record, context)
            self.count(self.PROCESSED_COUNT, record, context)
        context.save(self.output, output)
        context.save(self.error, errors)

@attr.s
class CollectorySource(Source):
    """Read a collectory metadata from the ALA collectory service"""
    service: str = attr.ib()

    @classmethod
    def create(cls, id:str, service="https://collections.ala.org.au/ws"):
        schema = CollectorySchema()
        output = Port.port(schema)
        error = Port.error_port(schema)
        return CollectorySource(id, output, error, service)

    def execute(self, context: ProcessingContext):
        output = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        try:
            dr = context.get_default('datasetID')
            url = self.service + "/dataResource/" + dr
            self.logger.info("Retrieving metadata from " + url)
            collection = requests.get(url).json()
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

