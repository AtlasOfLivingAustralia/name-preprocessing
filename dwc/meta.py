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
import datetime
from typing import List, Dict

import attr
from lxml.etree import _Element, Element, SubElement, ElementTree, Comment, parse

from processing.dataset import Port, Record
from processing.node import Node, ProcessingContext, ProcessingException
from processing.sink import Sink
from processing.transform import choose, normalise_spaces

METANS = 'http://rs.tdwg.org/dwc/text/'
EMLNS = 'eml://ecoinformatics.org/eml-2.1.1'

def _attr_translate(value: str):
    """
    Translate escapes for common DwCA delimiters, replacing tabs and newlines by escapes.

    :param value: The input string
    :return:
    """
    value = value.replace('\t', '\\t')
    value = value.replace('\n', '\\n')
    value = value.replace('\r', '\\r')
    return value

def _make_tag(tag: str, ns: str = None):
    return '{' + ns + '}' + tag if ns else tag

def _get_dom(source, context: ProcessingContext):
    if source is None:
        return None
    if isinstance(source, _Element):
        return source
    return parse(context.locate_input_file(source)).getroot()

@attr.s
class MetaFile(Node):
    """Generates a DwCA meta.xml file for a series of files."""

    core: Sink = attr.ib()
    extensions: List[Sink] = attr.ib()

    @classmethod
    def create(cls, id: str, *args, **kwargs):
        """
        Construct a meta builder.

        :param id: The node id
        :param args: One or more sinks
        :param kwargs: Addtional keyword arguments

        :return: A metafile sink
        """
        core = args[0]
        extensions = args[1:]
        return MetaFile(id, core, extensions, **kwargs)

    def predecessors(self) -> List[Node]:
        predecessors = super().predecessors()
        predecessors.append(self.core)
        predecessors.extend(self.extensions)
        return predecessors

    def report(self, context: ProcessingContext):
        self.logger.info("Created meta file")

    def execute(self, context: ProcessingContext):
        super().execute(context)
        root = Element(_make_tag('archive', METANS), nsmap={ None: METANS }, metadata='eml.xml')
        root.append(Comment('Generated on {timestamp}'.format(timestamp=str(datetime.datetime.now()))))
        self.createEntry(root, True, self.core, context)
        for ext in self.extensions:
            self.createEntry(root, False, ext, context)
        output = context.locate_output_file('meta.xml', False)
        document = ElementTree(root)
        document.write(output, encoding='utf-8', pretty_print=True)

    def createEntry(self, root, core: bool, sink: Sink, context: ProcessingContext):
        schema = sink.input.schema
        rowType = None
        if hasattr(schema.Meta, 'uri'):
            rowType = schema.Meta.uri
        if rowType is None and  hasattr(schema.Meta, 'metadata'):
            rowType = schema.Meta.metadata.get('uri')
        if rowType is None:
            rowType = 'http://rs.tdwg.org/dwc/terms/Taxon'
        namespace = None
        if hasattr(schema.Meta, 'namespace'):
            namespace = schema.Meta.namespace
        dialect = sink.dialect if hasattr(sink, 'dialect') else 'ala'
        dialect = csv.get_dialect(dialect)
        format = {
            'rowType': rowType,
            'encoding': 'UTF-8',
            'fieldsTerminatedBy': _attr_translate(dialect.delimiter),
            'linesTerminatedBy': _attr_translate(dialect.lineterminator),
            'fieldsEnclosedBy': _attr_translate(dialect.quotechar),
            'ignoreHeaderLines': '1'
        }
        table = SubElement(root, _make_tag("core" if core else "extension", METANS), format)
        files = SubElement(table, _make_tag("files", METANS))
        location = SubElement(files, _make_tag('location', METANS))
        location.text = sink.fileName()
        SubElement(table, _make_tag('id' if core else 'coreid', METANS), index='0')
        fields = sink.reduced_fields(context)
        for i, field in enumerate(fields):
            field = schema.fields[field]
            uri = field.metadata.get('uri', namespace + field.name if namespace is not None else field.name)
            SubElement(table, _make_tag('field', METANS), index=str(i), term=uri)

    def vertex_color(self, context: ProcessingContext):
        return 'cadetblue'

@attr.s
class EmlFile(Node):
    """Generates eml.xml file for a series of files."""
    metadata: Port = attr.ib()
    publisher: Port = attr.ib()


    @classmethod
    def create(cls, id: str, metadata: Port, publisher: Port, **kwargs):
        """
        Construct a meta builder.

        :param id: The node id
        :param source: The metadata entry for the record(s)
        :param publisher: The xml containing the publisher metadata, either a file path or a dom tree
        :param kwargs: Addtional keyword arguments

        :return: A metafile sink
        """
        return EmlFile(id, metadata, publisher, **kwargs)

    def inputs(self) -> Dict[str, Port]:
        inputs = super().inputs()
        inputs['metadata'] = self.metadata
        if self.publisher is not None:
            inputs['publisher'] = self.publisher
        return inputs

    def report(self, context: ProcessingContext):
        self.logger.info("Created eml file")

    def execute(self, context: ProcessingContext):
        super().execute(context)
        metadata = context.acquire(self.metadata)
        if len(metadata.rows) == 0:
            raise ProcessingException("No metadata rows")
        publisher = context.acquire(self.publisher) if self.publisher is not None else None
        if publisher is not None and len(publisher.rows) == 0:
            raise ProcessingException("No publisher rows")
        publisher = publisher.rows[0]
        primary = metadata.rows[0]
        secondary = metadata.rows[1:]
        timestamp = datetime.datetime.utcnow()
        pubdate = primary.lastUpdated
        series = choose(primary.acronym, context.get_default('series'), primary.uid.upper())
        identifier = series
        if pubdate is not None:
            identifier += "-" + pubdate.strftime("%Y%m%d")
        identifier += '-' + timestamp.strftime("%Y%m%d")
        root = Element(_make_tag('eml', EMLNS), nsmap={ 'eml': EMLNS })
        self.addDataset(primary, publisher, secondary, series, identifier, timestamp, context, root)
        self.addAdditionalMetadata(primary, publisher, identifier, timestamp, context, root)
        output = context.locate_output_file('eml.xml', False)
        document = ElementTree(root)
        document.write(output, encoding='utf-8', pretty_print=True)

    def addDataset(self, primary: Record, publisher: Record, secondary: List[Record], series: str, identifier: str, timestamp: datetime.datetime, context: ProcessingContext, parent: Element):
        dataset = SubElement(parent, _make_tag('dataset'))
        alternativeIdentifier = SubElement(dataset, _make_tag('alternativeIdentifier'))
        alternativeIdentifier.text = identifier
        title = SubElement(dataset, _make_tag('title'))
        title.text = primary.name
        self.addOrganisation(primary, 'creator', dataset)
        for org in secondary:
            self.addOrganisation(org, 'creator', dataset)
        self.addOrganisation(publisher, 'metadataProvider', dataset)
        if primary.lastUpdated is not None:
            pubdate = SubElement(dataset, _make_tag('pubDate'))
            pubdate.text = primary.lastUpdated.strftime('%F')
        if context.get_default('language') is not None:
            language = SubElement(dataset, _make_tag('language'))
            language.text = context.get_default('language')
        seriesElt = SubElement(dataset, _make_tag('series'))
        seriesElt.text = series
        abstract = SubElement(dataset, _make_tag('abstract'))
        self.addPara(primary.pubDescription, abstract)
        for org in secondary:
            self.addPara(secondary.pubDescription, abstract)
        intellectualRights = SubElement(dataset, _make_tag('intellectualRights'))
        if primary.license is not None:
            licensed = SubElement(dataset, _make_tag('licensed'))
            licenseName = SubElement(licensed, _make_tag('licenceName'))
            licenseName.text = primary.license
        self.addCopyright(primary, timestamp, intellectualRights)
        for org in secondary:
            self.addCopyright(org, timestamp, intellectualRights)
        if publisher.organisation != primary.organisation:
            self.addCopyright(publisher, timestamp, intellectualRights)
        if publisher.websiteUrl is not None:
            distribution = SubElement(dataset, _make_tag('distribution'), scope='document')
            online = SubElement(distribution, _make_tag('online'))
            url = SubElement(online, _make_tag('url'), function='information')
            url.text = publisher.websiteUrl
        geographic = choose(primary.geographicCoverage, context.get_default('geographicCoverage'), context.get_default('country'))
        taxonomic = choose(primary.taxonomicCoverage, context.get_default('taxonomicCoverage'))
        if geographic is not None or taxonomic is not None:
            coverage = SubElement(dataset, _make_tag('coverage'))
            if geographic is not None:
                geographicCoverage = SubElement(coverage, _make_tag('geographicCoverage'))
                geographicDescription = SubElement(geographicCoverage, _make_tag('geographicDescription'))
                geographicDescription.text = geographic
            if taxonomic is not None:
                taxonomicCoverage = SubElement(coverage, _make_tag('taxonomicCoverage'))
                generalTaxonomicCoverage = SubElement(taxonomicCoverage, _make_tag('generalTaxonomicCoverage'))
                generalTaxonomicCoverage.text = taxonomic
        self.addOrganisation(publisher, 'contact', dataset)

    def addAdditionalMetadata(self, primary: Record, publisher: Record, identifier: str, timestamp: datetime.datetime, context: ProcessingContext, parent: Element):
        additionalMetadata = SubElement(parent, _make_tag('additionalMetadata'))
        metadata = SubElement(additionalMetadata, _make_tag('metadata'))
        gbif = SubElement(metadata, _make_tag('gbif'))
        dateStamp = SubElement(gbif, _make_tag('dateStamp'))
        dateStamp.text = timestamp.strftime('%FT%T%Z')
        citation = SubElement(gbif, _make_tag('citation'), identifier=identifier)
        cites = []
        if primary.citation:
            cites.append(primary.citation)
        else:
            cites.append(primary.name)
            if primary.organisation is not None:
                cites.append(primary.organisation)
        if publisher is not None and publisher.organisation is not None:
            cites.append(publisher.organisation)
        if primary.lastUpdated is not None:
            cites.append(primary.lastUpdated.strftime('%F'))
        citation.text= ', '.join(cites)

    def addOrganisation(self, metadata: Record, tag: str, parent: Element):
        if metadata is None or metadata.organisation is None:
            return
        details = SubElement(parent, _make_tag(tag))
        organizationName = SubElement(details, _make_tag('organizationName'))
        organizationName.text = metadata.organisation
        if metadata.street is not None or metadata.postBox is not None:
            address = SubElement(details, _make_tag('addrress'))
            deliveryPoint = SubElement(address, _make_tag('deliveryPoint'))
            deliveryPoint.text = metadata.postBox if metadata.postBox is not None else metadata.street
            if metadata.city is not None:
                city = SubElement(address, _make_tag('city'))
                city.text = metadata.city
            if metadata.state is not None:
                administrativeArea = SubElement(address, _make_tag('administrativeArea'))
                administrativeArea.text = metadata.state
            if metadata.postcode is not None:
                postalCode = SubElement(address, _make_tag('postalCode'))
                postalCode.text = metadata.postcode
            if metadata.country is not None:
                country = SubElement(address, _make_tag('country'))
                country.text = metadata.count
        if metadata.email is not None:
            electronicMailAddress = SubElement(details, _make_tag('electronicMailAddress'))
            electronicMailAddress.text = metadata.email
        if metadata.websiteUrl is not None:
            onlineUrl = SubElement(details, _make_tag('onlineUrl'))
            onlineUrl.text = metadata.websiteUrl

    def addCopyright(self, metadata: Record, timestamp: datetime.datetime, parent: Element):
        copyright = metadata.rights
        if copyright is None and metadata.organisation is not None:
            copyright = "Copyright " + timestamp.strftime("%Y") + ", " + metadata.organisation
        if copyright is None:
            return
        if metadata.licence is not None:
            copyright += ' ' + metadata.licence
        self.addPara(copyright, parent)

    def addPara(self, text: str, parent: Element):
        text = normalise_spaces(text)
        if text is None:
            return
        para = SubElement(parent, _make_tag('para'))
        para.text = text
