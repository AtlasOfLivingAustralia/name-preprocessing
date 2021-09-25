import re
from typing import List

import attr

from afd.schema import FormattedPublicationSchema, FormattedReferenceSchema
from processing.dataset import Port, Record, Keys
from processing.node import ProcessingContext
from processing.transform import LookupTransform

# Types of publication
PUBLICATION_JOURNAL = "publication.type.J"
PUBLICATION_BOOK =  "publication.type.B"
PUBLICATION_CHAPTER_IN_BOOK =  "publication.type.C"
PUBLICATION_MISC =  "publication.type.M"
PUBLICATION_ARTICLE_IN_JOURNAL =  "publication.type.P"
PUBLICATION_SECTION_IN_ARTICLE =  "publication.type.S"
PUBLICATION_THESIS =  "publication.type.T"
PUBLICATION_URL =  "publication.type.U"

PAGE_RANGE = re.compile("^\\d+-\\d+")

@attr.s
class PublicationTransform(LookupTransform):
    """Convert the AFD publication into a simple formatted reference"""

    @classmethod
    def create(cls, id: str, input: Port, input_keys, lookup_keys, **kwargs):
        output = Port.port(FormattedPublicationSchema())
        input_keys = Keys.make_keys(input.schema, input_keys)
        lookup_keys = Keys.make_keys(input.schema, lookup_keys)
        return PublicationTransform(id, input, input, output, None, input_keys, lookup_keys, **kwargs)

    def _get_part(self, record: Record, parent: Record, key: str) -> str:
        val = None
        if record is not None:
            val = record.data.get(key)
        if val is None and parent is not None:
            val = parent.data.get(key)
        if val is None:
            return None
        vs: str = str(val)
        if vs is None:
            return
        vs = vs.strip()
        if len(vs) == 0:
            return None
        return vs

    def _build(self, parts: List[str], value: str, sep: str = " ", begin: str = None, end: str = None):
        if value is None:
            return
        if len(parts) > 0 and sep is not None:
            parts.append(sep)
        if begin is not None:
            parts.append(begin)
        parts.append(value)
        if end is not None:
            parts.append(end)

    def compose(self, record: Record, parent: Record, context: ProcessingContext, additional):
        parts = []
        type = self._get_part(record, parent, 'TYPE')
        author = self._get_part(record, parent, 'AUTHOR')
        parent_author = self._get_part(parent, None, 'AUTHOR')
        year = self._get_part(record, parent, 'YEAR')
        title = self._get_part(record, parent, 'STRIPPED_TITLE')
        parent_title = self._get_part(parent, None, 'STRIPPED_TITLE')
        editor = None
        publication = None
        abbrev = self._get_part(record, parent, 'ABBREV')
        parent_abbrev = self._get_part(parent, None, 'ABBREV')
        series = self._get_part(record, parent, 'SERIES')
        volume = self._get_part(record, parent, 'VOLUME')
        part = self._get_part(record, parent, 'PART')
        pages: str = self._get_part(record, parent, 'PAGES')
        edition =  self._get_part(record, parent, 'EDITION')
        publisher = self._get_part(record, parent, 'PUBLISHER')
        place = self._get_part(record, parent, 'PLACE')
        doi = self._get_part(record, parent, 'DOI')
        source = self._get_part(record, None, 'CITE_AS')
        if type == PUBLICATION_JOURNAL:
            publication = title
            title = None
        elif type == PUBLICATION_BOOK:
            publication = title
            title = None
        elif type == PUBLICATION_CHAPTER_IN_BOOK:
            publication = parent_title if parent_title is not None else parent_abbrev
            editor = parent_author
            abbrev = None
        elif type == PUBLICATION_ARTICLE_IN_JOURNAL:
            publication = parent_title if parent_title is not None else parent_abbrev
            abbrev = None
        elif type == PUBLICATION_SECTION_IN_ARTICLE:
            full = []
            self._build(full, title)
            self._build(full, parent_title, " in ")
            title = ''.join(parts)
        if publication is None and abbrev is not None:
            publication = abbrev
            abbrev = None
        self._build(parts, author)
        self._build(parts, year)
        self._build(parts, title, ", ", '"', '"')
        self._build(parts, editor, ", ", 'Ed. ')
        self._build(parts, publication, ", ")
        self._build(parts, abbrev, " ", "(", ")")
        self._build(parts, series, ", ", 'ser. ')
        self._build(parts, volume, ", ", 'vol. ')
        self._build(parts, part, ", ", 'no. ')
        self._build(parts, pages, ", ")
        self._build(parts, edition, ", ", None, "ed.")
        self._build(parts, publisher, ", ")
        self._build(parts, place, ", ")
        self._build(parts, doi, ", ", "doi:")
        formatted = Record(record.line, {
            'PUBLICATION_ID': record.data['PUBLICATION_ID'],
            'namePublishedInYear': year,
            'namePublishedIn': ''.join(parts),
            'namePublishedInID': doi,
            'source': source
        }, None)
        return formatted

class ReferenceTransform(LookupTransform):
    """Link publication data to references"""

    @classmethod
    def create(cls, id: str, references: Port, publications: Port, input_keys, lookup_keys, **kwargs):
        output = Port.port(FormattedReferenceSchema())
        input_keys = Keys.make_keys(references.schema, input_keys)
        lookup_keys = Keys.make_keys(publications.schema, lookup_keys)
        return ReferenceTransform(id, references, publications, output, None, input_keys, lookup_keys, **kwargs)

    def compose(self, reference: Record, publication: Record, context: ProcessingContext, additional):
       ref: str = publication.namePublishedIn
       page: str = reference.PAGES
       qualification: str = reference.QUALIFICATION
       if page is not None:
           if not page.startswith('p'):
               page = ("pp" if PAGE_RANGE.match(page) else "p") + page
           ref = ref + " " + page
       if qualification is not None:
           ref = ref + " (" + qualification + ")"
       formatted = Record(reference.line, {
           'OBJECT_ID': reference.OBJECT_ID,
           'REFERENCE_ID': reference.REFERENCE_ID,
           'PUBLICATION_ID': publication.PUBLICATION_ID,
           'namePublishedInYear': publication.namePublishedInYear,
           'namePublishedIn': ref,
           'namePublishedInID': publication.namePublishedInID,
           'source': publication.data.get('source'),
       }, None)
       return formatted
