import os
import shutil
import tempfile
import unittest

from lxml.etree import fromstring

from dwc.meta import MetaFile, EmlFile
from dwc.schema import TaxonSchema
from processing.dataset import Port
from processing.node import ProcessingContext
from processing.sink import CsvSink


class MetaTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.workdir, True)

    def test_meta_execute_1(self):
        context = ProcessingContext("execute_1")
        context.output_dir = self.workdir
        context.work_dir = self.workdir
        schema = TaxonSchema()
        port = Port.port(schema)
        sink = CsvSink.create("sink_1", port, "test.csv", "excel")
        meta = MetaFile.create("meta_1", sink)
        meta.execute(context)
        self.assertTrue(os.path.isfile(os.path.join(self.workdir, "meta.xml")))

    def test_eml_execute_1(self):
        context = ProcessingContext("execute_1")
        context.output_dir = self.workdir
        context.work_dir = self.workdir
        ala_str = """
        <organization>
          <organizationName>The Atlas of Living Australia</organizationName>
          <address>
            <deliveryPoint>GPO Box 1700</deliveryPoint>
            <city>Canberra</city>
            <administrativeArea>ACT</administrativeArea>
            <postalCode>2601</postalCode>
            <country>Australia</country>
          </address>
          <electronicMailAddress>info@ala.org.au</electronicMailAddress>
          <onlineUrl>http://www.ala.org.au</onlineUrl>
        </organization>
        """
        ala = fromstring(ala_str)
        afd_str = """
        <metadata code="AFD" date="2018-10-03">
          <title>Australian Faunal Directory</title>
          <organization>
            <organizationName>Australian Faunal Directory</organizationName>
            <electronicMailAddress>australianfaunaldirectory@environment.gov.au</electronicMailAddress>
            <onlineUrl>http://www.environment.gov.au/biodiversity/abrs/online-resources/fauna/afd/home</onlineUrl>
          </organization>
          <description>Data from the Australian Faunal Directory (AFD) transformed into DwCA Taxon format, for loading into the Atlas of Living Australia's name indexes.</description>
          <geographicCoverage>
            <geographicDescription>Australia</geographicDescription>
          </geographicCoverage>
          <taxonomicCoverage>
            <generalTaxonomicCoverage>Covers Australian Fauna</generalTaxonomicCoverage>
            <taxonomicClassification>
              <taxonRankName>Kingdom</taxonRankName>
              <taxonRankValue>Animalia</taxonRankValue>
            </taxonomicClassification>
          </taxonomicCoverage>
          <citation>Atlas of Living Australia &amp; ABRS 2009. Australian Faunal Directory. Australian Biological Resources Study, Canberra.</citation>
        </metadata>
        """
        afd = fromstring(afd_str)
        meta = EmlFile.create("eml_1", afd, ala)
        meta.execute(context)
        self.assertTrue(os.path.isfile(os.path.join(self.workdir, "eml.xml")))


if __name__ == '__main__':
    unittest.main()
