import xml.sax

class StatisticsHandler(xml.sax.handler.ContentHandler):
    def __init__(self):
        self._result = dict()
        self._spectrum_identification_item = False

    def parse(self, f):
        """ SAX parser for mzid extracting statistics about software and identification parameters. """
        xml.sax.parse(f, self)
        return self._result

    def startElement(self, name, attrs):
        if name == "AnalysisSoftware":
            if 'name' in attrs.getNames():
                self._result['software'] = attrs.getValue('name')
            else:
                self._result['software'] = attrs.getValue('id')

        elif name == 'SpectrumIdentificationItem':
            if 'params' not in self._result:
                self._spectrum_identification_item = True

        elif self._spectrum_identification_item and name == 'cvParam':
            self._result['params'] = attrs.getValue('name')
    
    def endElement(self, name):
        if name == 'SpectrumIdentificationItem':
            self._spectrum_identification_item = False

         
if __name__ == '__main__':
    with open("/home/dan/Documents/Work/PRIDEdata/xml_handlers/kelstrup_hela-res15000_all-fractions.out.cpsx.xml") as f:
        print(StatisticsHandler().parse(f))