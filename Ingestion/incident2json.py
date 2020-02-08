from pathlib import Path
import xmltodict
import json
import pprint

filePath = Path(r'C:\Users\z003pwpn\git\railwaysuk\Ingestion\data\sample_incident.xml')
outPath  = Path(r'C:\Users\z003pwpn\git\railwaysuk\Ingestion\data\sample_incident.json')

def xmlToJson(filePath):
    with open(filePath) as fd:
        doc = xmltodict.parse(fd.read())
    return json.dumps(doc)

# Print json results
#pp = pprint.PrettyPrinter(indent=4)
#pp.pprint(json.dumps(doc))    

print(xmlToJson(filePath))

# save parsed file to disk
with open(filePath) as in_file:
    xml = in_file.read()
    with open(outPath, 'w') as out_file:
        json.dump(xmltodict.parse(xml), out_file)