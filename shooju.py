#That's a project for Shooju Company
#https://www.jodidata.org/_resources/files/downloads/gas-data/jodi_gas_csv_beta.zip
#Downloads, unzips, parses based on that single URL input
#Writes to stdout a new line delimited (\n) JSON series-by-series representation of the input CSV
#3 keys
#series_id ->
#points
#fields
import urllib2
import zipfile
import csv
import json
import itertools
import pprint

from datetime import datetime
startTime = datetime.now()

url = "https://www.jodidata.org/_resources/files/downloads/gas-data/jodi_gas_csv_beta.zip"

r = urllib2.urlopen(url).read()
with open("jodi_gas.zip", 'wb') as code:
    code.write(r)

csv_file = zipfile.ZipFile('jodi_gas.zip').extractall()

def createUniqueSet(reader,key):
    newset = set()
    for row in reader:
        newset.add(row[key])
    return newset

def createUniqueListBySet(reader, keyPairs):
    uniqueList = []
    a = []
    for keypair in keyPairs:
        for row in list(reader):
            #print keypair[0], keypair[1], keypair[2], keypair[3]
            if row['FLOW_BREAKDOWN'] == keypair[0] and row['REF_AREA'] == keypair[1] and row['ENERGY_PRODUCT'] == keypair[2] and row['UNIT_MEASURE'] == keypair[3]:
                a.append(row)
                reader.remove(row)
        if a != [] :
            #print 'pushing array with: ' + str(len(a)) + " items."
            print createSeries(a), '\n' 
            uniqueList.append(a)
            a = []
        
    return uniqueList

def createKeyPairs(COUNTRIES, FLOW_BREAKDOWN, ENERGY_PRODUCT, UNIT_MEASURE):
    bigList = []
    a = []
    for country in COUNTRIES:
        for flow in FLOW_BREAKDOWN:
            for energy in ENERGY_PRODUCT:
                for measure in UNIT_MEASURE:
                    a.append(country)
                    a.append(flow)
                    a.append(energy)
                    a.append(measure)
                    bigList.append(a)
                    a = []
    return bigList

def createSeries(arr):
    item = arr[0]
    points = []
    series_id = item['REF_AREA'] + "_" + item['FLOW_BREAKDOWN'] #+ "_" + item['UNIT_MEASURE'] + "_"+ item['ENERGY_PRODUCT']
    country = item['REF_AREA']
    metric = item['FLOW_BREAKDOWN']
    #energy_product = item['ENERGY_PRODUCT']
    #unit_measure = item['UNIT_MEASURE']

    
    for item in arr:
        a = []
        a.append(item['TIME_PERIOD'] + '-01')
        a.append(float(item['OBS_VALUE']))
        points.append(a)

    json_Series = {
        "series_id": series_id,
        "points": points,
        "fields":{
            "country":country,
            "metric":metric
            #'energy_product': energy_product,
            #'unit_measure': unit_measure
        }
    }

    return json_Series

series = []

with open('jodi_gas_beta.csv') as csvfile:
    
    reader = list(csv.DictReader(csvfile))

    COUNTRIES = createUniqueSet(reader, "REF_AREA")
    FLOW_BREAKDOWN = createUniqueSet(reader, "FLOW_BREAKDOWN")
    UNIT_MEASURE = createUniqueSet(reader, 'UNIT_MEASURE')
    ENERGY_PRODUCT = createUniqueSet(reader, 'ENERGY_PRODUCT')
    

    for country in COUNTRIES:
        for flow in FLOW_BREAKDOWN:
            iterator = list(itertools.ifilter(lambda x: x['REF_AREA'] == country and x['FLOW_BREAKDOWN'] == flow, reader))
            if len(iterator) > 0:
                a = createSeries(iterator)
                print json.dumps(a, indent=4) , '\n'
                series.append(a) 
    



    #keyPairs = createKeyPairs(COUNTRIES,FLOW_BREAKDOWN, ENERGY_PRODUCT, UNIT_MEASURE)
    #uniqueList = createUniqueListBySet(reader, keyPairs)
    
with open('myjsonfile_2.json', 'a') as myfile:
    print json.dump(a, myfile, sort_keys=True, indent=4, separators=(',', ': ')) 
print len(series)
print datetime.now() - startTime