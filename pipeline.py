import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
#  all the components of the command line 
from sys import argv

# python3 pipeline.py -goodbye 'Cheerio' 
# Output: ['pipeline.py', '-goodbye', 'Cheerio']
# print(argv) 

PROJECT_ID = 'Your_Project_ID'
SCHEMA = 'sr:INTEGER,abv:FLOAT,id:INTEGER,name:STRING,style:STRING,ounces:FLOAT'

"""
import pandas as pd

data = pd.read_csv("beers.csv", index_col=[0])
print(data.columns)
print(data.head(5))
"""

# Get data without missing values
def discard_miss(data):
    return len(data['abv'])>0 and len(data['id']) > 0 and len(data['name']) > 0 and len(data['style']) > 0

# Data transformations
def convert_types(data):
    data['abv'] = float(data['abv']) if 'abv' in data else None
    data['id'] = int(data['id']) if 'id' in data else None
    data['name'] = str(data['name']) if 'name' in data else None
    data['style'] = str(data['style']) if 'style' in data else None
    data['ounces'] = float(data['ounces']) if 'ounces' in data else None
    return data

# Drop unwanted variables from dataset
def del_unwanted_cols(data):
    del data['ibu']
    del data['brewery_id']
    return data


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    known_args = parser.parse_known_args(argv)
    # print(known_args)
    p = beam.Pipeline(options=PipelineOptions())

    (p | 'ReadData' >> beam.io.ReadFromText('gs://pipeline-beamm/batch/beers.csv', skip_header_lines=1)
       | 'SplitData' >> beam.Map(lambda x: x.split(','))
       | 'FormatToDict' >> beam.Map(lambda x: {'sr': x[0], 'abv': x[1], "ibu": x[2], "id": x[3], "name": x[4], "style": x[5], "brewery_id": x[6], "ounces": x[7]}) 
       | 'DeleteIncompleteData' >> beam.Filter(discard_miss)
       | 'ChangeDataType' >> beam.Map(convert_types)
       | 'DeleteUnwantedData' >> beam.Map(del_unwanted_cols)
       | 'WriteToBigQuery' >> beam.io.WriteToBigQuery('{0}:beer.beer_data'.format(PROJECT_ID), 
            schema=SCHEMA, write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    ) 

    result = p.run()
    result.wait_until_finish()



