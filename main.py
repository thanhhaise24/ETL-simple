import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "dealership_logfile.txt"
target_file = "dealership_transformed_data.csv"

# CSV Extract Function
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

# JSON Extract Function
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines=True)
    return dataframe

# XML Extract Function
def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=['car_model','year_of_manufacture','price','fuel'])

    tree = ET.parse(file_to_process)

    root = tree.getroot()

    for person in root:
        car_model = person.find("car_model").text
        year_of_manufacture = int(person.find("year_of_manufacture").text)
        price = float(person.find("price").text)
        fuel = person.find("fuel").text

        dataframe = dataframe.append({"car_model":car_model, "year_of_manufacture":year_of_manufacture,
                                      "price":price, "fuel":fuel}, ignore_index=True)

    return dataframe

# Extract Function
def extract():
    extracted_data = pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])
    # csv file
    for csv_file in glob.glob("dealership/*.csv"):
        extracted_data = extracted_data.append(extract_from_csv(csv_file), ignore_index=True)
    # json file
    for json_file in glob.glob("dealership/*.json"):
        extracted_data = extracted_data.append(extract_from_json(json_file), ignore_index=True)
    # xml file
    for xml_file in glob.glob("dealership/*.xml"):
        extracted_data = extracted_data.append(extract_from_xml(xml_file), ignore_index=True)

    return extracted_data

# Transform
def transform(data):
    data['price'] = round(data.price, 2)
    return data

# Load
def load(target_file, data_to_load):
    data_to_load.to_csv(target_file)

def log(msg):
    timestamp_format= '%H:%M:%S-%h-%d-%Y'
    now=datetime.now()
    timestamp=now.strftime(timestamp_format)
    with open('logfile.txt','a') as f:
        f.write(timestamp+', '+msg+'\n')

if __name__ == '__main__':
    log('ETL Job Started')

    log("Extract phase Started")
    extracted_data = extract()
    log("Extract phase Ended")

    log("Transform phase Started")
    transformed_data =  transform(extracted_data)
    log("Transform phase Ended")

    log("Load phase Started")
    load(target_file, transformed_data)
    log("Load phase Ended")

