import pandas as pd
import requests
import os
import logging
import math
import requests
import pandas as pd
import json
import numpy as np
from datetime import datetime, timedelta
import time
from tqdm import tqdm

# No need to call basicConfig here, as it's set in main.py

autho = {'Authorization': 'Bearer ' + os.environ.get('API_KEY_BZ_new')}
ItemTyper = 44
start = '*h-1h'
end = '*h'
site_list = ['MN', 'SENT', 'SNDY', 'MAV', 'SUGR', 'DFS', 'OWF', 'SO', 'SL', 'RLWEP', 'MOR', 'BLH', 'AMHST']
site_MCMS = ['MN-MCMS', 'OWF-MCMS', 'SUGR-MCMS', 'BLH-MCMS', 'RLWEP-MCMS', 'SO-MCMS', 'SL-MCMS', 'MOR-MCMS',
             'AMHST-wtgs37-MCMS', 'DFS-MCMS1', 'DFS-MCMS2', 'MAV-MCMS1', 'MAV-MCMS2', 'SNDY-MCMS1', 'SNDY-MCMS2',
             'SENT-MCMS1', 'SENT-MCMS2']
MCMS_tag = ['LWC', 'NacelleTemperature', 'NacelleTemperature2', 'Precipitation', 'RH', 'WindSpeed', 'IcingSeverity',
            'IceThickness']
MCMS_tag_AMHST = ['LiquidWaterContentEfficiency', 'Temperature1-Scaled', 'MCMS-Temperature2', 'Precipitation',
                  'RelativeHumidity-Scaled', 'WindSpeed-Scaled', 'Severity', 'IceThickness']

MeasuresList = ['KPI-PerformanceIndex', 'ActivePower', 'KPI-TheoreticalPower']  #


def AssetList_Model_from_SiteName(Site, ItemTyper):
    logging.info(f"Fetching asset list for site: {Site}")

    BF_AssetNames = []
    BF_Assettitle = []
    BF_Assetmodel = []
    BF_Request = f'https://liberty.bazefield.com/bazefield.services/api/turbines/{Site}?format=json'
    BF_Result = requests.get(BF_Request, headers=autho)

    if BF_Result.status_code != 200:
        logging.error(f"Failed to fetch asset list for site {Site}")
        return pd.DataFrame()

    BF_Res_Text = BF_Result.text
    i = 0
    while i < len(json.loads(BF_Res_Text)):
        asset = json.loads(BF_Res_Text)[i]
        if asset['itemType']['itemTypeId'] == ItemTyper and asset['site']['shortName'] == Site:
            BF_Assettitle.append(asset['title'])
            BF_Assetmodel.append(asset['model']['name'])
        i += 1

    logging.info(f"Successfully fetched asset list for site {Site}")
    return pd.DataFrame({'Assettitle': BF_Assettitle, 'Assetmodel': BF_Assetmodel})


def get_turbine_tag_data(Asset_Names, MeasuresList, start, end, interval="1h"):
    logging.info(f"Fetching turbine data for assets: {Asset_Names}")

    data_full = pd.DataFrame()
    tags = []
    for turbine in Asset_Names:
        tags += [f'{turbine}-{t}' for t in MeasuresList]

    max_tags = 30
    loops = math.ceil(len(tags) / max_tags)
    results_df = None

    try:
        if loops > 1:
            logging.info(f"Querying Bazefield in {loops} batches")
            first, last = 0, max_tags
            for i in tqdm(range(loops)):
                table = tags[first:last]
                url = f"https://liberty.bazefield.com/BazeField.Services/api/measurements/{','.join(table)}/aggregates/AVERAGE/from/{start}/to/{end}/interval/{interval}?format=json"
                result = requests.get(url, headers=autho).json()
                if i == 0:
                    results_df = pd.DataFrame(result['timeSeriesList'])
                else:
                    tmp = pd.DataFrame(result['timeSeriesList'])
                    results_df = pd.concat([results_df, tmp]).reset_index(drop=True)

                first = last
                last = min(last + max_tags, len(tags))
        else:
            url = f"https://liberty.bazefield.com/BazeField.Services/api/measurements/{','.join(tags)}/aggregates/AVERAGE/from/{start}/to/{end}/interval/{interval}?format=json"
            logging.info(f"Querying Bazefield for tags: {tags}")
            result = requests.get(url, headers=autho).json()
            results_df = pd.DataFrame(result['timeSeriesList'])

        logging.info("Processing turbine data")
        return_df = pd.DataFrame()
        for i in tqdm(range(len(results_df))):
            tmp_df = pd.DataFrame(results_df["timeSeries"][i])
            tmp_df.drop(["q"], axis=1, inplace=True)
            tmp_df.rename({"v": results_df['measurementName'][i]}, axis=1, inplace=True)
            if i == 0:
                return_df = tmp_df.copy()
            else:
                return_df = pd.merge(return_df, tmp_df, how="outer", on=["t_local", "t"])

        return_df.rename({"t": "DateTimeUTC", "t_local": "DateTimeLocal"}, axis=1, inplace=True)
        return_df['DateTimeUTC'] = pd.to_datetime(return_df['DateTimeUTC'], unit='ms')

        json_file = "site_params.json"
        site_code = Asset_Names[0].split(sep='-')[0]
        with open(json_file, 'r') as f:
            site_data = json.load(f)

        site_info = site_data["sites"].get(site_code)
        if not site_info:
            logging.error(f"Site {site_code} not found in JSON file.")
            raise Exception(f"Site {site_code} not found in JSON file.")

        tz = site_info["tz"]
        dst_observed = site_info["DSTobserved"]
        if dst_observed:
            tz += 1

        return_df['DateTimeLocal'] = return_df['DateTimeUTC'] + timedelta(hours=tz)
        cols_to_move = ['DateTimeUTC', 'DateTimeLocal']
        return_df = return_df[cols_to_move + [col for col in return_df.columns if col not in cols_to_move]]

        logging.info(f"Successfully fetched and processed turbine data for {Asset_Names}")
        return return_df

    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch turbine data: {e}")
        raise e


def turbine_1h_data(SiteName):
    logging.info(f"Fetching 1-hour turbine data for site {SiteName}")
    Asset_info = AssetList_Model_from_SiteName(SiteName, ItemTyper)
    Asset_Names = Asset_info['Assettitle'].to_list()
    return get_turbine_tag_data(Asset_Names, MeasuresList, start, end, interval="1h")


def MCMS_get_data(SiteName):
    logging.info(f"Fetching MCMS data for site {SiteName}")
    if SiteName == 'AMHST':
        MCMS_output = get_turbine_tag_data(['AMHST-wtgs37-MCMS'], MCMS_tag_AMHST, start, end, interval='1h')
    else:
        sensor_list = [item for item in site_MCMS if SiteName in item]
        MCMS_output = get_turbine_tag_data(sensor_list, MCMS_tag, start, end, interval='1h')

    logging.info(f"Successfully fetched MCMS data for {SiteName}")
    return MCMS_output
