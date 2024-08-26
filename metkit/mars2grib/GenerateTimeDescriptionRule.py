#!/bin/env python3

# TODO handle origin key

import os
import sys
from collections import OrderedDict

import sys

import yaml
from yaml.loader import SafeLoader
import json


def printDatabaseInformation():
    import pymysql
    with pymysql.connect(host="webapps-db-prod", database="param", user="ecmwf_ro", password="ecmwf_ro") as conn:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute("""DESCRIBE grib_encoding""")
            rows = cur.fetchall()
        
        print(rows)


def fetchStatisticalParamIds():
    import pymysql
    with pymysql.connect(host="webapps-db-prod", database="param", user="ecmwf_ro", password="ecmwf_ro") as conn:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
#             cur.execute("""
# SELECT 
#     DISTINCT grib_encoding.param_id as paramId, 
#              grib.attribute_value as typeOfStatisticalProcessing 
# FROM 
#     grib 
#     join grib_encoding on grib_encoding.id=grib.encoding_id 
# WHERE grib.attribute_id = (select id from attribute where name = \"typeOfStatisticalProcessing\") 
#   and grib.attribute_value is not NULL
# """)
            cur.execute("""
SELECT 
    DISTINCT grib_encoding.param_id as paramId, 
             grib_encoding.param_version,
             typeOfStat.typeOfStatisticalProcessing,
             lenTimeRange.lengthOfTimeRange,
             unitTimeRange.indicatorOfUnitForTimeRange
FROM 
    grib_encoding
    INNER JOIN (SELECT encoding_id, attribute_value as typeOfStatisticalProcessing 
        FROM grib 
        WHERE grib.attribute_id = (select id from attribute where name = \"typeOfStatisticalProcessing\") 
      AND grib.attribute_value is not NULL) typeOfStat 
      ON grib_encoding.id=typeOfStat.encoding_id 
    LEFT JOIN (SELECT encoding_id, attribute_value as lengthOfTimeRange
        FROM grib 
        WHERE grib.attribute_id = (select id from attribute where name = \"lengthOfTimeRange\")) lenTimeRange
      ON grib_encoding.id=lenTimeRange.encoding_id 
    LEFT JOIN (SELECT encoding_id, attribute_value as indicatorOfUnitForTimeRange
        FROM grib 
        WHERE grib.attribute_id = (select id from attribute where name = \"indicatorOfUnitForTimeRange\")) unitTimeRange
      ON grib_encoding.id=unitTimeRange.encoding_id 
""")
            rows = cur.fetchall()

    index = OrderedDict()
    keyCol = "paramId"
    for row in rows:
        key = row[keyCol]
        if key not in index:
            index[key] = []
        index[key].append({k: v for k, v in row.items() if k != keyCol})
        
   
    statisticalParamIds = {}
    for key, vals in sorted(index.items()):
        # paramId=key, typeOfStatisticalProcessing=vals
        # Dump only paramIds as a list
        statisticalParamIds[key] = max(vals, key = lambda d: d["param_version"])
        del statisticalParamIds[key]["param_version"]
        for k in list(statisticalParamIds[key].keys()):
            if (statisticalParamIds[key][k] is None):
                del statisticalParamIds[key][k]
    
    return statisticalParamIds
        
def fetchStatisticalParamIdsLocal(fname = "./statistical-paramids.yaml"):
    with open(fname) as f:
        return yaml.load(f, Loader=SafeLoader)



def main():
    statisticalParamIds = fetchStatisticalParamIds()
    # statisticalParamIds = fetchStatisticalParamIdsLocal()
        
    
    # For some paramId decision is made in combiantion with stream and origin - that should be put here
    specialHandlingForParamId  = {}
    
    # Example
    # specialHandlingForParamId[123] = {
    #     "key": "stream",
    #     "dict": "initial",
    #     "default": {
    #         "write-work": {
    #             "timeExtent": "timeRange"
    #         }
    #     },
    #     "value-map": {} # PUT in stream general stuff
    # }
    

    # TODO handle other products like satellite which have no timeExtent. Possibly have to evaluate stream for that
    pointInTime = {
        "write-work": {
                    "timeExtent": "pointInTime"
        }
    }
    timeRange = lambda more: {
        "write-work": {
                    "timeExtent": "timeRange",
                    **more
        }
    }
    
    # Try to reuse existing dictionaries - allows YAML to create anchors and reduce file size
    dictRepo = {}
    def getDict(myDict):
        hash = json.dumps(myDict, sort_keys=True)
        if hash not in dictRepo.keys():
            dictRepo[hash] = myDict
        return dictRepo[hash]
    
    
    timeExtentRule = {
        "key": "paramId",
        "dict": "initial",
        "default": {
            "key": "stream",
            "dict": "initial",
            "default": getDict(pointInTime),
            "value-map": {} # PUT in stream general stuff
        },
        "value-map": { 
            "{}".format(paramId): (getDict(specialHandlingForParamId[paramId])
                if paramId in specialHandlingForParamId.keys() else getDict(timeRange(more))
                ) for paramId,more in statisticalParamIds.items()
        }
    }
    

    with open("rules/TimeDescription.yaml", "w") as f:
        f.write(
            "# File automatically generated by %s\n# Do not edit\n\n"
            % (os.path.basename(__file__))
        )
        f.write(yaml.safe_dump(timeExtentRule, default_flow_style=False, sort_keys=False))


if __name__ == "__main__":
    main()

