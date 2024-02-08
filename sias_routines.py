import sys
from os import path, environ
from flask import Flask, request
import psycopg2
from psycopg2.extras import RealDictCursor
import json
import argparse
import logging
import subprocess
import datetime as dt

def get_slitmask_data_for_sias():
    """
    """

    output = {"status":None}

    queryType = request.args.get('type')
    startDate = request.args.get('date1')
    endDate   = request.args.get('date2')

    # Verify that all three parameters exist
    if queryType not in ['1','2'] or not startDate or not endDate:
        output['status'] = 'PARAMETER_ERROR'
        return json.dumps(output)

    # Verify correct format for dates
    try:
        test = dt.datetime.strptime(startDate, '%b %d %Y')
        test = dt.datetime.strptime(endDate, '%b %d %Y')
    except:
        output['status'] = 'PARAMETER_DATE_ERROR'
        return json.dumps(output)

    if queryType == '1':
        query = "select b.date_use,c.maskid,b.guiname,a.instrume,d.lastnm,d.firstnm,b.bluid \
                 from MaskDesign a, MaskBlu b, Mask c, observers d \
                 where date_use>=%s and date_use<=%s and \
                 (b.status<9 or b.status is null) and \
                 c.bluid=b.bluid and \
                 a.desid=b.desid and \
                 d.obid=b.blupid \
                 order by date_use"
    if queryType == '2':
        query = "select b.date_use,b.guiname,a.instrume,c.lastnm,c.firstnm,b.bluid \
                 from MaskDesign a, MaskBlu b, observers c \
                 where date_use>=%s and date_use<=%s and \
                 (b.status<9 or b.status is null) and \
                 a.desid=b.desid and \
                 c.obid=b.blupid \
                 and not exists (select * from Mask where bluid=b.bluid) \
                 order by date_use"

    cursor.execute(query, (startDate, endDate))
    result = cursor.fetchall()

    output['query'] = query
    output['length'] = len(result)

    data = []
    for row in result:
        entry = {}
        for key in row.keys():
            if 'date' in key:
                entry[key] = row[key].strftime('%b %d %Y %M:%S')
            else:
                entry[key] = row[key]
        data.append(entry)

    output['status'] = 'COMPLETE'
    output['data']   = data

    return json.dumps(output)

