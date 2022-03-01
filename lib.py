# coding=utf-8
import tornado.gen
import tornado.httpclient
from tornado.ioloop import IOLoop
import arrow
import copy
import time
import json
from pymongo.errors import BulkWriteError

from tests import base
from utils import sync
from utils import mongo_tender, mongo, store_external

import constants
from models import notifications, mongo_sync, mongo, mongo_ep, elasticsearch, mongo_ep_sync


@tornado.gen.coroutine
def course_project_parsing_2(q=None):
    if not q:
        q = {}

    ocids = []

    import csv

    with open("14700_tenders.csv", newline='') as csvf:
        # ocid,category,criteria,budget,value,comments,lots,bids,docs,items,cpv,idno,method
        reader = csv.DictReader(csvf, fieldnames=[
            'ocid', 'category', 'criteria', 'budget', 'value', 'comments', 'lots', 'bids', 'docs', 'items', 'cpv',
            'idno', 'method',
        ], restkey='residual', restval="-", delimiter=",", quotechar='"', escapechar="\\", doublequote=True)

        for row in reader:
            for k, v in row.items():
                if v == "ocid":
                    break

                if k == "ocid":
                    ocids.append(v)
                    break

    _skip = 0
    _take = 500

    q = {"id": {"$in": ocids}}
    ids = {}
    tenders = yield mongo.tenders.find(q, projection=["_id", "id"]
                                       ).skip(_skip).limit(_take).to_list(length=_take)
    while tenders:
        for tender in tenders:
            ids.update({tender["_id"]: tender['id']})

        _skip += _take
        tenders = yield mongo.tenders.find(q, projection=["_id", "id"]
                                           ).skip(_skip).limit(_take).to_list(length=_take)

        print(f"tenders: {_skip} of {len(ocids)}")

    q = {'parent_id': {'$in': list(ids.keys())}}

    _skip = 0
    _take = 500

    records = []

    bids = yield mongo.bids.find(q).skip(_skip).limit(_take).to_list(length=_take)
    while bids:
        for bid in bids:
            records.append(
                (
                    f"{bid['tenderers'][0].get('identifier', {}).get('id', '-') if bid.get('tenderers') else '-'},"
                    f"{bid.get('date', '-')},"
                    f"{bid['lot_value']['amount'] if bid.get('lot_value') else 0},"
                    f"{bid['value']['amount'] if bid.get('value') else 0},"
                    f"{1 if bid.get('auction_start_timestamp') or bid.get('lot_auctionPeriod') else 0},"
                    f"{bid.get('tender_procuringEntity', {}).get('identifier', {}).get('id', '-')},"
                    f"{ids.get(bid['parent_id'])},"
                    f"{len(bid.get('documents', []))},"
                    f"{1 if bid.get('award_statusDetails', '') == 'active' else 0}"
                )
            )
            # idno,date,lot_value,bid_value,auction,pe_idno,ocid,docs,win

        _skip += _take
        bids = yield mongo.bids.find(q).skip(_skip).limit(_take).to_list(length=_take)

        print(f"bids {_skip}")

    with open('bids.csv', 'w') as ff:
        print("idno,date,lot_value,bid_value,auction,pe_idno,ocid,docs,win", file=ff)
        for d in records:
            print(d, file=ff)

            
def course_project_parsing_1(q=None):
    data = []

    _skip = 0
    _take = 500

    if not q:
        q = {}
    res = mongo.tenders.find(q).limit(_take).skip(_skip)
    while res:
        for tender in res:
            # ocid,category,criteria,budget,value,comments,lots,bids,docs,items,cpv,idno,method
            data.append(
                (f"{tender['ocid']},"
                 f"{tender['tender']['mainProcurementCategory']},"
                 f"{tender['tender']['awardCriteria']},"
                 f"{tender['planning']['budget']['amount']['amount']},"
                 f"{tender['tender']['value']['amount']},"
                 f"{len(tender['tender'].get('enquiries', []))},"
                 f"{len(tender['tender'].get('lots', []))},"
                 f"{len(tender['tender'].get('bids', []))},"
                 f"{len(tender['tender'].get('documents', []))},"
                 f"{len(tender['tender'].get('items', []))},"
                 f"{tender['tender']['classification']['id']},"
                 f"{tender['tender']['procuringEntity']['identifier']['id'] if tender['tender']['procuringEntity'].get('identifier') else tender['tender']['procuringEntity']['id']},"
                 f"{tender['tender']['procurementMethod']}")
                )

        ret = res.retrieved
        if not ret:
            break

        _skip += _take
        print('%s - %s' % (ret, _skip))

        res = mongo.tenders.find(q).limit(_take).skip(_skip)

    with open('14700_tenders.csv', 'w') as f:
        print('writing file...')
        f.write('ocid,category,criteria,budget,value,comments,lots,bids,docs,items,cpv,idno,method\n')
        f.writelines([e + "\n" for e in data])
