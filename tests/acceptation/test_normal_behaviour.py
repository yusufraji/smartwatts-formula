# Copyright (c) 2022, INRIA
# Copyright (c) 2022, University of Lille
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * Neither the name of the copyright holder nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

"""
Run smartwatts on a mongodb database that contain 10 hwpc report per target :
- all
- mongodb
- influxdb
- sensor

as the model can't fit with 10 report , it should only return power report for the entire system containing RAPL data

We test if smartwatts return 5 powerReport for rapl target
"""
from datetime import datetime
import pytest

import pymongo
from powerapi.supervisor import SIMPLE_SYSTEM_IMP

from smartwatts.__main__ import run_smartwatts
from smartwatts.test_utils.reports import smartwatts_timeline

from powerapi.test_utils.actor import shutdown_system
from powerapi.test_utils.db.mongo import mongo_database
from powerapi.test_utils.db.mongo import MONGO_URI, MONGO_INPUT_COLLECTION_NAME, MONGO_OUTPUT_COLLECTION_NAME, MONGO_DATABASE_NAME


def check_db():
    mongo = pymongo.MongoClient(MONGO_URI)
    c_input = mongo[MONGO_DATABASE_NAME][MONGO_INPUT_COLLECTION_NAME]
    c_output = mongo[MONGO_DATABASE_NAME][MONGO_OUTPUT_COLLECTION_NAME]

    assert c_output.count_documents({}) == (c_input.count_documents({}) / 4) - 5

    for report in c_input.find({'target': 'all'})[:5]:
        ts = datetime.strptime(report['timestamp'], "%Y-%m-%dT%H:%M:%S.%f")
        query = {'timestamp': ts, 'sensor': report['sensor'],
                 'target': 'rapl'}
        assert c_output.count_documents(query) == 1



def check_db_real_time():
    mongo = pymongo.MongoClient(MONGO_URI)
    c_input = mongo[MONGO_DATABASE_NAME][MONGO_INPUT_COLLECTION_NAME]
    c_output = mongo[MONGO_DATABASE_NAME][MONGO_OUTPUT_COLLECTION_NAME]

    assert c_output.count_documents({}) == (c_input.count_documents({}) / 4) - 2

    for report in c_input.find({'target': 'all'})[:5]:
        ts = datetime.strptime(report['timestamp'], "%Y-%m-%dT%H:%M:%S.%f")
        query = {'timestamp': ts, 'sensor': report['sensor'],
                 'target': 'rapl'}
        assert c_output.count_documents(query) == 1



@pytest.fixture
def mongodb_content(smartwatts_timeline):
    return smartwatts_timeline

def test_normal_behaviour(mongo_database, shutdown_system):
    config = {'verbose': True,
              'stream': False,
              'actor_system': SIMPLE_SYSTEM_IMP,
              'input': {'puller_mongodb': {'type': 'mongodb',
                                           'model': 'HWPCReport',
                                           'uri': MONGO_URI,
                                           'db': MONGO_DATABASE_NAME,
                                           'collection': MONGO_INPUT_COLLECTION_NAME}},
              'output': {'power_pusher': {'type': 'mongodb',
                                          'model': 'PowerReport',
                                          'uri': MONGO_URI,
                                          'db': MONGO_DATABASE_NAME,
                                          'collection': MONGO_OUTPUT_COLLECTION_NAME},
                         'formula_pusher': {'type': 'mongodb',
                                            'model': 'FormulaReport',
                                            'uri': MONGO_URI,
                                            'db': MONGO_DATABASE_NAME,
                                            'collection': 'test_result_formula'}},
              'disable-cpu-formula': False,
              'disable-dram-formula': True,
              'cpu-rapl-ref-event': 'RAPL_ENERGY_PKG',
              'cpu-tdp': 125,
              'cpu-base-clock': 100,
              'cpu-frequency-min': 4,
              'cpu-frequency-base': 19,
              'cpu-frequency-max': 42,
              'cpu-error-threshold': 2.0,
              'sensor-report-sampling-interval': 1000,
              'learn-min-samples-required': 10,
              'learn-history-window-size': 60,
              'real-time-mode': False}
    run_smartwatts(config)
    check_db()



def test_normal_behaviour_real_time(mongo_database, shutdown_system):
    config = {'verbose': True,
              'stream': False,
              'actor_system': SIMPLE_SYSTEM_IMP,
              'input': {'puller_mongodb': {'type': 'mongodb',
                                           'model': 'HWPCReport',
                                           'uri': MONGO_URI,
                                           'db': MONGO_DATABASE_NAME,
                                           'collection': MONGO_INPUT_COLLECTION_NAME}},
              'output': {'power_pusher': {'type': 'mongodb',
                                          'model': 'PowerReport',
                                          'uri': MONGO_URI,
                                          'db': MONGO_DATABASE_NAME,
                                          'collection': MONGO_OUTPUT_COLLECTION_NAME},
                         'formula_pusher': {'type': 'mongodb',
                                            'model': 'FormulaReport',
                                            'uri': MONGO_URI,
                                            'db': MONGO_DATABASE_NAME,
                                            'collection': 'test_result_formula'}},
              'disable-cpu-formula': False,
              'disable-dram-formula': True,
              'cpu-rapl-ref-event': 'RAPL_ENERGY_PKG',
              'cpu-tdp': 125,
              'cpu-base-clock': 100,
              'cpu-frequency-min': 4,
              'cpu-frequency-base': 19,
              'cpu-frequency-max': 42,
              'cpu-error-threshold': 2.0,
              'sensor-report-sampling-interval': 1000,
              'learn-min-samples-required': 10,
              'learn-history-window-size': 60,
              'real-time-mode': True}
    run_smartwatts(config)
    check_db_real_time()
