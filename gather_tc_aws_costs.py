import argparse
import asyncio
from datetime import datetime
import logging
import os
import re

import boto3
import pandas as pd
import yaml

LOG_LEVEL = logging.INFO

# AWS artisinal log handling, they've already set up a handler by the time we get here
log = logging.getLogger()
log.setLevel(LOG_LEVEL)


def parse_args():
    """Extract arguments."""
    parser = argparse.ArgumentParser(description="CI Costs")
    parser.add_argument('--config', type=str, default='aws_tc_costs.yml')
    return parser.parse_args()


def iter_cost_and_usage_groups(config):
    ce = boto3.client(
        'ce',
        aws_access_key_id='<redacted>',
        aws_secret_access_key='<redacted>'
        )
    next_page_token = None
    while True:  # break below
        kwargs = dict(
            TimePeriod={
                'Start': '2018-09-01',  # Inclusive
                'End': '2018-10-01',  # Exclusive
                },
            Granularity='MONTHLY',  # MONTHLY, DAILY, HOURLY
            GroupBy=[{'Type': 'TAG', 'Key': 'WorkerType'}],
            Metrics=['UnblendedCost', 'UsageQuantity'],
            Filter={
                'Dimensions': {
                    'Key': 'USAGE_TYPE_GROUP',
                    'Values': ["EC2: Running Hours"],
                },
            },
        )
        if next_page_token:
            kwargs['NextPageToken'] = next_page_token
            next_page_token = None
        response = ce.get_cost_and_usage(**kwargs)
        log.debug(response['ResponseMetadata'])
        next_page_token = response.get('NextPageToken')
        # We don't expect multiple results by time per response
        assert len(response['ResultsByTime']) == 1
        yield from response['ResultsByTime'][0]['Groups']
        if not next_page_token:
            break


def split_worker_tag(tag):
    data = tag.split('/', maxsplit=1)
    if not data:
        raise ValueError("bad tag")
    if len(data) > 1:
        return data
    return ["none", data[0]]


async def fetch_raw_cost_explorer(config):
    worker_type_re = re.compile(re.escape(r'WorkerType$'))
    rows = []
    for group in iter_cost_and_usage_groups(config):
        # Expect only 1 key
        assert len(group['Keys']) == 1
        key = group['Keys'][0]
        if not worker_type_re.match(group['Keys'][0]):
            raise Exception("Unexpected Key in Group, {}".format(group))
        key = worker_type_re.sub('', key)
        if not key:
            key = "<untagged>"
        provisioner , worker_type = split_worker_tag(key)
        row = {
            'modified': datetime.now(),  # XXX Fixup
            'year': '2018',
            'month': '09',
            'provider': 'aws',
            'provisioner': provisioner,
            'worker_type': worker_type,
            'usage_hours': group['Metrics']['UsageQuantity']['Amount'],
            'cost': group['Metrics']['UnblendedCost']['Amount'],
        }
        #import pdb;pdb.set_trace()
        import pprint
        pprint.pprint(row)
        rows.append(row)
        #import json
        #print(json.dumps(response))
    import pdb;pdb.set_trace()


async def main(args):
    """Main program."""
    with open(args['config'], 'r') as cfg:
        config = yaml.load(cfg)
    os.environ['TC_CACHE_DIR'] = config['TC_CACHE_DIR']

    await fetch_raw_cost_explorer(config)


def lambda_handler(args, context):
    """AWS Lambda entry point."""
    assert context  # not currently used
    if 'config' not in args:
        args['config'] = 'nightlies.yml'
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))


if __name__ == '__main__':
    logging.basicConfig(level=LOG_LEVEL)
    # Use command-line arguments instead of json blob if not running in AWS Lambda
    lambda_handler(vars(parse_args()), {'dummy': 1})
