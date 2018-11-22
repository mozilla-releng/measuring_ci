import argparse
import asyncio
import logging
import os

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


async def fetch_raw_cost_explorer(config):
    ce = boto3.client(
        'ce',
        aws_access_key_id='<redacted>',
        aws_secret_access_key='<redacted>'
        )
    """ groupBy=TagKeyValue:WorkerType
        forecastTimeRangeOption=None
        hasBlended=false
        excludeRefund=false
        excludeCredit=false
        excludeRIUpfrontFees=false
        excludeRIRecurringCharges=false
        excludeOtherSubscriptionCosts=false
        excludeSupportCharges=false
        excludeTax=false
        excludeTaggedResources=false
        chartStyle=Stack
        timeRangeOption=LastMonth
        granularity=Monthly
        reportName=Monthly%20EC2%20running%20hours%20costs%20and%20usage
        isTemplate=true
        filter=[
            {
                "values":
                    [{"value":"EC2: Running Hours","unit":"Hrs"}],
                "dimension":"UsageTypeGroup",
                "include":true,
                "children":null
            }]
        reportType=CostUsage
        hasAmortized=false
        startDate=2018-09-01&endDate=2018-09-30
    """
    response = ce.get_cost_and_usage(
        TimePeriod={
            'Start': '2018-09-01',
            'End': '2018-10-01'
            },
        Granularity='MONTHLY',
        GroupBy=[{'Type': 'TAG', 'Key': 'WorkerType'}],
        Metrics=['UnblendedCost', 'UsageQuantity'],
        Filter={
            'Dimensions': {
                'Key': 'USAGE_TYPE_GROUP',
                'Values': ["EC2: Running Hours"],
            },
        },
        )
    #import pdb;pdb.set_trace()
    #import pprint
    #pprint.pprint(response)
    import json
    print(json.dumps(response))


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
