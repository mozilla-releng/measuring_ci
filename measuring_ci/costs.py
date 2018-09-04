import pandas


def fetch_worker_costs(csv_filename):
    """static snapshot of data from worker_type_monthly_costs table."""

    df = pandas.read_csv(csv_filename)
    expect_columns = {
        'worker_type', 'unit_cost', 'provisioner', 'usage_hours', 'cost',
    }
    if expect_columns.symmetric_difference(df.columns):
        raise ValueError(
            "Expected worker_type_monthly_costs to have a specific set of columns",
        )
    # XXX Callek not sure why provisioner is wrong, but
    # it doesn't match the taskgraph and there ends up being some
    # worker type duplicates across multiple provisioners.
    # Manual inspection indicates that costs are equal across provisioners
    df.drop_duplicates('worker_type', inplace=True)
    df.set_index('worker_type', inplace=True)
    return df
