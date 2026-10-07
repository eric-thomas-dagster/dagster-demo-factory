import dagster as dg

from skygen.partitions import TENANT_DAILY_PARTITIONS


@dg.template_var
def tenant_daily_partitions():
    return TENANT_DAILY_PARTITIONS
