# -*- coding: utf-8 -*-
import json
import requests
import time

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException, AirflowSkipException
from ..hooks.dbt_cloud_hook import DbtCloudHook
from ..operators.dbt_cloud_run_job_operator import DbtCloudRunJobOperator


class DbtCloudRunAndWatchJobOperator(DbtCloudRunJobOperator):
    """
    Operator to run a dbt cloud job.
    :param dbt_cloud_conn_id: dbt Cloud connection ID.
    :type dbt_cloud_conn_id: string
    :param project_id: dbt Cloud project ID.
    :type project_id: int
    :param job_name: dbt Cloud job name.
    :type job_name: string
    """

    @apply_defaults
    def __init__(self,
                 poke_interval=60,
                 timeout=60 * 60 * 24,
                 soft_fail=False,
                 *args, **kwargs):
        self.poke_interval = poke_interval
        self.timeout = timeout
        self.soft_fail = soft_fail
        super(DbtCloudRunAndWatchJobOperator, self).__init__(*args, **kwargs)

    def execute(self, **kwargs):
        run_id = super(DbtCloudRunAndWatchJobOperator, self).execute(**kwargs)

        # basically copy-pasting the Sensor code
        self.log.info(f'Starting poke for job {run_id}')
        try_number = 1
        started_at = time.monotonic()

        def run_duration():
            nonlocal started_at
            return time.monotonic() - started_at

        while not self.poke(run_id):
            if run_duration() > self.timeout:
                if self.soft_fail:
                    raise AirflowSkipException(f'Time is out!')
                else:
                    raise AirflowException(f'Time is out!')
            else:
                time.sleep(self.poke_interval)
                try_number += 1
        self.log.info('Success criteria met. Exiting.')

    def poke(self, run_id):
        self.log.info('Sensor checking state of dbt cloud run ID: %s', run_id)
        dbt_cloud_hook = DbtCloudHook(dbt_cloud_conn_id=self.dbt_cloud_conn_id)
        run_status = dbt_cloud_hook.get_run_status(run_id=run_id)
        self.log.info('State of Run ID {}: {}'.format(run_id, run_status))

        TERMINAL_RUN_STATES = ['Success', 'Error', 'Cancelled']
        FAILED_RUN_STATES = ['Error', 'Cancelled']

        if run_status.strip() in FAILED_RUN_STATES:
            raise AirflowException('dbt cloud Run ID {} Failed.'.format(run_id))
        if run_status.strip() in TERMINAL_RUN_STATES:
            return True
        else:
            return False
