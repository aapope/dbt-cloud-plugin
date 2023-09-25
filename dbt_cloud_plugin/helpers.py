from airflow.utils.task_group import TaskGroup

from .operators.dbt_cloud_check_model_result_operator import DbtCloudCheckModelResultOperator


def generate_dbt_model_dependency(dbt_job_task, downstream_tasks, dependent_models, ensure_models_ran=True):
    """
    Create a dependency from one or more tasks on a set of models succeeding
    in a dbt task. This function generates a new DbtCloudCheckModelResultOperator
    task between dbt_job_task and downstream_tasks, checking that dependent_models
    all ran successfully.

    :param dbt_job_task: The dbt Cloud operator which kicked off the run you want to check.
        Both the credentials and the run_id will be pulled from this task.
    :type dbt_job_task: DbtCloudRunJobOperator or DbtCloudRunAndWatchJobOperator
    :param downstream_tasks: The downstream task(s) which depend on the model(s) succeeding.
        Can be either a single task, a single TaskGroup, or a list of tasks.
    :type downstream_tasks: BaseOperator or TaskGroup or list[BaseOperator]
    :param dependent_models: The name(s) of the model(s) to check. See
        DbtCloudCheckModelResultOperator for more details.
    :type dependent_models: str or list[str]
    :param ensure_models_ran: Whether to require that the dependent_models actually ran in
        the run. If False, it will silently ignore models that didn't run.
    :type ensure_models_ran: bool, default True
    """
    
    if isinstance(downstream_tasks, list):
        task_id = f'check_dbt_model_results__{dbt_job_task.task_id}__{len(downstream_tasks)}_downstream'
    elif isinstance(downstream_tasks, TaskGroup):
        task_id = f'check_dbt_model_results__{dbt_job_task.task_id}__{downstream_tasks.group_id}'
    else:
        task_id = f'check_dbt_model_results__{dbt_job_task.task_id}__{downstream_tasks.task_id}'
    
    check_dbt_model_results = DbtCloudCheckModelResultOperator(
        task_id=task_id,
        dbt_cloud_conn_id=dbt_job_task.dbt_cloud_conn_id,
        dbt_cloud_run_id=f'{{{{ ti.xcom_pull(task_ids="{dbt_job_task.task_id}", key="dbt_cloud_run_id") }}}}',
        model_names=dependent_models,
        ensure_models_ran=ensure_models_ran,
        trigger_rule='all_done',
        retries=0
    )

    return dbt_job_task >> check_dbt_model_results >> downstream_tasks
