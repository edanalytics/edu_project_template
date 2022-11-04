from typing import Optional

from airflow.utils.task_group import TaskGroup


def assign_endpoints_to_edfi_dag(
    edfi_dag,
    edfi_endpoints: dict,
    domain_mapping: Optional[dict] = None,

    *,
    table: Optional[str] = None,
    get_deletes: bool = True,
    use_change_version: bool = True,
):
    """

    :param edfi_dag:
    :param edfi_endpoints:
    :param domain_mapping:
    :param table:
    :param get_deletes:
    :param use_change_version:
    :return:
    """
    # If no domains are defined, do not group endpoint chains into domain task groups.
    if domain_mapping is None:

        # Iterate all resources generated from Swagger.
        # Add these to their respective TaskGroups.
        for endpoint, endpoint_vars in edfi_endpoints.items():

            # Not all resources must be ingested per DAG run.
            if not endpoint_vars.get('enabled'):
                continue

            namespace = endpoint_vars.get('namespace')
            page_size = endpoint_vars.get('page_size', 500)

            endpoint_task_group = edfi_dag.build_edfi_to_snowflake_task_group(
                endpoint, namespace=namespace, deletes=False,
                table=table, page_size=page_size,
                use_change_version=use_change_version
            )

            if use_change_version:
                edfi_dag.edfi_change_version_operator >> endpoint_task_group

            if endpoint_vars.get('fetch_deletes'):
                endpoint_deletes_task_group = edfi_dag.build_edfi_to_snowflake_task_group(
                    endpoint, namespace=namespace, deletes=True,
                    table='_deletes', page_size=page_size,
                    use_change_version=use_change_version
                )

                if use_change_version:
                    edfi_dag.edfi_change_version_operator >> endpoint_deletes_task_group

    # Otherwise, build a task group for each domain, and nested task groups for base and deletes if relevant.
    else:
        # Establish nested task groups based on domains mapped in `edfi_domain_mapping.yml`.
        # These objects must be referenced directly downstream, so they're saved in a mapping.
        dag_domain_task_groups = {}
        dag_domain_nested_task_groups = {}  # Only used in deletes-compatible runs.

        unclassified_domain_name  = "Unclassified Domain"
        get_task_group_id         = lambda dd: f"{dd} Endpoints"
        get_deletes_task_group_id = lambda dd: f"{dd} Deletes"

        edfi_domains = set(domain_mapping.values()) | {unclassified_domain_name}
        for domain in edfi_domains:

            domain_task_group = TaskGroup(
                group_id=domain,
                prefix_group_id=False,
                dag=edfi_dag.dag
            )
            dag_domain_task_groups[domain] = domain_task_group

            # If deletes are also ingested, divide domains into separate TaskGroups for resources and deletes.
            if get_deletes:
                domain_main_task_group = TaskGroup(
                    group_id=get_task_group_id(domain),
                    prefix_group_id=False,
                    parent_group=domain_task_group,
                    dag=edfi_dag.dag
                )
                dag_domain_nested_task_groups[get_task_group_id(domain)] = domain_main_task_group

                domain_deletes_task_group = TaskGroup(
                    group_id=get_deletes_task_group_id(domain),
                    prefix_group_id=False,
                    parent_group=domain_task_group,
                    dag=edfi_dag.dag
                )
                dag_domain_nested_task_groups[get_deletes_task_group_id(domain)] = domain_deletes_task_group


        # Iterate all resources generated from Swagger.
        # Add these to their respective TaskGroups.
        for endpoint, endpoint_vars in edfi_endpoints.items():

            # Not all resources must be ingested per DAG run.
            if not endpoint_vars.get('enabled'):
                continue

            namespace = endpoint_vars.get('namespace')
            page_size = endpoint_vars.get('page_size', 500)
            domain = domain_mapping.get(endpoint, unclassified_domain_name)

            # Some resources require a deletes sister table to be ingested as well.
            if get_deletes:
                edfi_dag.build_edfi_to_snowflake_task_group(
                    endpoint, namespace=namespace, deletes=False,
                    table=table, page_size=page_size,
                    use_change_version=use_change_version,
                    parent_group=dag_domain_nested_task_groups.get( get_task_group_id(domain) )
                )

                if endpoint_vars.get('fetch_deletes'):
                    edfi_dag.build_edfi_to_snowflake_task_group(
                        endpoint, namespace=namespace, deletes=True,
                        table='_deletes', page_size=page_size,
                        use_change_version=use_change_version,
                        parent_group=dag_domain_nested_task_groups.get( get_deletes_task_group_id(domain) )
                    )

            else:
                edfi_dag.build_edfi_to_snowflake_task_group(
                    endpoint, namespace=namespace,
                    table=table, page_size=page_size,
                    use_change_version=use_change_version,
                    parent_group=dag_domain_task_groups.get(domain)
                )

        # Chain the Resource and Deletes TaskGroups to the change version operator.
        # Note: Chaining the TaskGroups before adding individual resource TaskGroups
        #       yields NULL returns from the `edfi_change_version_operator`.
        if use_change_version:
            for task_group in dag_domain_task_groups.values():
                edfi_dag.edfi_change_version_operator >> task_group
