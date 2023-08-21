import dataiku

# Initialize the API client
client = dataiku.api_client()

def import_sql_table(project_key: str, dataset_name: str, dataset_type: str, connection: str, database: str,schema: str, table: str):
    """
    This function creates an empty SQL DSS dataset, then points that dataset to a SQL table/view.
    """
    project = client.get_project(project_key)
                      
    # Create a blank SQL dataset
    dataset = project.create_sql_table_dataset(dataset_name = dataset_name, type = dataset_type, connection = connection, schema = schema, table = table)
    
    # Since the create_sql_table() function is missing the 'database' input, we need to update the settings
    settings = dataset.get_settings()
    settings.set_table(connection=connection, catalog=database, schema=schema, table=table)
    
    # If we have changed the table, there is a good chance that the schema is not good anymore, so we must
    # have DSS redetect it. `autodetect_settings` will however only detect if the schema is empty, so let's clear it.
    del settings.schema_columns[:]
    settings.save()
    
    # We run autodetection
    settings = dataset.autodetect_settings()
    # settings is now an object containing the "suggested" new dataset settings, including the completed schema
    # We can just save the new settings in order to "accept the suggestion"
    settings.save()
    
    return f'{dataset_name} created'

def move_dataset_to_zone(project_key: str, dataset: str, target_flow_zone: str):
    """
    This function moves a DSS dataset into a chosen Flow Zone. If the zone does not exist, it creates it.
    """
    project = client.get_project(project_key)
    
    # Get flow object
    flow = project.get_flow()
    for flow_zone in flow.list_zones():
        if flow_zone.name == target_flow_zone:
                target_flow_zone_object = flow_zone
                break
        else:
            target_flow_zone_object = None
   
    # If target flow zone does not exist, create it
    if target_flow_zone_object == None:
        target_flow_zone_object = flow.create_zone(target_flow_zone)
        
    # Move the dataset to the target flow zone
    dataset = project.get_dataset(dataset)
    dataset.move_to_zone(target_flow_zone_object)
    
    return f'{dataset} moved to {target_flow_zone}'
        
def list_snowflake_datasets(project_key):
    project = client.get_project(project_key)
    datasets = project.list_datasets()
    
    snowflake_datasets = []
    for dataset in datasets:
        if dataset['type'] == 'Snowflake':
            try:
                d = {}
                d['DSS_NAME'] = dataset['name']
                d['CONNECTION'] = dataset['params']['connection']
                d['DATABASE'] = dataset['params']['catalog']
                d['SCHEMA'] = dataset['params']['schema']
                d['TABLE'] = dataset['params']['table']

                snowflake_datasets.append(d)
            except Exception as e:
                print(f"{dataset['name']} failed | Error: {e}")
    
    return snowflake_datasets    
