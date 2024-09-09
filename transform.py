from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path
from pandas import DataFrame
import pandas as pd
import numpy as np

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform_in_postgres(df, *args, **kwargs) -> DataFrame:
    """
    Performs a transformation in Postgres
    """
    dataFrame=df[0]

    #columns to exclude during merge
    params=df[1]

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    # Specify your SQL transformation query
    query = 'SELECT * FROM employees2'

    # Specify table to sample data from. Use to visualize changes to table.
    sample_table = 'employees2'
    sample_schema = 'public'
    sample_size = 10_000

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        # Write queries to transform your dataset with
        loader.execute(query)
        loader.commit() # Permanently apply database changes
        data = loader.sample(sample_schema, sample_table, sample_size)

        # data['id'] = data['id'].astype(int)

        # data['code'] = data['code'].astype('int64')
        data.rename(columns={'employee_id':'id'},inplace=True)
        dataFrame.rename(columns={'employee_id':'id'},inplace=True)

        merged = pd.merge(dataFrame, data, on=['id'], how='outer', indicator=True)
        #mergedLeft = pd.merge(data, df, on=['code'], how='left')




       # print(mergedInner)
        # print(mergedInsert)
        # print(mergedUpdate)
        # merged['id'] = merged['id'].astype(int)
        merged['id'] = pd.to_numeric(merged['id'], errors='coerce')
        merged['id'].replace([np.nan, np.inf, -np.inf], 0, inplace=True)
        # print(merged.dtypes)
        # return merged
        # return merged
        #merged.replace([id], 0, inplace=True)
        merged['id'] = merged['id'].astype('int64')
        merged['id'].replace(0, '', inplace=True)
        #print(merged.dtypes)

        # Drop both 'first_name_y' and 'last_name_y' in one step
        colnamesOriginal=dataFrame.columns.difference(params)
        colnamesToDrop=[col+"_y" for col in colnamesOriginal]
        merged = merged.drop(colnamesToDrop, axis=1)

        
        for column in colnamesOriginal:
            merged = merged.rename(columns={f'{column}_x': f'{column}'})


        #create two separate tables 
        mergedInsert = merged[merged['_merge'] != 'both']
        mergedUpdate = merged[merged['_merge'] == 'both']

        mergedInsert.drop(columns=params,axis=1,inplace=True)
        mergedInsert.drop(columns={"_merge"},axis=1,inplace=True)

        mergedUpdate.drop(columns={"_merge"},axis=1,inplace=True)

        return [mergedUpdate,mergedInsert]


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
