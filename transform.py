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
def transform_in_postgres(df,data, *args, **kwargs) -> DataFrame:
    """
    Performs a transformation in Postgres
    """

    dataFrame=df[0]
    params=df[1]

    
    # Insert the column name you want to merge by
    mergeOn='id'


    ### If the column you want to merge by is an int
    dataFrame[mergeOn] = dataFrame[mergeOn].astype('int64')
    data[mergeOn] = data[mergeOn].astype('int64')

    ### If the column you want to merge by is a string:
    # dataFrame[mergeOn] = dataFrame[mergeOn].astype('str')
    # data[mergeOn] = data[mergeOn].astype('str')

    merged = pd.merge(dataFrame, data, on=['id'], how='outer', indicator=True)

  
    colnamesOriginal=dataFrame.columns.difference(params)
    colnamesToDrop=[col+"_y" for col in colnamesOriginal]
    merged = merged.drop(colnamesToDrop, axis=1)

    
    for column in colnamesOriginal:
        merged = merged.rename(columns={f'{column}_x': f'{column}'})

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
