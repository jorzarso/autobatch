from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from google.colab import auth
from oauth2client.client import GoogleCredentials
import gspread
from google.auth import default
from gspread.models import Worksheet ,Cell
import xlsxwriter
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import row_number, monotonically_increasing_id, desc, concat_ws
from pyspark.sql.functions import col, when, lit, regexp_replace, from_json, max, expr, first
from pyspark.sql.window import Window
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *
from typing import List
from google.oauth2.sts import Client
import pyspark.sql.functions as F
from xlsxwriter.utility import *
from datetime import date
from datetime import datetime
from datetime import timedelta 
import functools
from functools import reduce
from pyspark.sql.functions import when
from xlsxwriter.utility import xl_col_to_name
from google.auth.impersonated_credentials import Credentials

def create_df(report, sheet, gc: Client, spark: SparkSession):
    """ Open worksheets and creates Dataframes"""

    worksheets = gc.open_by_key(report).worksheet(sheet)
    rows = worksheets.get_all_values()
    return spark.createDataFrame(rows, rows[0])

def map_sheet(sheet,concatenated_df,odate_l,gc: Client,batch_report:str,errors:list=None):
    """ Converts to A1 notation the ODATES 
        Maps ok/ko in our worksheet """

    worksheet_batch = gc.open_by_key(batch_report).worksheet(sheet)
    
    try:  
        colum: str = xl_col_to_name(worksheet_batch.get_all_values()[0].index(odate_l))
        list_index = concatenated_df.select("index", str(odate_l)).where(col(odate_l).isNotNull()).rdd.map(tuple).collect()
        range = colum + "2:" + colum + "5000"
        cell_list = worksheet_batch.range(range)
        i = 0
        for cell in (cell for cell in cell_list if cell.row in list(map(lambda x:x[0],list_index))):
            cell.value = list_index[i][1]
            i=i+1

        worksheet_batch.update_cells(cell_list)

    except Exception as error:
        if error == True:
            print(error)
            errors.append(odate_l)
        else:
            pass

def create_dataframe(worksheet: Worksheet ,spark: SparkSession) -> DataFrame:
  rows: list = worksheet.get_all_values()
  return spark.createDataFrame(rows,rows[0])

def filter_controm_df(control_m: DataFrame) -> DataFrame:
  return control_m.select("Name").dropDuplicates()

def filter_batch_df(batch_df: DataFrame):

  filtered_batch = batch_df.withColumn('index', row_number().over(Window.orderBy(monotonically_increasing_id())))\
  .filter(~batch_df["Proyecto"].isin(["", "Proyecto", "Capa HdfS", "Nombre Poyecto","Contacto","Nota"]))

  return filtered_batch

def join_df(filter_batch: DataFrame, control_m_filter: DataFrame, odate :str):
  
  return filter_batch.select("Planificación","Job en Control-M","Job en Argos - Dataproc","index")\
  .join(control_m_filter,filter_batch["Job en Control-M"] == control_m_filter["Name"],"left")\
  .withColumn(odate,when(col("Name").isNull(),"N/E").otherwise("PENDING")).orderBy("index")