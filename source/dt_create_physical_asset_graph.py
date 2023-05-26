# Databricks notebook source
# MAGIC %sh
# MAGIC pip install neo4j

# COMMAND ----------

import logging

from datetime import datetime
from delta.tables import DeltaTable
from neo4j import GraphDatabase
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

# COMMAND ----------

def get_json_vertices(df : DataFrame) -> list:
    try:
        distinct_vertices_df = df.select(col("ID").alias("id")).union(df.select(col("ParentID").alias("id"))).distinct()

        vertices = (distinct_vertices_df.alias("a").join(df.alias("b"),col("a.id") == col("b.ID"), "left")
                    .select(col("a.id"), col("b.Name"), col("b.Description"), col("b.GroupType"), col("b.ParentID"), col("b.Mode"), col("b.State"))
                    .filter(col("id").isNotNull())
                   )

        vertices_data = []

        for row in vertices.collect():
            dict = {}
            if row['Description'] != None and '"' in row['Description']:
                dict['DESCRIPTION'] = row['Description'].replace('"','')
            else:
                dict['DESCRIPTION'] = row['Description']
            dict['ID'] = row['id']
            dict['NAME'] = row['Name']
            dict['GROUP_TYPE'] = row['GroupType']
            dict['PARENT_ID'] = row['ParentID']
            dict['MODE'] = row['Mode']
            dict['STATE'] = row['State']

            vertices_data.append(dict)

        return vertices_data
    except Exception as e:
        logger.error(f'Error creating vertices in JSON format : {str(e)}')
        raise Exception(f'Error creating vertices in JSON format : {str(e)}')

# COMMAND ----------

def get_json_edges(df : DataFrame) -> list:
    try:
        edges = df.select(col("ParentID").alias("src"), col("ID").alias("dst"))

        edges_data = []

        for row in edges.distinct().collect():
            dict = {}
            dict['from'] = row['src']
            dict['to'] = row['dst']
            edges_data.append(dict)

        return edges_data
    except Exception as e:
        logger.error(f'Error creating edges in JSON format : {str(e)}')
        raise Exception(f'Error creating edges in JSON format : {str(e)}')

# COMMAND ----------

def create_batch_data(data : list) -> list :
    try:
        i = 0
        batch_size = 20000
        total_count = len(data)

        batch_data = []

        while total_count > 0:
            if (i + batch_size) > len(data):
                batch_data.append(data[i: len(data)])
            else:
                batch_data.append(data[i: (i + batch_size)])
            i = i + batch_size
            total_count = total_count - batch_size

        return batch_data
    except Exception as e:
        logger.error(f'Error creating batches of data : {str(e)}')
        raise Exception(f'Error creating batches of data : {str(e)}')

# COMMAND ----------

def main():
    '''This function creates graph in Neo4j from delta table'''
    try:
        catalog = 'iam_dev_new'
        database = 'onepm_bronze'
        table_name = 'physical_asset'
        
        logger.info(f'Reading the {catalog}.{database}.{table_name} delta table...')
        physical_asset_df = ( DeltaTable.forName(spark, f'{catalog}.{database}.{table_name}')
              .toDF()
              #.filter(col('is_deleted') == 'N') 
             )
        
        logger.info(f'Creating vertices...')
        vertices_data = get_json_vertices(physical_asset_df)
        
        logger.info(f'Creating batches of vertices...')
        vertices_batch_data = create_batch_data(vertices_data)
        
        logger.info(f'Creating edges...')
        edges_data = get_json_edges(physical_asset_df)
        
        logger.info(f'Creating batches of edges...')
        edges_batch_data = create_batch_data(edges_data)        
        
        logger.info(f'Connecting to Neo4j server...')
        graph_db = GraphDatabase.driver(uri='neo4j://10.15.200.7:7687', auth=('neo4j','dAta@team'))
        session = graph_db.session()
        
        logger.info(f'Creating Nodes in Neo4j...')
        for batch_data in vertices_batch_data:
            session.run('''UNWIND $batch_data as row
            CREATE (n:Asset)
            SET n = row
            ''', batch_data = batch_data)
            
        logger.info(f'Creating Indexes on Nodes in Neo4j...')
        # session.run('CREATE CONSTRAINT asset_id_unique FOR (a:Asset) REQUIRE a.ID IS UNIQUE')
        # session.run('CREATE INDEX asset_parent_id FOR (a:Asset) ON (a.PARENT_ID)')
        # session.run('CREATE INDEX asset_name FOR (a:Asset) ON (a.NAME)')
        # session.run('CREATE INDEX asset_description FOR (a:Asset) ON (a.DESCRIPTION)')
        
        logger.info(f'Creating Relationships in Neo4j...')
        for batch_data in edges_batch_data:
            session.run('''UNWIND $batch_data as row
            MATCH (a:Asset), (b:Asset)
            WHERE a.ID = row.from AND b.ID = row.to
            MERGE (a)-[r:HAS]->(b)
            ''', batch_data = batch_data)
        
        logger.info(f'Physical Asset graph created in Neo4j successfully!')
    except Exception as e:
        logger.error(f'Error in main : {str(e)}')
        raise Exception(f'Error in main : {str(e)}')

# COMMAND ----------

def configure_logger(app_name : str, date : str) -> logging.Logger :
    '''This function creates a logger object for a given application name and date'''
    logger = logging.getLogger(f"{app_name}")
    logger.setLevel(logging.DEBUG)
    
    ch = logging.FileHandler(f"/tmp/{app_name}_{date}.log", mode='a')
    sh = logging.StreamHandler()

    formatter = logging.Formatter('%(asctime)s~|~%(name)s~|~%(levelname)s~|~%(message)s')

    ch.setFormatter(formatter)
    sh.setFormatter(formatter)

    for handler in logger.handlers:
        logger.removeHandler(handler)

    logger.addHandler(ch)
    logger.addHandler(sh)
    
    return logger

# COMMAND ----------

if __name__ == '__main__':
    try:
        app_name = 'dt_create_graph'
        date = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
        logger = configure_logger(app_name , date)
        main()
    except Exception as e:
        raise e
    finally:
        pass
        #move the log file to ADLS storage
        #dbutils.fs.mv(f'file:/tmp/{app_name}_{date}.log', '')
