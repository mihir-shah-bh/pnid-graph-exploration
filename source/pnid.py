from neo4j import GraphDatabase
from neo4j import Session
import pandas as pd
import numpy as np


def __read_csv():
    inst_df = pd.read_csv('D:\Projects\IAM\Code\pnid-graph-exploration\data\inst-1440-20-PI-DG-4419.csv')
    
    return inst_df


def __get_prepare_data(source_df):
    """
    Prepare data to be loaded in the graph db.
    Following transformation has to be done.
    1. Replace nan with '' - Done
    2. Make all node text 'Title Case'. i.e. Valve, Process
    3. Remove hyphen '-' from category title/ i.e. valve-ball should be ValveBall (with rule 2 applied)
    4. Remove rows that has instrument category starting 'link-'
    5. Remove 'link' prefix from link category.
    6. Make link category 'ALL_CAPS'
    """
    transformed_df = source_df.replace(np.nan, '')
    
    return transformed_df


def __open_session(uri):
    """
    Opens a session with the graph DB and return the session object.
    """
    graph_db = GraphDatabase.driver(uri=uri, auth=('neo4j','P@55word'))
    session = graph_db.session()

    print(f'Connected to Graph Db: {uri}')

    return session


def __clear_graph(session):
    """
    WARNING: Clears the entire graph database.
    """
    query = """
        MATCH (n)
        DETACH DELETE n
        """

    session.run(query)


def __print_entire_graph(session):
    return_value = session.run('''MATCH (x)-[*]->(y) RETURN x.name,y.name''')
    graph_inst_df = return_value.to_df()
    print(graph_inst_df)


def __create_source_nodes(session, source_df):
    
    for (index, row) in source_df.iterrows():

        query = f"""
            MERGE (x:{row.values[1]} {{name: '{row.values[1]}', id: '{row.values[0]}'}}) 
            SET x.predConfidence={row.values[2]}
            """
        
        session.run(query)


def __prepare_relationship_query(row, destination_node):
    query = f"""
        MATCH (x:{row.values[1]} {{id: '{row.values[0]}'}}) 
        MATCH (y) WHERE y.id = '{destination_node}'
        MERGE (x)-[r:{row.values[6]} {{id: '{row.values[5]}'}}]->(y) 
        """
    
    return query


def __create_relationships(session, source_df):
    for (index, row) in source_df.iterrows():
        if row[5] != '':
            query = __prepare_relationship_query(row, f'{row.values[7]}') 
            session.run(query)
            
            if row[8] != '':
                query = __prepare_relationship_query(row, f'{row.values[8]}') 
                session.run(query)

            if row[9] != '':
                query = __prepare_relationship_query(row, f'{row.values[9]}')
                session.run(query)


if __name__ == '__main__':

    session = None
    try:
        # Read the instruments data from CSV
        source_df = __read_csv()
        prepared_df = __get_prepare_data(source_df)

        session = __open_session('bolt://localhost:7687')

        print("Clearing entire graph Db")
        __clear_graph(session)

        print("Creating nodes")
        __create_source_nodes(session, prepared_df)

        print("Creating relationships")
        __create_relationships(session, prepared_df)

        print("Getting all nodes from graphs...")
        __print_entire_graph(session)

    except Exception as e:
        print(e)