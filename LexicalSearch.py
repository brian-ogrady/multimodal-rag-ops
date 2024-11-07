import os
import cassio
import uuid
from cassandra.cluster import Session
from cassandra.query import SimpleStatement
from dotenv import load_dotenv
from typing import List, Optional, Dict
from enum import Enum


load_dotenv()


OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
ASTRA_DB_APPLICATION_TOKEN = os.environ.get('ASTRA_DB_APPLICATION_TOKEN')
ASTRA_DB_DATABASE_ID = os.environ.get('ASTRA_DB_DATABASE_ID')
ASTRA_DB_KEYSPACE = os.environ.get('ASTRA_DB_KEYSPACE')


cassio.init(
    database_id=ASTRA_DB_DATABASE_ID,
    token=ASTRA_DB_APPLICATION_TOKEN,
    keyspace=ASTRA_DB_KEYSPACE,
)
session = cassio.config.resolve_session()


class Index(Enum):
    ANN = 'ann_index'
    LEXICAL = 'lexical_index'


def create_vector_table(session: Session,
                        keyspace: str,
                        table_name: str,
                        field_list: List[List[str]]):
    fields = ', '.join([' '.join(f) for f in field_list])
    create_table_statement = SimpleStatement(f"""CREATE TABLE IF NOT EXISTS
                                             {keyspace}.{table_name} ({fields});""")
    session.execute(create_table_statement)
    print(f"Table {keyspace}.{table_name} created successfully.")
    return


def create_index(session: Session,
                 keyspace: str,
                 table_name: str,
                 field_name: str,
                 index_type: Index,
                 options: Optional[Dict]):
    create_ann_index_statement = f"""CREATE CUSTOM INDEX {index_type} ON 
                                    {keyspace}.{table_name}({field_name})
                                    USING 'StorageAttachedIndex'"""
    if options is not None:
        create_ann_index_statement = ' '.join([create_ann_index_statement, f"""WITH {str(options)}"""])
    create_ann_index_statement = ''.join([create_ann_index_statement, ';'])
    session.execute(SimpleStatement)
    return


def insert_row(keyspace: str,
               table_name: str,
               *args):
    fields = ', '.join(args)
    values = ', '.join(['%s'*len(args)])
    insert_query = f"""
    INSERT INTO {keyspace}.{table_name} ({fields})
    VALUES ({values});
    """
    session.execute(insert_query, (args))
