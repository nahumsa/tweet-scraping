import psycopg2
import pandas as pd
import sqlalchemy

def create_table(database_connection, table_name: str) -> bool:
    """ Create a table on the database with table_name.

    """
    
    try:
        cursor = database_connection.cursor()
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name}(
                id SERIAL PRIMARY KEY, 
                date TIMESTAMP NOT NULL,
                tweet TEXT NOT NULL,
                language TEXT NOT NULL
            );
            """
        cursor.execute(create_table_query)
        database_connection.commit()
        cursor.close()
        return True
    
    except:
        return False

def add_df_to_sql(df: pd.DataFrame, engine: sqlalchemy.engine.Engine, table_name:str) -> bool:
    """[summary]

    Args:
        df (pd.DataFrame): Dataframe that will be added to the database.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine that will be used
            to connect to the database.
        table_name(str): name of the dable that will be inserted.

    Returns:
        bool: True if data was added, and false if it was not added.
    """

    try:
        df.to_sql(table_name, engine, index=False, if_exists='append')
        return True
    
    except:
        return False
