from pyspark.sql.functions import count

def check_data(df, table_name):
    """
    Call functions for data quality checks
    
    Parameters: 
     - df: dataframe for table
     - table_name: name of the table
    Return: none
    """
    row_count = df.count()
    
    empty_check(df, table_name, row_count)
    unique_check(df, table_name, row_count)
    
    
def empty_check(df, table_name, row_count):
    """
    Checks to see if each table has data
    
    Parameters: 
     - df: dataframe for table
     - table_name: name of the table
     - row_count: count of rows in the table
    Return: none
    """
    if row_count <= 0:
        raise ValueError(f"Empty check failed. Table: {table_name} is empty.")
    else:
        print(f"Empty check passed. Table: {table_name} is not empty. There are {row_count} rows.")

        
def unique_check(df, table_name, row_count):
    """
    Checks to see if each row is unique in the table
    
    Parameters: 
     - df: dataframe for table
     - table_name: name of the table
     - row_count: count of rows in the table
    Return: none
    """
    table_row_count = row_count
    unique_row_count = df.distinct().count()

    
    if table_row_count != unique_row_count:
        raise ValueError(f"Unique check failed. Table: {table_name} has {table_row_count} rows, but there are only {unique_row_count} unique rows.")
    else:
        print(f"Unique check passed. Table: {table_name} has {unique_row_count} unique rows.")
    
    