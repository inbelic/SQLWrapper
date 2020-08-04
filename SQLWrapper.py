import sqlite3
import pandas as pd

class SQLWrapper():
    def __init__(self, database_name):
        self.database = sqlite3.connect(database_name)
        self.cur = self.database.cursor()

    ############################################################
    ### Methods using execute_commmand to interact with database
    ############################################################
    def execute_command(self, command):
        self.cur.execute(command)


    def create_table(self, table_name, columns):
        execution_str = f"CREATE TABLE {table_name} ("

        for name in columns:
            execution_str += f" {name} {columns[name]},"

        execution_str = execution_str[:-1]
        execution_str += " );"

        self.execute_command(execution_str)


    def erase_table_entries(self, table_name, condition=''):
        if condition != '':
            self.cur.execute(f"DELETE FROM {table_name} WHERE {condition};")
        else:
            self.cur.execute(f"DELETE FROM {table_name};")


    def drop_table(self, table_name):
        self.cur.execute(f"DROP TABLE {table_name};")



    def insert(self, table_name, columns, values):
        # Adds a new item into an existing table,
        # when columns = [] then the added item requires all possibles values
        # otherwise a list of columns corresponds to the columns being filled in
        # and the other are set to None or NaN accordingly 

        execution_str = f"INSERT INTO {table_name}"

        if len(columns):
            execution_str += " ("
            for col_name in columns:
                execution_str += f" {col_name},"

            execution_str = execution_str[:-1]
            execution_str += " ) VALUES ("
        else:
            execution_str += " VALUES ("

        for val in values:
            if type(val) == str:
                execution_str += f" '{val}',"
            else:
                execution_str += f" {val},"

        execution_str = execution_str[:-1]
        execution_str += " );"

        self.execute_command(execution_str)


    def alter(self, table_name, columns={}, add=True):
        execution_str = f"ALTER TABLE {table_name}"
        if add:
            execution_str += " ADD"
            print(columns)
            for name in columns:
                execution_str += f" {name} {columns[name]},"
        else:
            execution_str += " DROP COLUMN"
            for name in columns:
                execution_str += f" {name},"

        execution_str = execution_str[:-1]
        execution_str += ";"

        self.execute_command(execution_str)


    ############################################################
    ### Methods that use run_query to interact with the database
    ############################################################
    def run_query(self, query):
        return pd.read_sql_query(query, self.database)


    def select(self, table_name, columns=[], distinct=False, clauses='', order_asc=True, order_by=''):
        
        execution_str = "SELECT"
        if distinct:
            execution_str += " DISTINCT"
        if len(columns) == 0:
            execution_str += " *"
        else:
            for col_name in columns:
                execution_str += f" {col_name},"
            execution_str = execution_str[:-1]

        execution_str += f" FROM {table_name}"

        if len(clauses):
            execution_str += f" WHERE {clauses};"
        
        if len(order_by):
            if execution_str[-1] == ";":
                execution_str = execution_str[:-1]
            if order_asc:
                direc = "ASC"
            else:
                direc = "DESC"
            execution_str += f" ORDER BY {order_by} {direc};"

        print(execution_str)
        return self.run_query(execution_str)




    ########################
    ### Misc Methods
    ########################
    def commit_changes(self):
        self.database.commit()

