import ServerConnections as sc
import SQLWrapper as sqlw
import threading

class Server():
    def __init__(self):
        self.create_database()
        self.server_connections = sc.ServerConnection()

        self.table_names = []

    #####################################################
    ### Methods that interact with the SQLWrapper Library
    #####################################################
    def create_database(self):
        self.database = sqlw.SQLWrapper('ServerDB.db')

    
    def create_table(self, table_name, columns):
        self.table_names.append(table_name)
        self.database.create_table(table_name,columns)
        self.database.commit_changes()


    def add_entry(self, table_name, columns, values):
        self.database.insert(table_name, columns, values)
        self.database.commit_changes()

    
    def mod_table(self, table_name, adding, columns):
        print(columns)
        self.database.alter(table_name, columns, adding)


    #####################################################
    ### Methods that break down componenets to have valid
    ### parameters for their according method above
    #####################################################
    def command_add(self, components):
        index = 0
        for comp in components:
            if comp == ',':
                break
            index += 1
        
        if index == len(components):
            self.add_entry(components[1], [], components[2:])
        else:
            self.add_entry(components[1],components[2:index], components[index+1:])


    def command_mod(self, components):
        index = 3
        columns = {}

        while index < len(components) - 1:
            columns[components[index]] = components[index + 1]
            index += 2

        self.mod_table(components[1], components[2], columns)

    
    def command_que(self, components):
        comp_len = len(components)

        if len(components) == 2:
            return self.database.select(components[1])

        index = 0
        for comp in components:
            if type(comp) == bool:
                break
            index += 1

        if index == comp_len:
            return self.database.select(components[1], components[2:])
        
        distinct = components[index]

        if index == comp_len - 1:
            return self.database.select(components[1], components[2:index], distinct)
        
        index += 1

        clauses_start = index

        remaining = components[index:]

        for comp in remaining:
            if type(comp) == bool:  
                break
            index += 1

        clause_list = []

        if index == comp_len:
            for e in map(lambda x: str(x), remaining):
                clause_list.append(e)
            clauses = " ".join(clause_list)
            return self.database.select(components[1], components[2:clauses_start - 1], distinct, clauses)

        for e in map(lambda x: str(x), components[clauses_start:index]):
            clause_list.append(e)
        clauses = " ".join(clause_list)

        order_asc = components[index]
        order_by = components[index + 1:]
        return self.database.select(components[1], components[2: clauses_start - 1], distinct, \
            clauses, order_asc, order_by[0])
        
        


    def command_create(self, components):
        index = 2
        columns = {}

        while index < len(components) - 1:
            columns[components[index]] = components[index + 1]
            index += 2

        self.create_table(components[1],columns)



    ###############################################################
    ### Methods to deal with the incoming commands from the clients
    ###############################################################
    @staticmethod
    def convert_types(components):
        for index in range(len(components)):
            if components[index] == 'false':
                components[index] = False
            
            elif components[index] == 'true':
                components[index] = True

            elif components[index].isdigit():
                components[index] = float(components[index])
        
        return components


    def check_commands(self):
        # We loop through each client that is connected with this
        # however if we had a large amount of users we could group users
        # into subpods and thread each sub pod to ensure that the
        # loop is only going through x clients each iteration
        for client in self.server_connections.client_connections:
            if client.command:
                components = client.command.split(' ')
                components = self.convert_types(components)
                client.command = None
                if components[0] == 'add':
                    try:
                        self.command_add(components)
                        client.data = 'Successfully added'
                    except:
                        client.data = 'Unsuccessfully added'

                elif components[0] == 'mod':
                    try:
                        self.command_mod(components)
                        client.data = 'Successfully altered'
                    except:
                        client.data = 'Unsuccessfully altered'

                elif components[0] == 'que':
                    try:
                        client.data = self.command_que(components).to_dict()
                    except:
                        client.data == 'Unsuccessful query'
                
                elif components[0] == 'crt':
                    try:
                        self.command_create(components)
                        client.data = 'Successfully created'
                    except:
                        client.data = 'Unsuccessfully created'


    # Used to start the server
    def main(self):
        connections_thread = threading.Thread(target=self.server_connections.server_main)
        connections_thread.start()

        while True:
            self.check_commands()


s = Server()
s.main()
