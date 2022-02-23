import psycopg2
import string
from random import choices
from datetime import datetime


class PostGreSQL:
    table_name = "url"
    def __init__(self, host, port, dbname, user, password):
        """
        This function sets the database parameters
        """
        try:
            self.host = host
            self.port = port
            self.dbname = dbname
            self.user = user
            self.password = password
            self.connection_string = "host=%s port=%s dbname=%s user=%s password=%s" % (self.host, self.port, self.dbname, self.user, self.password)
        except Exception as e:
            raise Exception(f"(__init__): Something went wrong on initiation process\n" + str(e))

    def createDatabase(self, database_name):
        """
        This function to create a database inside the PostGreSQLserver
        """
        try:
            # get connection and cursor
            cursor, connection = self.createCursor()

            # Check if the table already exists
            cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{}';".format(database_name))

            check = cursor.fetchone()

            # Query to create a database
            create_database_query = "CREATE DATABASE %s;" % (database_name)

            # Execute the query to create a database named xVectorLabs_DB
            if not check:
                cursor.execute(create_database_query)

                # Commit the transaction
            connection.commit()

            # Close the cursor
            self.closeCursor(cursor)

            # Close the connection
            self.closeConnection(connection)

        except Exception as e:
            raise Exception(f'Something went wrong while creating a database in PostGreSQL server', str(e))

    def createTable(self):
        """
        This function drop table if already exists and
        create new table for urls and url metadata
        """
        table_column_string = '''id_ serial primary key not null, long varchar, 
                                short varchar(200), visits integer default 0,
                                link_added date default current_timestamp'''
        try:
            drop_table_query = "drop table if exists %s" % (self.table_name)

            create_table_query = "create table if not exists %s (%s)" % (self.table_name, table_column_string)

            # Creating cursor
            cursor, connection = self.createCursor()

            # Cursor executing table drop query
            cursor.execute(drop_table_query)

            # Cursor executing the table creation query
            cursor.execute(create_table_query)

            # Commit the transaction
            connection.commit()

            # Close the cursor
            self.closeCursor(cursor)

            # Close the connection
            self.closeConnection(connection)

        except Exception as e:
            raise Exception(f"Something went wrong on creating table\n" + str(e))

    def createConnection(self):
        """
        This function creates and establishes the connection between database and application
        """
        try:
            connection = psycopg2.connect(self.connection_string)
            return connection
        except Exception as e:
            raise Exception("(createConnection): Something went wrong on creation of connection \n" + str(e))

    def closeConnection(self, connection):
        """
        This function closes the connection of db
        """
        try:
            connection.close()
        except Exception as e:
            raise Exception(f"Something went wrong on closing connection\n", str(e))

    def createCursor(self):
        """
        This function created the cursor to execute databse queries
        """
        try:
            connection = self.createConnection()
            cursor = connection.cursor()
            return cursor, connection
        except Exception as e:
            raise Exception(f"(createCursor): Something went wrong on creation of cursor\n" + str(e))

    def closeCursor(self, cursor):
        """
        This function closes the cursor object
        """
        try:
            cursor.close()
        except Exception as e:
            raise Exception(f"(closeCursor): Something went wrong on closing cursor\n" + str(e))

    def generate_short_link(self):
        characters = string.ascii_lowercase + string.ascii_uppercase + string.digits

        short_url = ''.join(choices(characters, k=3))
        link = self.check_if_short_url_exists(short_url)
        
        if link:
            return self.generate_short_link()
        return short_url

    def get_shorten_url_from_DB(self, long_url):
        """
        This function stores the original url into database and returns the shortened url
        :param long_url:
        :return: short_url
        """
        try:
            long_url = str(long_url)
            shorten_url_query = "select short from url where long='{}'" .format(long_url)

            # Creating cursor
            cursor, connection = self.createCursor()

            # Cursor executing the shorten_url_query
            cursor.execute(shorten_url_query)

            # Fetching short from cursor and storing into shorten_url
            shorten_url = cursor.fetchone()

            # Commit the transaction
            connection.commit()

            # Close the cursor
            self.closeCursor(cursor)

            # Close the connection
            self.closeConnection(connection)

            return shorten_url[0]

        except Exception as e:
            raise Exception(f"Something went wrong on getting short url from DB\n" + str(e))

    def add_long_short_to_DB(self, long, short):

        """
        This function inserts original url and shortened url to db
        :param long:
        :param short:
        :return:
        """
        add_long_short_query = "insert into url(long,short) values('{}', '{}')".format(str(long),str(short))

        # Creating cursor
        cursor, connection = self.createCursor()

        # Cursor executing the shorten_url_query
        cursor.execute(add_long_short_query)

        # Commit the transaction
        connection.commit()

        # Close the cursor
        self.closeCursor(cursor)

        # Close the connection
        self.closeConnection(connection)

    def check_if_url_exists(self, long_url):
        """
        This function checks existence of original url in the
        database name and returns boolean value.
        :param long_url:
        :return: Boolean
        """
        # checks if already long-url exists
        check_query = "select exists(select long from {} where long='{}');".format(self.table_name,long_url)

        # Creating cursor
        cursor, connection = self.createCursor()

        # Cursor executing the shorten_url_query
        cursor.execute(check_query)

        # Fetching boolean from cursor and storing into check_status variable
        check_status = cursor.fetchone()

        # Commit the transaction
        connection.commit()

        # Close the cursor
        self.closeCursor(cursor)

        # Close the connection
        self.closeConnection(connection)

        return check_status[0]
    
    def check_if_short_url_exists(self,short_url):
        """
        This function checks existence of short url in the
        database name and returns boolean value.
        :param short url:
        :return: Boolean
        """
        # checks if already short-url exists
        check_query = "select exists(select short from url where short='{}');".format(short_url)

        # Creating cursor
        cursor, connection = self.createCursor()

        # Cursor executing the shorten_url_query
        cursor.execute(check_query)

        # Fetching boolean from cursor and storing into check_status variable
        check_status = cursor.fetchone()

        # Commit the transaction
        connection.commit()

        # Close the cursor
        self.closeCursor(cursor)

        # Close the connection
        self.closeConnection(connection)

        return check_status[0]

    def get_original_link_from_DB(self, short_url):
        """
        This function retreives and returns original url from database
        :param short_url:
        :return: long_url
        """
        try:
            short_url = str(short_url)
            original_url_query = "select long from url where short='{}'" .format(short_url)

            # Creating cursor
            cursor, connection = self.createCursor()

            # Cursor executing the shorten_url_query
            cursor.execute(original_url_query)

            # Fetching short from cursor and storing into shorten_url
            original_url = cursor.fetchone()

            # Commit the transaction
            connection.commit()

            # Close the cursor
            self.closeCursor(cursor)

            # Close the connection
            self.closeConnection(connection)

            print(original_url)
            print(original_url[0])
            return original_url[0]

        except Exception as e:
            raise Exception(f"Something went wrong on getting short url from DB\n" + str(e))
        
    def add_up_url_visits(self, short_url):        
        """
        This function adds up the visit count of given short-url in db
        :param: short-url
        """
        add_up_visits_query = "update url set visits = visits + 1 where short = '{}'".format(short_url)

        # Creating cursor
        cursor, connection = self.createCursor()

        # Cursor executing the shorten_url_query
        cursor.execute(add_up_visits_query)

        # Commit the transaction
        connection.commit()

        # Close the cursor
        self.closeCursor(cursor)

        # Close the connection
        self.closeConnection(connection)
