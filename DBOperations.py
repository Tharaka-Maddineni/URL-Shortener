import psycopg2
from pyshorteners import Shortener

class PostGreSQL:
    table_name = "url";
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
        table_column_string = "id_ integer primary key not null, long text not null, short text, TotalHits bigint, HourlyHits bigint"
        try:
            drop_table_query = "drop table if exists %s" % (self.table_name)

            create_table_query = "create table %s (%s)" % (self.table_name, table_column_string)

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

    def get_shorten_url(self, long_url):
        """
        This function stores the original url into database and returns the shortened url
        :param long_url:
        :return: short_url
        """
        try:
            if self.check_if_url_exists(long_url):

                shorten_url_query = "select short from url where long = {}".format(long_url)

                # Creating cursor
                cursor, connection = self.createCursor()

                # Cursor executing the shorten_url_query
                cursor.execute(shorten_url_query)

                # Commit the transaction
                connection.commit()

                # Close the cursor
                self.closeCursor(cursor)

                # Close the connection
                self.closeConnection(connection)

            else:
                shortener_obj = Shortener()
                shorten_url = shortener_obj.tinyurl.short(long_url)

                # Upload or insert long url and short url into database
                self.add_long_short_to_DB(long_url, shorten_url)

            return shorten_url

        except Exception as e:
            raise Exception(f"Something went wrong on getting shortened url\n" + str(e))

    def add_long_short_to_DB(self, long, short):

        """
        This function inserts original url and shortened url to db
        :param long:
        :param short:
        :return:
        """
        count = 1
        query = "insert into {} values({}, {}, {})".format(self.table_name,count,long,short)
        count += 1

    def check_if_url_exists(self, long_url):
        """
        This function checks existence of original url in the
        database name and returns boolean value.
        :param long_url:
        :return: Boolean
        """
        # checks if already long-url exists
        query = "select exists(select long from {} where long='{}');".format(self.table_name,long_url)
        return query
