import sqlite3 as sql


class Sqplite:

    # This is the main database class that we will be using to store data
    # as sql records. Each object of this class will connect to a single
    # database file and handle a single table at a time (I intend to extend
    # it to support more than one table later)
    # This may seem like a hassle right now but the code will a lot more readable
    # and maintainable when everything comes together.

    # Just a static string to make life a bit simpler when I come around to implement
    # multiple onOpen table creation commands
    onOpenSQL = 'CREATE TABLE IF NOT EXISTS '

    def __init__(self, dbname, onOpen):
        self.dbconnection = sql.connect(dbname)
        if not onOpen == '':
            cursor = self.connection.cursor()
            cursor.execute(self.onOpenSQL+onOpen)

    @property
    def connection(self):
        # returns the cursor object for the dataconnection
        return self.dbconnection

    def ImportfromOtherDatabase(self, source, sourcetable, destinationtable):
        # use this method to import records from another database file
        # the database file name and the table to copy from are provided
        # as the source and sourcetable arguements while the table to copy to
        # is provided as the destinationtable
        # this method automatically handles the fieldnames
        # the columns present in the source but not in the target table are skipped
        # while the columsn present in the target but not the source are
        # entered as None.

        sourceconnection = sql.connect(source)
        sourcecursor = sourceconnection.cursor()
        sourcecursor.execute('SELECT * FROM {0}'.format(sourcetable))
        rawsourceresults = sourcecursor.fetchall()
        sourcecolumnlist = []
        sourcecursor.execute('PRAGMA table_info({0})'.format(sourcetable))
        schema = sourcecursor.fetchall()
        for x in schema:
            sourcecolumnlist.append(x[1])

        sourceresults = []
        for raw in rawsourceresults:
            datamap=dict()
            for index in range(len(raw)):
                datamap[sourcecolumnlist[index]] = raw[index]
            sourceresults.append(datamap)
        destinationfieldlist = self.getColumns(destinationtable)
        for resulttoinsert in sourceresults:
            self.insert(tablename=destinationtable, fieldnames=destinationfieldlist, datamap=resulttoinsert, batch = True)
        self.connection.commit()


    def insert(self, tablename, datamap, fieldnames = None, batch = False):
        # The name of the table in which the values are to be inserted is passed
        # as tablename, the fieldnames arguwmwnt is optional and should ideally
        # only be used when inputing custom records that do not have all the data
        # required by the table.
        # The data should be passed as a dictionary with keys as the column names
        # and the values as values of the respective keys
        # batch should be used when you are inserting many records at once
        # batching helps speedup the inserting process as the time consuming
        # file I/O is only executed sequentailly after all the operation are buffered
        # in the memory, If you choose to batch the inserts, you need to call the commit
        # method on the connection object
        # Example : [Sqplite object].connection.commit()

        cursor = self.connection.cursor()
        if fieldnames == None:
            fieldnames = self.getColumns(tablename)

        values = ''
        for key in fieldnames:
            if key in datamap.keys():
                if datamap[key].isnumeric():
                    values = values+datamap[key]+','
                else:
                    values = values+'\''+datamap[key]+'\''+','
            else:
                continue
        values = values[:-1]

        fieldnamesstring = ''
        for field in fieldnames:
            if field in datamap.keys():
                fieldnamesstring = fieldnamesstring+field+','
        fieldnamesstring = fieldnamesstring[:-1]
        sqlstring = 'INSERT INTO {0} ({1}) VALUES ({2})'.format(
            tablename, fieldnamesstring, values)
        cursor.execute(sqlstring)
        if batch == False:
            self.connection.commit()

    def formatRawResults(self, rawResults, tablename, columnlist=''):
        # Refactored method of getting the results as a list of dictionaries
        # instead of tuples, earlier this was handled by the query method

        columnnames = self.getColumns(tablename)
        resultlist = []
        for recordtuple in rawResults:
            datamap = dict()
            for fieldvalueindex in range(len(recordtuple)):
                datamap[columnnames[fieldvalueindex]
                        ] = recordtuple[fieldvalueindex]
            resultlist.append(datamap)
        return resultlist

    def query(self, tablename, where = None):

        # Pass the tablename and the where condition (in sql)
        # returns the result as a dictionary with the column names
        # as keys and the values as the corresponding values of the
        # dictionary. The where arguement should be given an sql command
        # string.
        # Example where = 'Id = 24'


        cursor = self.connection.cursor()
        if where == None:
            cursor.execute('SELECT * FROM {0}'.format(tablename))
        else:
            sqlstring = 'SELECT * FROM {0} WHERE {1}'.format(tablename, where)
            cursor.execute(sqlstring)
        results = cursor.fetchall()
        return self.formatRawResults(rawResults=results, tablename=tablename)

    def update(self, tablename, newValue, where, fieldnames = None):

        # pass the tablename, the fieldnames as list , newValue should be
        # passed as a map similar to how it is done while inserting a record
        # this means that you need to pass the complete a new data object when
        # updating a specific record. It can be understood as if this method
        # takes in a new object and replaces the previous object in the database
        # (identified by the where parameter) with the one passed in
        # (newValue parameter)
        if fieldnames == None:
            fieldnames = self.getColumns(tablename)

        cursor = self.connection.cursor()
        updatestring = ''
        for fieldname in fieldnames:
            if fieldname in newValue.keys():
                if newValue[fieldname].isnumeric():
                    updatestring = updatestring+fieldname + \
                        '='+newValue[fieldname]+','
                else:
                    updatestring = updatestring+fieldname + \
                        '='+'\''+newValue[fieldname]+'\''+', '

        updatestring = updatestring[:-2]
        sqlstring = 'UPDATE {0} SET {1} WHERE {2}'.format(
            tablename, updatestring, where)
        cursor.execute(sqlstring)
        self.connection.commit()

    def delete(self, tablename, where):
        # Just pass in the name of the table from which you want to delete the record
        # the where the string is expected in proper sql format
        cursor = self.connection.cursor()
        sqlstring = 'DELETE FROM {0} WHERE {1}'.format(tablename, where)
        cursor.execute(sqlstring)
        self.connection.commit()

    def getColumns(self, tablename):
        # returns a list of column names in the table passed as tablename
        cursor = self.connection.cursor()
        columnlist = []
        cursor.execute('PRAGMA table_info({0})'.format(tablename))
        schema = cursor.fetchall()
        for x in schema:
            columnlist.append(x[1])
        return columnlist

    def execute(self, sqlcommand):
        # this simply exposes the cursor object and executes raw sql commands
        # as a method of the class. Ideally, this should never be used except while
        # debugging the program as all the transactions should be handled by specific
        # methods of the class
        cursor = self.connection.cursor()
        cursor.execute(sqlcommand)