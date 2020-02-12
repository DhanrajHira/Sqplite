import sqlite3 as sql
import abc
from abc import ABC, abstractmethod


class Sqplite:

    # This is the main database class that is used to store data
    # as sql records. Each object of this class will connect to a single
    # database file and handle a single table at a time (multiple tables planned) 

    onOpenSQL = 'CREATE TABLE IF NOT EXISTS '

    def __init__(self, dbname, onOpen = None):
        # dbname is the name of the sqlite3 file that is created 
        # to store the data
        self.dbconnection = sql.connect(dbname)
        if not onOpen == None:
            cursor = self.connection.cursor()
            print(onOpen.toSQL())
            cursor.execute(onOpen.toSQL())

    @property
    def connection(self):
        # returns the cursor object for the dataconnection
        return self.dbconnection

    def importFromOtherDatabase(self, source, sourcetable, destinationtable):
        # use this method to import records from another database file.
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
        # raw results in the form of tuples is stored 
        # in the rawsourceresults
        sourcecolumnlist = []
        sourcecursor.execute('PRAGMA table_info({0})'.format(sourcetable))
        schema = sourcecursor.fetchall()
        for x in schema:
            sourcecolumnlist.append(x[1])
        # sourcecolumnlist now hold the list of columns in the source 
        # table
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
        # as tablename, the fieldnames arguemwnt is optional and should 
        # be used when inputing records that do not have all the data
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
        for fieldname in fieldnames:
            if fieldname in datamap.keys():
                if type(datamap[fieldname]) is int:
                    values = values+str(datamap[fieldname])+','
                elif datamap[fieldname].isnumeric():
                    values = values+datamap[fieldname]+','
                else:
                    values = values+'\''+datamap[fieldname]+'\''+','
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

    


    def formatRawResults(self, rawResults, tablename = None, columnlist=None):
        # Refactored method of getting the results as a list of dictionaries
        # instead of tuples, the list of tuples containing results are passed in as 
        # rawResults. The tablename arguement is needed to get the fieldnames of the
        # tables from which the results are retrivied so that they can be used as 
        # keys in the dictionaries. Alternatively if you wish to use this as a standalone 
        # method to format results, provide the columnlist arguement and skip the tablename
        # the results are formatted in such a way that the corrersponding element of the 
        # columnlist act as keys for the dictionarys that are returned. 
        # Example the columnlist is provided as ['Name', 'Age] and rawResults are
        # provided as [(19 , 'John')] then the result will look like
        # [{'Name' : 18, Age : 'John'}]  

        if tablename is not None:
            columnnames = self.getColumns(tablename)
        else: 
            if len(columnlist)==0:
                print('Please enter columnlist')
                return None
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
            cursor.execute('SELECT * FROM {0} WHERE {1}'.format(tablename, whereParser(where)))
        resultlist = cursor.fetchall()
        return self.formatRawResults(rawResults=resultlist, tablename=tablename)

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
                if type(newValue[fieldname]) == int:
                    updatestring = updatestring+fieldname + \
                        '='+str(newValue[fieldname])+', '
                else:
                    updatestring = updatestring+fieldname + \
                        '='+'\''+newValue[fieldname]+'\''+', '

        updatestring = updatestring[:-2]
        sqlstring = 'UPDATE {0} SET {1} WHERE {2}'.format(
           tablename, updatestring, whereParser(where))
        cursor.execute(sqlstring)
        self.connection.commit()

    def delete(self, tablename, where):
        # Just pass in the name of the table from which you want to delete the record
        # the where the string is expected in proper sql format
        cursor = self.connection.cursor()
        sqlstring = 'DELETE FROM {0} WHERE {1}'.format(tablename, whereParser(where))
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


class SqlSyntaxGenerator(ABC):

    @abc.abstractmethod
    def toSQL(self):
        pass


class CharField(SqlSyntaxGenerator):
    def __init__(self, name, primary_key = False):
        self.name = name
        self.primary_key = primary_key

    def toSQL(self):
        sqlString = '{0} CHAR '.format(self.name)
        if self.primary_key:
            sqlString = sqlString + ' PRIMARY KEY '

        return sqlString

class IntField(SqlSyntaxGenerator):
    def __init__(self, name, primary_key = False, auto_increment = False):
        self.name = name
        self.primary_key = primary_key 
        self.auto_increment = auto_increment 

    def toSQL(self):
        sqlString = '{0} {1} '.format(self.name, 'INTEGER')
        if self.auto_increment:
            sqlString = sqlString + ' AUTO INCREMENT '
        if self.primary_key :
            sqlString = sqlString + ' PRIMARY KEY '
        return sqlString

class RawSqlWrapper(SqlSyntaxGenerator):
    def __init__(self, sqlString):
        self.sqlString = sqlString

    def toSQL(self):
        return self.sqlString

class Field:

    def __init__(self, fieldname):
        self.fieldname = fieldname

    def isEqualTo(self, value):
        # returns the records which contain fields 
        # whose values are equal to [value]
        if type(value)==str:
            return RawSqlWrapper('{0} = \'{1}\''.format(self.fieldname, value))
        elif type(value) == int:
            return RawSqlWrapper('{0} = {1}'.format(self.fieldname, value))

    def isLike(self, value):
        # matches the substring [value] to the stored value in the 
        # field
        if type(value) == str:
            return RawSqlWrapper('{0} LIKE \'%{1}%\''.format(self.fieldname, value))

    def startsWith(self, value):
        # returns records that start with [value]
        return RawSqlWrapper('{0} LIKE \'{1}%\''.format(self.fieldname, value))

    def endsWith(self, value):
        # returns records that end with [value]
        return RawSqlWrapper('{0} LIKE \'%{1}\''.format(self.fieldname, value))

    def isGreaterThan(self, value):
        return RawSqlWrapper('{0} > {1}'.format(self.fieldname, value))

    def isSmallerThan(self, value):
        return RawSqlWrapper('{0} < {1}'.format(self.fieldname, value))

class All:

    def __init__(self, query_conditions_list):
        self.query_conditions_list = query_conditions_list

    @property
    def query_condition_list(self):
        return self.query_conditions_list

class Any:
    def __init__(self, query_conditions_list):
        self.any_query_conditions_list = query_conditions_list

    @property
    def query_conditions_list(self):
        return self.any_query_conditions_list

def createTable(name, fields):
    definationString = ''
    for field in fields:
        definationString = definationString + field.toSQL()
    return RawSqlWrapper('CREATE TABLE IF NOT EXISTS {0} ({1})'.format(name, definationString))
    
def whereParser(where):
    sqlString = ''
    if type(where) == All:
        for condition in where.query_conditions_list:
            sqlString = sqlString + condition.toSQL() + ' AND '
        sqlString = sqlString[:-4]
        print(sqlString)
        return sqlString

    elif type(where) == Any:
        for condition in where.query_conditions_list:
            sqlString = sqlString + condition.toSQL() + ' OR '
        sqlString = sqlString[:-3]
        print(sqlString)
        return sqlString

    elif type(where) == RawSqlWrapper:
        print(where.toSQL())
        return where.toSQL()