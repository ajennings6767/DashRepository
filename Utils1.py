import pandas as pd
import blpapi
import datetime as dt
from optparse import OptionParser
import collections
import sqlite3
import xlwings as xw
from sqlalchemy import create_engine

'''
One function must contain the entire BLPAPI process or else the connection will be severed
'''

class Security(object):
    '''
    Security(series)

    Security(object) is an object that has all of the necessary information needed to pull dat from BBG

    Security Attributes:
        - SecName =
        - SecID =
        - SecIDType =
        - BBG_ID =
        - YellowKey =
        - Country =
        - Country_Code =
        - Tenor =
        - Tenor_Num =
        - Tenor_Unit =
        - Fields =
    '''

    def __init__(self, series):
        '''

        :param series:
        '''
        self.SecName = series.loc['SecName']
        self.SecID = series.loc['SecID']
        self.SecIDType = series.loc['SecIDType']
        self.BBG_ID = series.loc['BBG_ID']
        self.YellowKey = series.loc['YellowKey']
        self.Country = series.loc['Country']
        self.Country_Code = series.loc['Country_Code']
        self.Tenor = series.loc['Tenor']
        self.Tenor_Num = series.loc['Tenor_Num']
        self.Tenor_Unit = series.loc['Tenor_Unit']
        self.Shortable = series.loc['Shortable']
        self.Fields = self.add_fields(series)

    def add_fields(self, series):
        '''
        Turn the fields listed in the series into a dictionary
        :param series:
        :return:
        '''
        indexList = series.index.tolist()
        i = 0
        while 'Field' not in indexList[i]:
            i += 1
        fieldList = indexList[i:]
        field_dict = collections.defaultdict(dict)
        for k in fieldList:
            field_dict[k.split('_')[0]][k.split('_')[1]] = series.loc[k]

        field_dict = self.clean_up_fields(field_dict)
        return field_dict

    def clean_up_fields(self, field_dict):
        '''
        Remove any empty fields from the dictionary
        :param field_dict:
        :return:
        '''
        for k1 in list(field_dict):
            for k2 in list(field_dict[k1]):
                val = field_dict[k1][k2]
                if type(val) == float:
                    del field_dict[k1][k2]
            if bool(field_dict[k1]) == False:
                del field_dict[k1]
        return field_dict

    def __repr__(self):
        pass

    def __str__(self):
        pass

class CIXS(object):

    def __init__(self):
        self.ID
        self.Name
        self.ShortLeg
        self.LongLeg
        self.Fields
        pass





class DBManager(object):

    db = "H:\BOND_TRA\ATJ\Projects\Data\database.db"

    def __init__(self):
        '''

        '''
        self.conn = sqlite3.connect(self.db)
        self.cur = self.conn.cursor()


    def create_new_table(self, security):
        '''

        :param security:
        :return:
        '''
        # print("Create new table")
        new_table = "CREATE TABLE IF NOT EXISTS %s (id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, " % (security.SecID)
        colList = []
        d = sec.Fields
        for key1 in d.keys():
            for key2 in d[key1]:
                if key2 == "Name":
                    val = d[key1][key2]
                    colList.append(val)

        for f in colList:
            st = f + " REAL, "
            new_table += st

        new_table = new_table[:-2]
        new_table = new_table + ")"
        # print(new_table)
        self.cur.execute(new_table)

        return

    # need to add a variable to pass the list of fields that need to be added
    def create_new_field(self, security):
        '''

        :return:
        '''
        # print("Create new field")
        tableName = security.SecID
        field = 'llnmvsdf'
        new_field = "ALTER TABLE " + tableName + " ADD COLUMN " + field + "'float'"
        self.cur.execute(str(new_field))
        return


    def does_table_exist(self, security):
        '''
        Check if a given security exists within the database. If it doesn't, call the create_new_table
        function and pass the security as an argument

        :param security:
        :return:
        '''
        tableName = security.SecID
        self.cur.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=(?)", (tableName,))
        data = self.cur.fetchone()
        exists = data[0]
        if exists == 0:
            self.create_new_table(security)
            exists = 'Table Does Not Exist! New Table Created!'
            # print(exists)
        else:
            exists = 'Table Already Exists!'
            # print(exists)
        return exists


    def does_field_exist(self, security):
        '''
        Check if a given security has a specific field. If not, add that field to the table.
        :return:
        '''
        tableName = security.SecID

        self.cur.execute("SELECT ? FROM sqlite_master", (tableName,))
        data = self.cur.fetchall()
        # for i in data:
        #     print(i)

        # here I need to run through thr gammut of finding all the field names specified by the sec master
        field = "null"
        l = []
        code = "PRAGMA table_info(" + tableName + ")"
        self.cur.execute(str(code))
        data1 = self.cur.fetchall()
        for i in data1:
            l.append(i[1])

        if field not in l:
            self.create_new_field(security)
            exists = 'Field Does Not Exist! New Field Created!'
        else:
            exists = 'Field Already Exists!'
        # print(exists)
        return exists


    def create_Sec_Master(self, tableDF):
        try:
            tableDF.to_sql("secMaster", con=self.conn, index=False, flavor='sqlite', if_exists="fail")
            print("secMaster was created!")
        except ValueError as VE:
            print("secMaster already exists!")
        return


    def update_Sec_Master(self):
        '''
        Ensure that each field end date is updated with the most recent date
        create tables needs to be run first
        :return:
        '''

        """
        select all tables on sec master
        create list of tables
        for each table select fields
        for each field find max date
        find table and field on sec master
        update end date with max date if different
        """


        self.cur.execute("SELECT secMaster.SecID FROM secMaster")
        tupleList = self.cur.fetchall()
        tableList = []
        for i in tupleList:
            tableList.append(i[0])

        print(tableList)

        for table in tableList:
            fieldList = []
            cmd = "PRAGMA table_info(%s)" % table
            self.cur.execute(cmd)
            tempCMD = self.cur.fetchall()
            # print(tempCMD)
            # print(table)
            fieldList = [x[1] for x in tempCMD]
            fieldList.remove('date')
            fieldList.remove('id')
            # print(fieldList)
            for f in fieldList:
                # print(f)
                cmd2 = "SELECT MAX(date) FROM %s WHERE %s IS NOT NULL" % (table, f)
                self.cur.execute(cmd2)
                tempCMD2 = self.cur.fetchall()
                # print(tempCMD2)
                # print("!!")
                for i in tempCMD2:
                    # print(i)
                    # print('!')
                    if i[0] != None:
                        # print("Yo!")
                        fieldNum = self.find_field_name(table, f)
                        # print(i[0] + "!!")
                        self.update_End_Date(fieldNum, table, str(i[0]).replace("-", ""))


        return


    def find_field_name(self, table, f):
        self.cur.execute("SELECT * FROM secMaster WHERE secID LIKE (?)", (table,))
        des = self.cur.description
        query = self.cur.fetchall()
        fieldPos = list(query[0]).index(f)
        colList = [y for y in des]
        colName = colList[fieldPos][0].split("_")[0]
        # print(colName + "!")
        fieldNum = colName[-1]
        # print(fieldNum)
        return fieldNum

    def update_End_Date(self, fieldNum, sec, date):
        field = "Field%s_ED" % fieldNum
        # print(field)
        # print(date)
        cmd = "UPDATE secMaster SET %s = '%s' WHERE secID LIKE '%s'" % (field, str(date), sec)
        # print(cmd)
        self.cur.execute(cmd)
        self.conn.commit()
        print(sec + " " + field + " Updated!")
        return

    def check_if_col_is_empty(self, table, col):
        cmd = "SELECT * FROM %s WHERE %s NOT NULL" % (table, col)
        self.cur.execute(cmd)
        data = self.cur.fetchall()
        # print(data)
        if not data:
            status = "Empty"
        else:
            status = "Active"
        return status


    def delete_duplicate_rows(self, sec, field):
        tableName = sec.SecID
        cmd = "DELETE FROM %s WHERE id NOT IN (SELECT MIN(id) FROM %s GROUP BY date, %s)" % (tableName, tableName, field)
        self.cur.execute(cmd)


    def continue_with_call(self, sec, field):
        cmd = "SELECT MAX(date) FROM %s WHERE %s NOT NULL" % (sec.SecID, field)
        self.cur.execute(cmd)
        ans = self.cur.fetchall()
        # print(ans[0][0])
        date = dt.date.today()
        # print(date)
        if ans[0][0] == str(date):
            # print('Most recent date is today')
            return True
        else:
            # print('Most recent date not today')
            date -= dt.timedelta(days=1)
            while date.weekday() > 4:
                date -= dt.timedelta(days=1)
            # print(str(date) + "-continue")
            if ans[0][0] == str(date):
                # print('Most recent date equal previous business day')
                return True
            else:
                # print('Most recent date not equal to previous business ')
                return False


    def __repr__(self):
        pass

    def __str__(self):
        pass


class CIXSManager(DBManager):

    CIXS_db = "H:\BOND_TRA\ATJ\Projects\Data\CIXS.db"

    def __init__(self):
        self.conn = sqlite3.connect(self.CIXS_db)
        self.cur = self.conn.cursor()

        # self.ShortList = self.getShortList()
        # self.FullList = self.getFullList()

        self.SecEngine = create_engine(r'sqlite:///H:\BOND_TRA\ATJ\Projects\Data\database.db')
        self.CIXSEngine = create_engine(r'sqlite:///H:\BOND_TRA\ATJ\Projects\Data\CIXS.db')

    def does_CIXS_Master_Exist(self):
        '''
        This first checks that the cixsMaster table exists. If not, it is the CIXS are generated and the table is created.
        If the table already exists, we have the option of updating the cixsMaster for new CIXS
        :return:
        '''
        cmd = "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='cixsMaster'"
        self.cur.execute(cmd)
        data = self.cur.fetchone()
        exists = data[0]
        if exists == 0:
            exists = 'cixsMaster Does Not Exist! New Table Created!'
            df = self.create_CIXS_df()
            # TODO add assorted fields to the df (i.e. updated columns, field dictionary etc.)
            df['Create Date'] = today.strftime("%Y-%m-%d")
            df['Active'] = 'Y'
            df['Field1_Name'] = 'Spread'
            print(df.head())
            self.create_CIXS_Master(df)
            print(exists)
        else:
            exists = 'cixsMaster Already Exists!'
            # TODO - create some mechanisms to control the update process for cixsMaster
            df = self.create_CIXS_df()
            # self.update_CIXS_Master(df)
            print(exists)
        return exists


    def create_CIXS_df(self):
        '''
        Created a dataframe containing all possible combinations of securities on the long and short lists. CIXS are given ids
        and have each leg specified
        :return:
        '''

        # TODO add logic to parse dates field
        secMaster = pd.read_sql_table("secMaster", con=self.SecEngine, parse_dates={'Field1_SD': {'format': '%Y%m%d'},
                                                                                    'Field1_ED': {'format': '%Y%m%d'}})
        secMaster = secMaster[secMaster['CIXS_Active'] == 'Y']

        shortList = secMaster.loc[(secMaster['Shortable'] == 'Y') & (secMaster['SecID'] == 'GACGB3')]['SecID'].tolist()

        # shortList = secMaster.loc[(secMaster['Shortable'] == 'Y')]['SecID'].tolist()

        fullList = secMaster['SecID'].tolist()

        df = pd.DataFrame()
        cixsList = [(short + "_" + long) for short in shortList for long in fullList if short != long]
        df['CIXS_ID'] = cixsList
        df['ShortLeg'] = df['CIXS_ID'].str.split('_', expand=True)[0]
        df['LongLeg'] = df['CIXS_ID'].str.split('_', expand=True)[1]
        return df

    def create_CIXS_Master(self, df):
        """
        This simple creates the cixsNaster table from the CIXS df
        :return:
        """

        # create the CIXS_Master table
        try:
            df.to_sql("cixsMaster", con=self.CIXSEngine, index=False, flavor='sqlite', if_exists="fail")
            print("cixsMaster was created!")
        except ValueError as VE:
            print("cixsMaster already exists!")
        return

    def check_CIXS_Master(self, df):
        '''
        Identifies any CIXS that are missing from the cixsMaster
        :param df: CIXS df as created by the function create_CIXS_df
        :return:
        '''
        print('update')
        cixsMaster = pd.read_sql_table("cixsMaster", con=self.CIXSEngine, parse_dates={'Field1_SD': {'format': '%Y%m%d'},
                                                                                       'Field1_ED': {'format': '%Y%m%d'}})
        cixsList = df['CIXS_ID'].tolist()
        masterList = cixsMaster['CIXS_ID'].tolist()

        missingList = [i for i in cixsList if i not in masterList]
        return

    def update_CIXS_Master(self, df):
        self.check_CIXS_Master(df)
        return


    def create_CIXS_Tables(self):
        '''
        Creates a table for each CIXS id as listed on the cixsMaster
        Currently only creates PX_Last field
        :return:
        '''
        # TODO - add field dictionary nonsense to actively read which field to create
        cixsMaster = pd.read_sql_table("cixsMaster", con=self.CIXSEngine, parse_dates={'Field1_SD': {'format': '%Y%m%d'},
                                                                                       'Field1_ED': {'format': '%Y%m%d'}})
        # TODO - add validation of existing tables vs cixsMaster
        masterList = cixsMaster['CIXS_ID'].tolist()
        for i in masterList:
            new_table = "CREATE TABLE IF NOT EXISTS %s (id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, Spread REAL)" % (i)
            self.cur.execute(new_table)

        return

    def CIXS_Update_Table_Data(self):
        '''
        Cycle through a given CIXS fields (calcs) and update them if necessary
        :return:
        '''
        '''
        *1. Get list of all tables that are listed as active
        2. Iterate through list of actives
        3. Generate list of fields
        4. Iterate through fields
        5. Go to field function and calc
        6. Insert new data into table
        7. Clean up data duplicates
        '''
        # Get list of all active CIXS
        self.cur.execute("SELECT CIXS_ID FROM cixsMaster WHERE ACTIVE = 'Y'")
        tupleList = self.cur.fetchall()
        tableList = []
        for i in tupleList:
            tableList.append(i[0])

        # Iterate through list of CIXS
        for table in tableList:
            # Find all the fields (columns) within the table
            cmd = "PRAGMA table_info(%s)" % table
            self.cur.execute(cmd)
            tempCMD = self.cur.fetchall()
            print(table)

            # Clean the tuple and remove date and id columns
            fieldList = [x[1] for x in tempCMD]
            fieldList.remove('date')
            fieldList.remove('id')

            # Iterate though each field in the CIXS table (i.e. Spreads, zScore_7, zScore_30, etc.)
            for f in fieldList:
                print(f)
                if f == "Spread":
                    # Find the most recent date on the table where the field is not null
                    # The field can only be null if we have multiple fields
                    cmd2 = "SELECT MAX(date) FROM %s WHERE %s IS NOT NULL" % (table, f)
                    self.cur.execute(cmd2)
                    tempCMD2 = self.cur.fetchall()

                    for i in tempCMD2:
                        # If a max date exists, try to update the spread data using the max date as the start date
                        if i[0] != None:
                            startDate = i[0]
                            self.CIXS_Update_Table_Spreads(table, f, startDate)

                        # If there is no date (i.e. the table is empty), pull spread data for all available dates
                        else:
                            self.CIXS_New_Table_Spreads(table, f)
                elif 'zScore' in f:
                    '''
                    call zScore calc function and pass the spreads and days as an arg
                    '''
                    print("zscore")
                    days = f.split('_')[1]
                    print('days')
                    df = pd.read_sql_table(table, con=self.SecEngine, parse_dates={'date': {'format': '%Y%m%d'}})
                    self.CIXS_Calc_zScore(df, days)
                else:
                    print('else')
        pass

    def CIXS_New_Table_Spreads(self, tableName, field):
        tableNameList = tableName.split('_')
        shortName = tableNameList[0]
        longName = tableNameList[1]
        shortDF = pd.read_sql_table(shortName, con=self.SecEngine, parse_dates={'date': {'format': '%Y%m%d'}})
        longDF = pd.read_sql_table(longName, con=self.SecEngine, parse_dates={'date': {'format': '%Y%m%d'}})

        df = shortDF.merge(longDF, how="left", on="date")
        df['Spread'] = df["PX_LAST_y"] - df["PX_LAST_x"]
        df = df[['date', 'Spread']]

        df.to_sql(tableName, con=self.CIXSEngine, if_exists='append', index=False)
        self.conn.commit()

    def CIXS_Update_Table_Spreads(self, tableName, fieldName, startDate):
        tableNameList = tableName.split('_')
        shortName = tableNameList[0]
        longName = tableNameList[1]

        shortDF = pd.read_sql_table(shortName, con=self.SecEngine, parse_dates={'date': {'format': '%Y%m%d'}})
        longDF = pd.read_sql_table(longName, con=self.SecEngine, parse_dates={'date': {'format': '%Y%m%d'}})

        df = shortDF.merge(longDF, how="left", on="date")
        df['Spread'] = df["PX_LAST_y"] - df["PX_LAST_x"]
        df = df[['date', 'Spread']]
        df['date'] = pd.to_datetime(df['date'], exact='True', format='%Y-%m-%d')
        df['date'] = df['date'].apply(lambda x: x.date())
        startDate = dt.datetime.strptime(startDate, "%Y-%m-%d")
        startDate = startDate.date()
        df = df[(df['date'] > startDate)]

        df.to_sql(tableName, con=self.CIXSEngine, if_exists='append', index=False)
        self.CIXS_delete_duplicate_rows(tableName, fieldName)
        return

    def CIXS_delete_duplicate_rows(self, tableName, field):
        cmd = "DELETE FROM %s WHERE id NOT IN (SELECT MIN(id) FROM %s GROUP BY date, %s)" % (tableName, tableName, field)
        self.cur.execute(cmd)


    def CIXS_Calc_zScore(self, df, days):
        '''

        :param df:
        :param days:
        :return:
        '''
        '''
        - set df index to 'date'
        - df['Spreads'].interpolate()
        - min date = min date of index + days
        - create list of dates to calc
        -- Filter for dates less than min date
        
        '''
        return

    def CIXS_Calc_High(self, days):
        return

    def CIXS_Calc_Low(self, days):
        return

    def CIXS_Calc_MovingAverage(self, days):
        return

    def CIXS_Master_Add_Field(self, newField, default):
        '''

        :return:
        '''
        # Find all the fields (columns) within the table
        self.cur.execute("PRAGMA table_info(cixsmaster)")
        tempCMD = self.cur.fetchall()
        fieldList = [x[1] for x in tempCMD if "Field" in x[1]]
        fieldList = [(x.split("_")[0]) for x in fieldList]
        fieldList = [int(x[-1]) for x in fieldList]
        maxVal = max(fieldList)
        maxVal += 1
        colName = "Field" + str(maxVal) + "_Name"
        # Decide whether to populate the column with default data
        if default == "No":
            cmd = "ALTER TABLE cixsMaster ADD COLUMN %s TEXT" % colName
        else:
            cmd = "ALTER TABLE cixsMaster ADD COLUMN %s TEXT DEFAULT %s" % (colName, newField)
        self.cur.execute(cmd)
        return

    def CIXS_Table_Refresh_Fields(self):
        '''

        :return:
        '''
        # Read the cixsMaster table into a dataframe
        df = pd.read_sql_table("cixsMaster", con=self.CIXSEngine, parse_dates={'date': {'format': '%Y%m%d'}})
        cmColList = list(df.columns)
        cmColList = [x for x in cmColList if "Field" in x]
        cmColList.insert(0, "CIXS_ID")
        df = df[cmColList]

        tableList = df['CIXS_ID']

        for table in tableList:
            # Find all the fields (columns) within the table
            cmd = "PRAGMA table_info(%s)" % table
            self.cur.execute(cmd)
            tempCMD = self.cur.fetchall()
            fieldList = [x[1] for x in tempCMD]
            fieldList.remove('date')
            fieldList.remove('id')

            # Add table to the list so it is removed in the below list comprehension
            fieldList.append(table)

            # Select the row of the cixsMaster for given CIXS and convert the values to a list
            cmFieldList = df.loc[df['CIXS_ID'] == table].values.tolist()[0]

            # Use list comprehension to identify any missing values
            missingList = [x for x in cmFieldList if x not in fieldList]

            for field in missingList:
                cmd = "ALTER TABLE %s ADD COLUMN %s TEXT" % (table, field)
                self.cur.execute(cmd)
        return

    """
    - Get Lists
        -- Shortable List
        -- Full List
    - Create CIXS Master
    - Create CIXS
        -- Read sec master
        -- Perform combinations
        -- check if exists in CM
            --- if not add and create table
            --- if yes check that table exists create if not exists
    - Check if CIXS exists as a table
        -- Create table
    - Check if CIXS field exists on CIXS table
        -- Create new field
    - Update CM for most recent calc date
    - Identify which dates need to be calced

    - function for each calc
        -- Z score across various intervals
        -- Moving averages across various dates
        -- High/low across various intervals
        
    """

########################################################################################################################
today = dt.date.today()

# Set various variables equal to various features within BLPAPI
# These "names" references keys with the JSON that is returned by BBG after a call
SECURITY_DATA = blpapi.Name("securityData")
SECURITY = blpapi.Name("security")
FIELD_DATA = blpapi.Name("fieldData")
FIELD_EXCEPTIONS = blpapi.Name("fieldExceptions")
FIELD_ID = blpapi.Name("fieldId")
ERROR_INFO = blpapi.Name("errorInfo")


def parseCmdLine():
    parser = OptionParser(description="Retrieve reference data.")
    parser.add_option("-a",
                      "--ip",
                      dest="host",
                      help="server name or IP (default: %default)",
                      metavar="ipAddress",
                      default="localhost")
    parser.add_option("-p",
                      dest="port",
                      type="int",
                      help="server port (default: %default)",
                      metavar="tcpPort",
                      default=8194)

    (options, args) = parser.parse_args()

    return options


def processMessage(session, sec, field):
    while(True):
        ev = session.nextEvent(500)
        for msg in ev:
            # print(msg)
            if ev.eventType() == blpapi.Event.PARTIAL_RESPONSE or ev.eventType() == blpapi.Event.RESPONSE:
                secName = msg.getElement(SECURITY_DATA).getElementAsString(SECURITY)
                fieldDataArray = msg.getElement(SECURITY_DATA).getElement(FIELD_DATA)
                size = fieldDataArray.numValues()
                fieldDataList = [fieldDataArray.getValueAsElement(i) for i in range(0, size)]
                outDates = [x.getElementAsDatetime('date') for x in fieldDataList]
                dateFrame = pd.DataFrame({'date': outDates})
                strData = [field]
                output = pd.DataFrame(columns=strData)
                for strD in strData:
                    outData = [x.getElementAsFloat(strD) for x in fieldDataList]
                    output[strD] = outData
                    output['secID'] = secName
                    output = pd.concat([output], axis=1)
                output = pd.concat([dateFrame, output], axis=1)
        if ev.eventType() == blpapi.Event.RESPONSE:
            break
    return output


def BBG_main(dbm, secList, engine):

    options = parseCmdLine()

    # Fill SessionOptions
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost(options.host)
    sessionOptions.setServerPort(options.port)

    print("Connecting to %s:%s" % (options.host, options.port))
    # Create a Session
    session = blpapi.Session(sessionOptions)

    # Start a Session
    if not session.start():
        print("Failed to start session.")
        # return
    else:
        print('Session Started')

    try:
        # Open service to get historical data from
        if not session.openService("//blp/refdata"):
            print("Failed to open //blp/refdata")
            # return
            # Obtain previously opened service
    except UnboundLocalError as ULE:
        print(ULE)
        pass

    refDataService = session.getService("//blp/refdata")

    count = 0
    for sec in secList:
        print(sec.SecID)
        fields = sec.Fields
        for i in fields:
            print(fields[i])
            freq = fields[i]['Freq']
            field = fields[i]['Name']

            # Check if the most most recent entry is the previous business day or today
            # If today is weekend and most recent entry was the previous business day - continue
            # If today is the most recent entry - continue
            if dbm.continue_with_call(sec, field):
                print("Security is up to date!")
                continue

            # If first time pulling data, should be orig SD; else should be the most recent update date + 1 day
            status = dbm.check_if_col_is_empty(sec.SecID, field)
            if status == "Empty":
                sd = fields[i]['SD']
                print(str(sd) + "-Empty")
            else:
                sd = fields[i]['ED']
                # print(sd)
                try:
                    sd = dt.datetime.strptime(str(sd), '%m/%d/%Y')
                except ValueError as VE:
                    print(VE)
                    pass
                try:
                    sd = dt.datetime.strptime(str(sd), '%Y-%m-%d')
                except ValueError as VE:
                    print(VE)
                    pass
                try:
                    sd = dt.datetime.strptime(str(sd), '%Y%m%d')
                except ValueError as VE:
                    print(VE)
                    pass
                # print(sd)
                # print(type(sd))
                sd += dt.timedelta(days=1)
                sd = sd.strftime('%Y%m%d')
                print(str(sd) + "-SD")

            # This should always be today
            print(str(today) + "-Today")
            ed = today.strftime("%Y%m%d")

            # Create and fill the request for the historical data
            request = refDataService.createRequest("HistoricalDataRequest")

            request.getElement("securities").appendValue(sec.BBG_ID)

            request.getElement('fields').appendValue(field)

            # request.set("periodicityAdjustment", "ACTUAL")
            request.set("periodicitySelection", freq)
            request.set("startDate", sd)
            request.set("endDate", ed)
            # request.set("maxDataPoints", 100)

            # Send the request
            session.sendRequest(request)

            # Process received events
            holdFrame = processMessage(session, sec, field)

            print(holdFrame.head())
            holdFrame.drop(labels="secID", axis=1, inplace=True)

            holdFrame.to_sql(sec.SecID, con=engine, if_exists='append', index=False)
            dbm.delete_duplicate_rows(sec, field)
            dbm.conn.commit()




########################################################################################################################

def initializeAndUpdate():
    # Read the excel sheet into memory as a dataframe
    securityExcelFile = pd.ExcelFile("H:/BOND_TRA/ATJ/Projects/BBG/BBU/test.xlsx")
    security_df = securityExcelFile.parse('Sheet2')

    today = dt.date.today().strftime("%Y%m%d")
    engine = create_engine(r'sqlite:///H:\BOND_TRA\ATJ\Projects\Data\database.db')
    print(today)

    dbm = DBManager()
    # Create a list that will hold all of the security objects as they are created
    secList = []

    # Iterate through the dataframe, row by row
    for row in security_df.iterrows():
        s = row[1]
        sec = Security(s)

        # print(sec.SecName)
        secList.append(sec)

    dbm.create_Sec_Master(security_df)

    # for sec in secList:
    #     dbm.does_table_exist(sec)

    dbm.update_Sec_Master()

    # Run the BBG function
    # Args:

    BBG_main(dbm, secList, engine)

    dbm.update_Sec_Master()

    print("All Done!!!")

    dbm.conn.commit()
    dbm.cur.close()

def testCM():
    cm = CIXSManager()
    cixsDF = cm.createCIXS()
    print(cixsDF.head())


# initializeAndUpdate()










