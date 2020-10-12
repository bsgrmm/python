import mysql.connector
from mysql.connector import Error

class Db():
    """ A MySQL Database Object
        It contains multiple db functionalities

        Created by: RMM
    """
    loggedIn = False
    username = ""
    usertype = ""
    message = ""
    fnUsername = ""
    fnPassword = ""

    def __init__(self):
        self.fnUsername = "username"
        self.fnPassword = "password"
        self.connect()

    def getfieldUsername(self):
        return self.fnUsername

    def setfieldUsername(self, fUsername):
        self.fnUsername = fUsername

    def getfieldPassword(self):
        return self.fnPassword

    def setfieldPassword(self,fPassword):
        self.fnPassword = fPassword

    def connect(self,**kwargs):
        """Connect to Mysql Database

        Arguments:
        self    :
        **kwargs: 
        """
        host = ""
        dbname = ""
        username = ""
        password = ""
        for key in kwargs:
            if key.lower() == "host":
                host = kwargs[key]
            elif key.lower() == "database":
                dbname = kwargs[key]
            elif key.lower() == "username":
                username = kwargs[key]
            elif key.lower() == "password":
                password = kwargs[key]
            else:
                pass
        if host.strip() == "":
            host = "localhost"
            dbname = "pos"
            username = "root"
            password = ""

        try:
            self.connection = mysql.connector.connect(host=host,
                                                database=dbname,
                                                user=username,
                                                password=password)
            if self.connection.is_connected():
                self.cursor = self.connection.cursor(buffered=True)
                dbInfo = self.connection.get_server_info()
                self.message="Connected to MySQL Server version " + dbInfo
                return self.connection

        except Error as e:
            self.message="Error while connecting to MySQL. " + e
        
    def disconnect(self):
        """Disconnect to Mysql Database

        Arguments:
        self    : 
        """
        try:
            if (self.connection.is_connected()):
                self.cursor.close()
                self.connection.close()
                self.message="Disconnecting to MYSQL Server...\nMySQL connection is closed"
        except Error as e:
            self.message=e

    def status(self):
        """Show Mysql Database connection status

        Arguments:
        self    :
        """
        print(self.message)

    def update_records(self,sql,msg=""):
        try:
            self.cursor.execute(sql)
            self.connection.commit()
            if msg == "":
                self.message = self.cursor.rowcount, "Record Updated."
            else:
                self.message = self.cursor.rowcount, msg
        except Error as e:
            self.message = e

    def get_records(self,sql,single=True):
        rs = []
        try:
            self.cursor.execute(sql)
            if self.cursor.rowcount>0:
                if single == True:
                    rs = self.cursor.fetchone()
                else:
                    rs = self.cursor.fetchall()
        except Error as e:
            self.message = e
        return rs
    

    def __wrapper_sql(self, value):
        """Wrap value being passed in SQL statement

        Arguments:
        self    :
        value   : 

        Sample usage: wrapper_sql("apple")
        Sample output: 'apple' 
        """
        if str(value).isnumeric():
            return str(value)
        else:
            return repr(value)
 
    def login(self,**kwargs):
        """Login to Db Class

        Arguments:
        self    :
        **kwargs: 

        Sample usage: login(mydb,username="rmm",password="bongbong")
        """
        try:
            if (self.connection.is_connected()):
                uid = self.__getUser(username=kwargs[self.fnUsername])
                if uid[0][0]>0:
                    if uid[0][4]==1:
                        self.loggedIn=True
                        self.username=kwargs[self.fnUsername].upper()
                        self.usertype=self.__getUserType(**kwargs).upper()
                        self.message="Logged in.\t" + self.username + "\t" + self.usertype 
                    else:
                        self.message="Please contact Admin to activate your account!"
                else:
                    self.message="Username and/or Password not valid!"
            else:
                self.message="Could not log in..."
        except Error as e:
            self.message = e

    def logout(self):
        """Logout to Db Class

        Arguments:
        self    :
        
        Sample usage: logout(mydb)
        """
        if self.loggedIn==True:
            self.loggedIn=False
            self.username=""
            self.usertype=""
            self.message="Logged out!"

    def __getAllUsers(self):
        """List all users in POS Database

        Arguments:
        self    :

        Sample usage: getAllUsers(mydb)
        """
        if self.loggedIn==True:
            self.message="Get all users..."
            users = self.__getTable(cond=False)
            return users
        else:
            self.message="You are not Authorized!"

    def __getUser(self,**kwargs):
        """List single user details in POS Database

        Arguments:
        self    :
        **kwargs:

        Sample usage: getUser(mydb,username="rmm",password="bongbong")
        """
        if (self.connection.is_connected()):
            user = self.__getTable(cond=True,**kwargs)
            self.message="Get user..."
            return user
        else:
            self.message="You are not Connected to MYSQL Server!"

    def __getUserType(self,**kwargs):
        """Get user type in POS Database

        Arguments:
        self    :
        **kwargs:

        Sample usage: getUserType(mydb,username="rmm",password="bongbong")
        """
        user = self.__getUser(**kwargs)
        uid = user[0][5]
        rs = self.__getTable(fields="type",table="pos.users_type",cond=True,id=uid)
        self.message="Get user type..."
        return rs[0][0]

    def __activateUser(self,**kwargs):
        """Activate user in POS Database

        Arguments:
        self    :
        **kwargs:

        Sample usage: activateUser(mydb,username="rmm",password="bongbong")
        """
        if self.loggedIn==True:
            if self.username.upper()==kwargs[self.fnUsername].upper():
                self.message="User Currently Logged In."
            else:
                if self.usertype.upper()=="ADMIN":
                    if (self.__isUserActive(**kwargs)):
                        self.message="User is already Active!"
                    else:
                        self.message="Set user to active..."
                        mydct = {"username": kwargs[self.fnUsername]}
                        self.__updateTable(cond=mydct,newisactive=1)
                else:
                    self.message="Only Admin can set user to active!"

    def __isUserActive(self,**kwargs):
        """Check user status in POS Database

        Arguments:
        self    :
        **kwargs:

        Sample usage: isUserActive(mydb,username="rmm",password="bongbong")
        """
        isactive = self.__getUser(username=kwargs[self.fnUsername])
        if isactive[0][4]==1:
            return True
        else:
            return False

    def __setUser(self,**kwargs):
        """Register single user in POS Database

        Arguments:
        self    :
        **kwargs:

        Sample usage: setUser(mydb,username="rmm",password="bongbong")
        """
        if self.loggedIn==True:
            uid = self.__getUser(username=kwargs[self.fnUsername])
            if uid[0][0]==0:
                self.__insertToTable(fields="username,email,password,isactive,usertype,createdon",suf=",1,2,sysdate());",**kwargs)
                self.message="Register user..."
            else:
                self.message="User Already Exist!"
        else:
            self.message="You are not Authorized!"

    def __updateUser(self,**kwargs):
        """Update single user details in POS Database

        Arguments:
        self    :
        **kwargs:

        Sample usage: updateUser(mydb,username="rmm",password="bongbong",newpassword="rmm")
        """
        if self.loggedIn==True:
            if self.username.upper() == kwargs[self.fnUsername].upper():
                self.message="Update Not Allowed! User currently logged in..."
            else:
                uid = self.__getUser(username=kwargs[self.fnUsername])
                
                if uid[0][0]>0:
                    mydct={"username":kwargs[self.fnUsername],"password":kwargs[self.fnPassword]}
                    self.__updateTable(cond=mydct,**kwargs)
                    self.message="Updating user..."
                else:
                    self.message="Update Failed! User does not Exist!"
        else:
            self.message="You are not Authorized!"

    def __updateTable(self,table="pos.users",cond={},**kwargs):
        if self.loggedIn==True:
            sql = "UPDATE " + table + " SET " 
            qry = self.__sqlUpdatePart(**kwargs)
            sql += qry + " WHERE " + self.__sqlWherePart(cond) + ";"
            self.cursor.execute(sql)
            self.connection.commit()
            self.message=self.cursor.rowcount, "Record Updated."
        else:
            print("You are not Authorized!")

    def __insertToTable(self,fields="*",table="pos.users",cond=False,suf=";",**kwargs):
        if self.loggedIn==True:
            if cond == False:
                sql = "INSERT INTO " + table + " (" + fields + ") VALUES ("
                qry = self.__sqlInsertPart(**kwargs)
                sql += qry + suf
            self.cursor.execute(sql)
            self.connection.commit()
            self.message=self.cursor.rowcount, "Record Inserted."
        else:
            self.message="You are not Authorized!"

    def __getTable(self,fields="*",table="pos.users",cond=False,**kwargs):
        if cond == False:
            sql = "SELECT " + fields.strip() + " FROM " + table + ";"
        else:    
            sql = "SELECT " + fields.strip() + " FROM " + table + " where "
            qry = self.__sqlWherePart(**kwargs)
            sql += qry + ";"
        
        rs = []
        self.cursor.execute(sql)
        if self.cursor.rowcount>0:
            rs = self.cursor.fetchall()
        else:
            rs=[(0,)]
        return rs

    def __sqlWherePart(self,dct={},**kwargs):
        qry = ""
        if len(dct)>0:
            for key in dct:
                if key == self.fnPassword:
                    qry += key + "=sha1(" + self.__wrapper_sql(dct[key]) + ") and "
                else:
                    qry += key + "=" + self.__wrapper_sql(dct[key]) + " and "        
        else:
            for key in kwargs:
                if key == self.fnPassword:
                    qry += key + "=sha1(" + self.__wrapper_sql(kwargs[key]) + ") and "
                else:
                    qry += key + "=" + self.__wrapper_sql(kwargs[key]) + " and "
        return qry[:-5]

    def __sqlInsertPart(self,**kwargs):
        if self.loggedIn==True:
            qry = ""
            for key in kwargs:
                if key == self.fnPassword:
                    qry += "sha1(" + self.__wrapper_sql(kwargs[key]) + "),"
                else:
                    qry += self.__wrapper_sql(kwargs[key]) + ","
            return qry[:-1]
        else:
            self.message="You are not Authorized!"

    def __sqlUpdatePart(self,**kwargs):
        if self.loggedIn==True:
            qry = ""
            dct = {}
            for key in kwargs:
                if key[:3] == "new":
                    dct[key[3:]] = kwargs[key]
           
            for key in dct:
                if key == self.fnPassword:
                    qry += key + "=sha1(" + self.__wrapper_sql(dct[key]) + "),"
                else:
                    qry += key + "=" + self.__wrapper_sql(dct[key]) + ","    
            return qry[:-1]
        else:
            self.message="You are not Authorized!"
