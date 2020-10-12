from .db import Db
from mysql.connector import Error 

class Users():
    msg = "none"
    user = ""
    usertype = ""
    tablename = "users"
    tblusertype = "users_type"
    fldusert = "type"
    fldusertype = "usertype"
    fldpassword = "password"
    fldiduser = "idusers"
    fldid = "id"
    fldisactive = "isactive"
    fldusers = "username,email,password,isactive,usertype,createdon"
    fldinsertsuffix = "1,2,sysdate()"

    def __init__(self):
        self.mydb = Db()
        
    def get_message(self):
        print(self.msg)

    def get_tablename(self):
        return self.tablename

    def set_tablename(self,tbl):
        self.tablename=tbl

    def set_fldusers(self,flds):
        self.fldusers = flds

    def set_tableusertype(self,tbl):
        self.tblusertype = tbl

    def set_fldusert(self,ut):
        self.fldusert = ut

    def set_fldpassword(self,pwd):
        self.fldpassword=pwd

    def set_fldusertype(self,ut):
        self.fldusertype = ut

    def set_fldinsertsuffix(self,flds):
        self.fldinsertsuffix = flds

    def __wrapper_sql(self,value):
        if str(value).isnumeric():
            return value
        else:
            return repr(value)

    def get_userid(self,**kwargs):
        sql = "SELECT " + self.fldiduser + " from " + self.tablename + " where "
        qry = self.__sqlwherepart(**kwargs)
        userid = 0
        sql += qry + ";"
        rs = self.mydb.get_records(sql)
        for x in rs:
            userid = x
        self.msg = userid
        return userid

    def __sqlwherepart(self,**kwargs):
        qry=""
        for key in kwargs:
            if key[:3].lower() == "new":
                pass
            else:
                if key.lower() == self.fldpassword:
                    qry += key + "=sha1(" + self.__wrapper_sql(kwargs[key]) + ") and "
                else:
                    qry += key + "=" + self.__wrapper_sql(kwargs[key]) + " and "
        return qry[:-5] 

    def __sqlinsertpart(self,**kwargs):
        qry="("
        for key in kwargs:
            if key == self.fldpassword:
                qry += "sha1(" + self.__wrapper_sql(kwargs[key]) + "),"
            else:
                qry += self.__wrapper_sql(kwargs[key]) + ","
        qry += self.fldinsertsuffix + ");"
        return qry

    def __sqlupdatepart(self,**kwargs):
        qry=""
        dct={}
        for key in kwargs:
            if key[:3].lower() == "new":
                dct[key[3:]] = kwargs[key]

        for key in dct:
            if key.lower() == self.fldpassword:
                qry += key + "=sha1(" + self.__wrapper_sql(dct[key]) + "),"
            else:
                qry += key + "=" + self.__wrapper_sql(dct[key]) + ","
        return qry[:-1]

    def get_usertype(self,**kwargs):
        sql = "SELECT " + self.fldusert + " from " + self.tblusertype + " where " + self.fldid + " in (SELECT " + self.fldusertype + " from " + self.tablename + " where "
        qry = self.__sqlwherepart(**kwargs)
        
        sql += qry + ");"
        rs = self.mydb.get_records(sql)
        for x in rs:
            self.usertype = x
            self.msg = x
        return self.usertype

    def get_allusers(self):
        sql = "SELECT * from " + self.tablename + ";"
        rs = self.mydb.get_records(sql,False)
        return rs

    def get_user(self,**kwargs):
        sql = "SELECT " + self.fldusers +  " from " + self.tablename + " WHERE " 
        qry = self.__sqlwherepart(**kwargs)
        sql += qry + ";"
        rs = self.mydb.get_records(sql)
        return rs

    def update_user(self,**kwargs):
        sql = "UPDATE " + self.tablename + " SET "
        qry = self.__sqlupdatepart(**kwargs)
        sql += qry + " WHERE "
        qry = self.__sqlwherepart(**kwargs)
        sql += qry + ";"
        self.mydb.update_records(sql)
        self.msg = self.mydb.message

    def active_user(self,**kwargs):
        sql = "SELECT " + self.fldisactive + " FROM " + self.tablename + " WHERE "
        sql += self.__sqlwherepart(**kwargs) + ";"
        rs = self.mydb.get_records(sql)
        if rs[0] == 1:
            return "Active"
        else:
            return "Not Active"

    def activate_user(self,activ=1,**kwargs):
        stat = self.active_user(**kwargs) 
        if stat == "Active" and activ == 1:
            self.msg = "User already active!"
        elif stat == "Not Active" and activ == 0:
            self.msg = "User already not active!"
        else:
            sql = " UPDATE " + self.tablename + " SET " + self.fldisactive + "=" + str(activ) + " WHERE "
            sql += self.__sqlwherepart(**kwargs)
            ms = ""
            if activ == 1:
                ms = "User Account Activated!"
            else:
                ms = "User Account Deactivated!"
            self.mydb.update_records(sql,msg=ms)
            self.msg = self.mydb.message
            

    def register_user(self,**kwargs):
        userid = self.get_userid(**kwargs)

        if userid == 0:
            sql = "INSERT INTO " + self.tablename + " (" + self.fldusers + ")values"
            qry = self.__sqlinsertpart(**kwargs)
            sql += qry
            ms = "Registered Successfully!"
            self.mydb.update_records(sql,msg=ms)
            self.msg =  self.mydb.message
        else:
            self.msg = "User already exist!"
            
        return self.msg
