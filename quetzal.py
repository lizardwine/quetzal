import os,sys,pickle
import sqlparse
syntax = ["DEFAULT","CHECK","CONFIG","AI","AUTO","INCREMENT","CREATE","INTEGER","TEXT","REAL","FLOAT","UNIQUE","OBJECT","BOOL","SELECT","INTO","INSERT","DISTINCT","FROM","WHERE","WHILE","DELETE","SET","TO","UPDATE","BY","VALUES","DROP","ADD","ALTER","TABLE"]

#########


INTEGER = "int"
TEXT = "str"
BOOL = "bool"
OBJECT = "class"
REAL = FLOAT = "float"

NOT_NULL = "True"
UNIQUE = "True"


#########

def listdb(directory = os.getcwd()):
    return [".".join(x.split(".")[:-1]) for x in os.listdir(directory) if x.endswith(".qtz")]

def binary_search(arr, x):
    n = len(arr)
    lo = 0
    hi = n - 1
    mid = 0

    while lo <= hi:
        mid = (hi + lo) // 2
        if arr[mid] < x:
            lo = mid + 1
        elif arr[mid] > x:
            hi = mid - 1
        else:
            return mid
    return -1


def upper(L):
    L2 = []
    for i in L:
        L2.append(i.upper() if i.upper() in syntax else i)
    return L2
def IsVar(stream,List):
    return stream in list(List["__sqe__"])

def IsVariable(stream):
    return not stream in ["!=","==",">","<","<=",">=","AND","OR","True","False"] and not list(stream)[0] in ["[","{","(",'"',"0","1","2","3","4","5","6","7","8","9"] and not list(stream)[-1] in ["]","}",")",'"'] and not stream in syntax
def ToString(L):
        ret = ""
        for i in L:
            ret += i
        return ret
def setting(L):
    def tos(L):
        ret = ""
        for i in L:
            ret += '"' + str(i) + '"' + "," if type(i) != str else '"\'' + str(i) + '\'"' +","
        return ret
    ret = list(set(list(eval(tos(L)))))

    ret2 = []
    for i in ret:
        ret2.append(eval(i))
    return ret2


def pop(st,index):
   st = list(st)
   st.pop(index)
   return ToString(st)

def spl(stream):
    sqlparsed = sqlparse.parsestream(stream)
    sql = []
    for i in list(sqlparsed)[0]:
        if str(i) != " ":
            sql.append(str(i).replace("=","==").replace("====","==").replace("<>","!=").replace("!==","!=").replace("<==","<=").replace(">==",">=").replace("&&","AND").replace("||","OR"))
    return sql


def splitter(stream):


    linea = list(stream.strip())
    linea.append(" ")
    operadores = ["!=","==",">","<","<=",">=","AND","OR","True","False","="]
    starts = ["[","{","("]
    ends = {"[":"]","{":"}","(":")"}
    result = []
    largo = len(linea) - 1
    i = 0

    while i<largo:
        while linea[i] == " " and i < largo:
            i+=1

        if i == largo:
            result.append(ToString(linea[i:]))
            break
        if linea[i] in starts:
            needed = []
            c = 2
            app = ""
            j = i
            while len(needed) > 0 or c > 2 or j == i:

                if linea[j] == " ":
                    pass
                elif linea[j] in starts:
                    needed.append(linea[j])
                    app += linea[j]
                elif linea[j] == ends[needed[-1]]:
                    needed.pop(-1)
                    app += linea[j]

                elif linea[j] == '"' and c%2 == 0:
                    c += 1
                    app += linea[j]
                elif linea[j] == '"' and c%2 != 0:
                    c -= 1
                    app += linea[j]
                else:
                    app += linea[j]
                j += 1
            #fin = linea[i +2:].index(ends[linea[i]]) + i + 3
            result.append(app)
            i = j + 1

        else:
            fin = linea[i:].index(" ") + i
            result.append(ToString(linea[i:fin]))
            i = fin


    return result

class connection:
    #2**20 = 1,048,576 bytes
    def __init__(self,file,max_size_ram = 2**20,safe_start = False):
        self.filename = file + ".qtz"
        self.filetmp = file + ".tmp"
        self.__max_size_ram = max_size_ram
        if not os.path.exists(self.filename):
            open(self.filename,"w").write("")
        if os.path.exists(self.filetmp) and not safe_start:
            os.remove(self.filetmp)
        self.data = open(self.filename,"r").read()

        fopen = open(self.filename,"w")
        fopen.write(self.data)

        if self.data == "":
            fopen.write("{}")
            self.data = "{}"
        if sys.getsizeof(self.data) > self.__max_size_ram:
            self.data = None

        fopen.close()
    def __recovery_tmp__(self):
        self.__write_data(self.filename,self.__load_tmp())
    def close(self):
        if os.path.exists(self.filetmp):
            os.remove(self.filetmp)
    def delete_database(self):
        self.data = "{}"
        self.__write_data(self.filename,{})
    def commit(self):
        self.__write_data(self.filename,self.data if self.data != None else self.__load_tmp())
    def __save_tmp(self,data):
        open(self.filetmp,"w").write(str(data))
    def __load_tmp(self):
        data = open(self.filetmp,"r").read()
        os.remove(self.filetmp)
        return eval(data)
    def __write_data(self,file,data):
        open(file,"w").write(str(data))
    def __resize_ram(self,size):
        self.__max_size_ram = size
        if sys.getsizeof(self.data) > self.__max_size_ram:
            self.data = None

    def load_data(self,resize_ram):
        presize_ram = resize_ram if resize_ram > 0 else self.__max_size_ram + 1
        if presize_ram > self.__max_size_ram:
            data = open(self.filename,"r").read() if not os.path.exists(self.filetmp) else self.__load_tmp()
            size = sys.getsizeof(data) if resize_ram == -1 else resize_ram
            if sys.getsizeof(data) <= size:
                self.data = str(data)
                self.__max_size_ram = resize_ram
                return 0
            return 1
        return 2
    def resize_ram(self,resize_ram):
        if self.data == None:
            data = eval(open(self.filename,"r").read()) if not os.path.exists(self.filetmp) else self.__load_tmp()
        else:
            data = eval(self.data)
        sdata = sys.getsizeof(data)
        if resize_ram < sdata:
            self.data = None
            self.__max_size_ram = resize_ram
        else:
            self.__max_size_ram = resize_ram

    def __extract(ls,index,depth):
        def __substract__(ls,index):
            ret = []
            for i in ls:
                if type(i) in (list,tuple):
                    ret.append(i[index])
                else:
                    ret.append(i)
            return ret
        if depth == 1:
            return __substract__(ls,index)
        elif depth > 1:
            ret = __substract__(ls,index)
            for i in range(depth):
                ret = __substract__(ret,index)
            return ret
    def __find(self,L, target):
        start = 0
        end = len(L) - 1

        while start <= end:
            middle = (start + end)//2
            midpoint = L[middle]
            if midpoint > target:
                end = middle - 1
            elif midpoint < target:
                start = middle + 1
            else:
                return middle
        return -1
    def __SORT(self,table):
        if self.data == None:
            data = eval(open(self.filename,"r").read()) if not os.path.exists(self.filetmp) else self.__load_tmp()
        else:
            data = eval(self.data)
        tb = data[table][0]
        tb = list({str(x) for x in tb})
        tb = sorted(tb)
        tb2 = []
        for i in tb:
            tb2.append(eval(i))
        data[table][0] = tb2

        if sys.getsizeof(data) > self.__max_size_ram:
            self.data = None
            self.__save_tmp(str(data))
        else:
            self.data = str(data)

    def __SELECT(self,target,FROM,WHERE = "",WHILE = "",ORDER_BY = "",DISTINCT = False):


        if self.data == None:
            data = eval(open(self.filename,"r").read()) if not os.path.exists(self.filetmp) else self.__load_tmp()
        else:
            data = eval(self.data)
        table,config = data[FROM]
        ret = []
        for i in table:
            if WHILE != "":
                condition = ""
                condition2 = splitter(WHILE)
                for j in condition2:
                    if IsVar(j,config):
                        condition += f"{i[j]}" + " "

                    else:
                        condition += j.lower() + " "

                if not eval(condition):
                    break

            if WHERE == "" and target != "*":
                ret.append(i[target])

            elif WHERE == "" and target == "*":
                ret.append(list(i.values()))

            elif WHERE != "" and target != "*":
                condition = ""
                condition2 = splitter(WHERE)
                for j in condition2:
                    if IsVar(j,config):
                        condition += f"{i[j]}" + " "

                    else:
                        condition += j.lower() + " "


                if eval(condition):
                    ret.append(i[target])

            elif WHERE != "" and target == "*":
                condition = ""
                condition2 = splitter(WHERE)
                for j in condition2:
                    if IsVar(j,config):
                        condition += f"{i[j]}" + " "

                    else:
                        condition += j.lower() + " "


                if eval(condition):
                    ret.append(list(i.values()))

        if DISTINCT:
            ret = setting(ret)
        if ORDER_BY != "":
            if type(config[ORDER_BY][0]) in ["str","bytes"]:
                ret = sorted(ret, key=lambda x: x[config["__sqe__"][ORDER_BY]])
            else:
                ret = sorted(ret)
        return ret
    def __check_data_1(self,table):
        if not type(table) in [str]:
            raise TypeError(f"INTO needs str, but find {type(table)}")
    def __check_data_2(self,data,table,value,column):
        config = data[table][1]
        sqe = config["__sqe__"].copy()
        check = config[column][5]
        checkpass = True if check == "True" else False
        if not checkpass:
            check = splitter(check)

            chk = ""
            for j in check:
                if j == column:



                    chk += (str(value) + " ")

                else:
                    if j == "AND" or j == "OR":
                        chk += j.lower() + " "
                    else:
                        chk += j + " "
            checkpass = eval(chk)

        CleanValue = pop(pop(value,-1),0) if (value.startswith("\"") and value.endswith("\"") if type(value) == str else False) else value
        if CleanValue in list({x[column] for x in data[table][0]}) and eval(config[column][3]):
            CleanValue = CleanValue if not type(CleanValue) == str else f'"{CleanValue}"'
            raise Exception(f"{column} = {CleanValue} already exist in unique row")
        elif not checkpass:
            return [0,False]
        elif (type(CleanValue) == eval(config[column][0] if config[column][0] != "din" else "") or config[column][0] == "din") or (eval(config[column][0]) == int and CleanValue == "?" and eval(config[column][2])) or (eval(config[column][0]) == str) and checkpass:
            return [(str(CleanValue) if eval(config[column][0]) == str else CleanValue) if not (eval(config[column][0]) == int and CleanValue == "?" and eval(config[column][2])) else 1 if len(data[table][0]) == 0 else sorted(list({x[column] for x in data[table][0]}))[-1] + 1,True]

        elif value == None and not eval(config[column][1]):
            return [config[column][4],True]
        elif value == None and eval(config[column][1]):
            raise TypeError(f"Variable \"{column}\" Type can't be \"None\"")
        elif type(value) != eval(config[column][0]):
            raise TypeError(f"Variable \"{column}\" Type can't be \"{type(value)}\"")
    def __INSERT(self,INTO,values,columns = ("",)):
        self.__check_data_1(INTO)
        if type(values) not in [tuple,list]:
            values = (values,)
        if type(columns) not in [tuple,list]:
            columns = (columns,)
        if len(values) != len(columns) and columns != ("",):
            raise Exception(f"lenght of values and columns dont match, len({values}) != len({columns})")

        if self.data == None:
            data = eval(open(self.filename,"r").read()) if not os.path.exists(self.filetmp) else self.__load_tmp()
        else:
            data = eval(self.data)



        config = data[INTO][1]
        sqe = config["__sqe__"].copy()
        if columns == ("",):
            columns = tuple(config["__sqe__"].copy())
        else:
            V2 = []
            L = config.copy()
            L.pop("__sqe__")
            for i in range(len(values)):

                V2.insert(sqe[columns[i]],values[i])
                L.pop(columns[i])
            for i in list(L):
                V2.insert(sqe[i],L[i][4])
            values = V2
        columns = tuple(config["__sqe__"].copy())
        register = {}

        for i in range(len(columns)):
            dat = self.__check_data_2(data,INTO,values[i],columns[i])
            register[columns[i]],flag = dat
            if not flag:
                return
        data[INTO][0].append(register)
        if sys.getsizeof(data) > self.__max_size_ram:
            self.data = None
            self.__save_tmp(str(data))
        else:
            self.data = str(data)
    def __DELETE(self,FROM,WHERE = "True"):
        if self.data == None:
            data = eval(open(self.filename,"r").read()) if not os.path.exists(self.filetmp) else self.__load_tmp()
        else:
            data = eval(self.data)
        table,config = data[FROM]
        pops = []

        for i in range(len(table)):
            condition = ""
            condition2 = splitter(WHERE)

            for j in condition2:

                if IsVar(j,config):

                    if config[j][0] == "str":
                        condition += f"\"{table[i][j]}\"" + " "
                    else:
                        condition += f"{table[i][j]}" + " "
                else:
                    if j.upper() == "TRUE":
                        condition += "True"
                    elif j.upper() == "FALSE":
                        condition += "False"
                    else:
                        condition += j + " "
            if eval(condition):
                pops.append(i)
        pops = sorted(pops)
        pops.reverse()
        for i in pops:
            table.pop(i)
        data[FROM][0] == table
        if sys.getsizeof(data) > self.__max_size_ram:
            self.data = None
            self.__save_tmp(str(data))
        else:
            self.data = str(data)
    def __UPDATE(self,table,SET,TO,WHERE = "True"):
        self.__check_data_1(table)

        if self.data == None:
            data = eval(open(self.filename,"r").read()) if not os.path.exists(self.filetmp) else self.__load_tmp()
        else:
            data = eval(self.data)
        if type(SET) not in [tuple,list]:
            SET = (SET,)
        if type(TO) not in [tuple,list]:
            TO = (TO,)
        if len(TO) != len(SET):
            raise EOFError(f"lenght of values and columns dont match, len({TO}) != len({SET})")
        for i in range(len(data[table][0])):
            c1 = ""
            c2 = splitter(WHERE)
            for j in c2:
                if IsVar(j,data[table][1]):
                    c1 += str(data[table][0][i][j])
                else:
                    c1 += j
            if eval(c1):
                for j in range(len(TO)):
                    dat = self.__check_data_2(data,table,TO[j],SET[j])
                    data[table][0][i][SET[j]],flag = dat
                    if not flag:
                        return
        if sys.getsizeof(data) > self.__max_size_ram:
            self.data = None
            self.__save_tmp(str(data))
        else:
            self.data = str(data)
    def __CREATE_TABLE(self,name,config):
        if self.data == None:
            data = eval(open(self.filename,"r").read()) if not os.path.exists(self.filetmp) else self.__load_tmp()
        else:
            data = eval(self.data)
        if name in list(data):
            raise Exception(f"table {name} already exists")
        if not IsVariable(name):
            raise KeyError(f'"{name}" not is a valid name')
        data[name] = [[],config]

        if sys.getsizeof(data) > self.__max_size_ram:
            self.data = None
            self.__save_tmp(str(data))
        else:
            self.data = str(data)
    def __ALTER_TABLE(self,table,mode,name,i = None):
        if self.data == None:
            data = eval(open(self.filename,"r").read()) if not os.path.exists(self.filetmp) else self.__load_tmp()
        else:
            data = eval(self.data)


        if mode == "DROP":
            for t in range(len(data[table][0])):
                data[table][0][t].pop(name)
            data[table][1].pop(name)
            data[table][1]["__sqe__"].pop(name)
        elif mode == "ADD":

            i = spl(i.replace("(","").replace(")","").replace("\n",""))

            j = sorted(i)
            for www in range(len(j)):
                j[www] = j[www].upper()

            default = None
            if "DEFAULT" in j:
                default = eval(i[upper(i).index("DEFAULT") + 1])

            check = "True"
            if "CHECK" in j:
                check = " ".join(i[upper(i).index("CHECK")+1:])
                count2 = 0
                for i in splitter(check):
                    if IsVariable(i):
                        count2+=1
                    if count2 > 1:
                        raise TypeError("the check cannot contain more than one variable")
            TYPE = eval(i[0].upper())
            if TYPE != "int":
                #the conditional is separated to save resources
                if (binary_search(j,"AI") >= 0 or (binary_search(j,"AUTO") >= 0 and binary_search(j,"INCREMENT") >= 0)):
                    TYPE = "int"
            if type(default) != type(None):
                #the conditional is separated to save resources
                if type(default) != eval(TYPE):
                    raise TypeError("the default type is not equal to the variable's type")
            data[table][1][name] = [TYPE,str(binary_search(j,"NOT NULL") >= 0),str(binary_search(j,"AI") >= 0 or (binary_search(j,"AUTO") >= 0 and binary_search(j,"INCREMENT") >= 0)),str(binary_search(j,"UNIQUE") >= 0),default,check]
            for t in range(len(data[table][0])):
                if binary_search(j,"UNIQUE") == -1:
                    data[table][0][t][name] = default
                elif binary_search(j,"NOT NULL") == -1:
                    data[table][0][t][name] = None
                else:
                    raise TypeError(f"{name} cannot be established because UNIQUE and NOT NULL are True")
            data[table][1]["__sqe__"][name] = max(list(data[table][1]["__sqe__"].values())) + 1

        if sys.getsizeof(data) > self.__max_size_ram:
            self.data = None
            self.__save_tmp(str(data))
        else:
            self.data = str(data)
    def execute(self,stream):
        ret = []
        stream = spl(stream)
        stream = upper(stream)
        stream = " ".join(stream)
        stream = spl(stream)
        if stream[0] == "SELECT":

            WHILE = ""
            for i in range(len(stream)):
                if stream[i].startswith("WHILE"):
                    WHILE = stream[i+1]
                    break

            WHERE = ""
            for i in stream:
                if i.startswith("WHERE"):
                    WHERE = i.replace("WHERE","").strip()
                    break

            ret = self.__SELECT(target = stream[1],FROM = stream[3],WHERE = WHERE,WHILE = WHILE,ORDER_BY = stream[upper(stream).index("BY") + 1] if "BY" in stream else "",DISTINCT = "DISTINCT" in upper(stream))

        elif stream[0] == "SORT":
            self.__SORT(stream[1])
        elif stream[0] == "DELETE":
            for i in stream:
                if i.startswith("WHERE"):
                    break
            else:
                raise Exception("DELETE query need WHERE")
            WHERE = ""
            for i in stream:
                if i.startswith("WHERE"):
                    WHERE = i.replace("WHERE","").strip()
                    break
            self.__DELETE(stream[2],WHERE)
        elif stream[0] == "INSERT":
            columns = ("",)
            c = 0
            if stream[1] != "INTO":
                columns = eval(stream[1]) if stream[1].startswith("(") else (stream[1],)
                c = 1
            table = stream[2 + c]
            s = stream[3+c].split("(")
            s[0] = s[0].upper()
            s = "(".join(s)


            stream[3+c] = s.replace("VALUES","").replace("?",'"?"')

            index = 0
            data = ""
            if self.data == None:
                data = eval(open(self.filename,"r").read()) if not os.path.exists(self.filetmp) else self.__load_tmp()
            else:
                data = eval(self.data)
            sqe = list(data[table][1]["__sqe__"])
            vals = stream[3+c]
            vals = list(vals)
            vals.insert(-1,",")
            vals = "".join(vals)
            VALUES = list(eval(vals))
            for v in VALUES:
                if v == "?":
                    ID = self.execute(f"SELECT {sqe[index]} FROM {table} BY {sqe[index]}")
                    ID = 1 if len(ID) == 0 else ID[-1] + 1
                    v = ID
                    VALUES[index] = v
                index += 1
            stream[3 + c] = str(tuple(VALUES))
            values = eval(stream[3 + c]) if stream[3 + c].startswith("(") else stream[3 + c]
            self.__INSERT(table,values,columns)

        elif stream[0] == "CREATE":

            name = stream[2].replace('"',"")

            config = {}
            count = 0
            config["__sqe__"] = {}
            print(stream)
            for i in (stream[3]).split(";")[:-1]:
                i = spl(i.replace("(","").replace(")","").replace("\n",""))

                j = sorted(i)
                for www in range(len(j)):
                    j[www] = j[www].upper()
                print(j)
                default = None
                if "DEFAULT" in j:
                    default = eval(i[upper(i).index("DEFAULT") + 1])

                check = "True"
                if "CHECK" in j:
                    check = " ".join(i[upper(i).index("CHECK")+1:])
                    count2 = 0
                    for w in splitter(check):
                        if IsVariable(w):
                            count2+=1

                        if count2 > 1:
                            raise TypeError("the check cannot contain more than one variable")

                TYPE = eval(i[1].upper())
                if TYPE != "int":
                    #the conditional is separated to save resources
                    if (binary_search(j,"AI") >= 0 or (binary_search(j,"AUTO") >= 0 and binary_search(j,"INCREMENT") >= 0)):
                        TYPE = "int"
                if type(default) != type(None):
                    #the conditional is separated to save resources
                    if type(default) != eval(TYPE):
                        raise TypeError("the default type is not equal to the variable's type")
                print(binary_search(j,"NOT NULL"))
                config[i[0].replace('"',"")] = [TYPE,str(binary_search(j,"NOT NULL") >= 0),str(binary_search(j,"AI") >= 0 or (binary_search(j,"AUTO") >= 0 and binary_search(j,"INCREMENT") >= 0)),str(binary_search(j,"UNIQUE") >= 0),default,check]
                config["__sqe__"][i[0].replace('"',"")] = count
                count += 1

            self.__CREATE_TABLE(name,config)
        elif stream[0] == "ALTER":
            table = stream[2]
            mode = stream[3]
            name = stream[4]
            Config = None if len(stream) < 6 else stream[5].replace("CONFIG","")
            self.__ALTER_TABLE(table,mode,name,Config)
        elif stream[0] == "UPDATE":
            table = stream[1]
            columns = eval(stream[3].replace("(",'"').replace(")",'"').replace(",",'","')) if stream[3].startswith("(") and stream[3].endswith(")") else eval('"' + stream[3] + '"')
            values = eval(stream[5])
            self.__UPDATE(table,columns,values)
        return ret
squab = connection
