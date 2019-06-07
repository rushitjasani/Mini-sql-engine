'''
Mini SQL Engine
by : Rushitkumar M Jasani
Roll Number : 2018201034
'''

import os
import sys
import re
import csv
sys.path.insert(0,os.getcwd() + "/sqlparse-0.2.4")
import sqlparse


# Global variables.
metadata = 'metadata.txt'
schema_of_tables = dict()
all_columns_in_sequence = list()
result = list()
natural_join  = list()


# removing duplicate rows in case distinct is useed.
def process_distinct(ans):
    try:
        row = ans.split('\n')
        nr = list()
        for r in row:
            if r not in nr:
                nr.append(r)
        ret = '\n'.join(nr)
    except Exception:
        print "ERROR : syntax error"
        sys.exit()
    return ret

# processing aggregate function if any.
def process_agg_func(column,aggr):
    ans = ""
    for i in xrange(len(column)):
        if aggr[i].lower() == "max":
            ind = select_columns([column[i]])
            temp = list()

            for i in xrange(len(result)):
                temp.append(result[i][ind[0]])
            
            try:
                m = max(temp)
            except ValueError:
                m = 'null'

            ans+=str(m)+"\t"

        elif aggr[i].lower() == "min":
            ind = select_columns([column[i]])
            temp = list()

            for i in xrange(len(result)):
                temp.append(result[i][ind[0]])
            try:
                m = min(temp)
            except ValueError:
                m = 'null'

            ans+=str(m)+"\t"

        elif aggr[i].lower() == "sum":
            ind = select_columns([column[i]])
            temp = list()
            for i in xrange(len(result)):
                temp.append(result[i][ind[0]])
            try:
                m = sum(temp)
            except ValueError:
                m = 'null'
            ans+=str(m)+"\t"

        elif aggr[i].lower() == "avg":
            ind = select_columns([column[i]])
            temp = list()
            for i in xrange(len(result)):
                temp.append(result[i][ind[0]])
            try:
                m = sum(temp)
                m= float(float(m)/len(result))
                m = float("{0:.2f}".format(m))
            except Exception:
                m  = 'null'
            ans+=str(m)+"\t"
        else:
            print "ERROR : aggregate function not recognized."
            sys.exit()
    return ans

# processing natural join bydefault, and store indenticle columns in tuples.
def process_natural_join(condition):
    global natural_join
    try:

        delimiters="and","or"
        regexPattern = '|'.join(map(re.escape, delimiters))+"(?i)"
        con = re.split(regexPattern, condition)
        con = map(str.strip,con)

        for i in xrange(len(con)):
            split = operands(con[i])
            split = map(str.strip,split)

            if '.' in split[0] and '.' in split[1]:
                if split[2].strip() == "==":
                    same = find_col(split[0].strip()),find_col(split[1].strip())
                    natural_join.append(same)

    except Exception:
        print "ERROR : syntax error "
        sys.exit()

# processing that which columns are required in query and returning appropriate headers.
def select_columns(column):

    if len(all_columns_in_sequence) == 0:
        return []

    res_col = list()

    if ''.join(column) == '*':
        column = all_columns_in_sequence
    for col in column:
        try:
            res_col.append(all_columns_in_sequence.index(col))
        except ValueError:
            flag = 0
            search = ""
            for space in all_columns_in_sequence:
                if space.endswith("."+col):
                    flag += 1
                    search = space

            if(flag == 1):
                index = all_columns_in_sequence.index(search)
                res_col.append(index)
            else:
                return []

    if(len(natural_join)>0):
        for i in xrange(len(natural_join)):
            if natural_join[i][0] in res_col and natural_join[i][1] in res_col:
                i1 = res_col.index(natural_join[i][0])
                i2 = res_col.index(natural_join[i][1])
                if i1<i2:
                    del res_col[i2]
                else:
                    del res_col[i1]

    if(len(res_col) == 0):
        print "ERROR : syntax error in columns"
        sys.exit()

    return res_col

# find that the given column name is reside at which index in result table.
def find_col(col):
    ret = -1
    y = 0
    flag = 0

    for space in all_columns_in_sequence:
        if space.endswith("."+col) or space.lower() == col.lower():
            ret = y
            flag = 1
        y+=1
    if flag!=1:
        return -1
    return ret

# finding which relational operator is using in condition.
def find_rel_op(con):
    relop = ""
    i=0

    while i< len(con):

        if con[i] == '<' and con[i+1] == '=':
            relop = "<="
            i+=1
        elif con[i] == '<' and con[i+1] != '=':
            relop = "<"
            i+=1
        elif con[i] == '>' and con[i+1] == '=':
            relop = ">="
            i+=1
        elif con[i] == '>' and con[i+1] != '=':
            relop = ">"
            i+=1
        elif con[i] == '!' and con[i+1] == '=':
            relop = "!="
            i+=1
        elif con[i] == '=' and (con[i+1] != '=' or con[i+1] != '<'
                            or con[i+1] != '>' or con[i+1] != '!'):

            relop = "="
            i+=1
        i+=1
    return relop

# find operands of condition and convert condition into postfix type of list
def operands(con):
    split = list()
    try:
        relop = find_rel_op(con)
        split = con.split(relop)
        split = map(str.strip,split)
        if relop != "=":
            split.append(relop)
        else:
            split.append("==")

    except:
        print "ERROR : syntax error in where condition"
        sys.exit()

    return split

# convert condition on main result and then does evaluation of that condition
# using eval() function.
def process_where(condition):
    try:
        arr = condition.split(" ")
        arr = map(str.strip,arr)
        connector = list()
        for ar in arr:
            if ar.lower().strip() == "and" or ar.lower().strip() == "or":
                connector.append(ar)

        connector = map(str.lower,connector)

        delimiters="and","or"
        regexPattern = '|'.join(map(re.escape, delimiters))+"(?i)"
        con = re.split(regexPattern, condition)
        con = map(str.strip,con)

        for i in xrange(len(con)) :

            split = operands(con[i])
            split = map(str.strip,split)

            lhs = find_col(split[0].strip())
            rhs = find_col(split[1].strip())

            if lhs >-1 and rhs >-1:
                split[0] = split[0].replace(split[0],"result[i]["+str(lhs)+"]")
                split[1] = split[1].replace(split[1],"result[i]["+str(rhs)+"]")

            elif lhs>-1:
                split[0] = split[0].replace(split[0],"result[i]["+str(lhs)+"]")

            else:
                print "ERROR : syntax error"
                sys.exit()

            t = split[0],split[1]
            con[i] = split[2].join(t)

        new_con = con[0]+" "

        x = 0
        for j in con[1:]:
            new_con+= connector[x].lower()+" "
            new_con+= j +" "
            x += 1

        res = list()
        for i in xrange(len(result)):
            if eval(new_con):
                res.append(result[i])

    except Exception:
        print "ERROR : syntax error"
        sys.exit()

    return res

# read table and return result which is list of list.
def read_table(name,distinct):
    name = name + ".csv"
    result = list()

    try:
        reader=csv.reader(open(name),delimiter=',')
    except Exception:
        print "ERROR : table not found."
        sys.exit()

    for row in reader:
        for i in xrange(len(row)):
            if row[i][0] == "\'" or row[i][0] == '\"':
                row[i] = row[i][1:-1]
        row = map(int,row)
        temp = list()

        for r in row:
            temp.append(int(r))

        if distinct == 1:
            if row not in result:
                result.append(row)
        else:
            result.append(row)

    return result

# perform join in tables in query.
def join(table_names,distinct):
    table = read_table(table_names[0],distinct)
    if len(table_names) == 1:
        return table
    else:
        for table_name in table_names[1:]:
            t = read_table(table_name,distinct)
            temp = list()
            for row in table:
                for k in xrange(0,len(t)):
                    temp.append(row+t[k])
            table = temp
    return table

# find all column names of all tables which are there in the query.
def find_queried_columns(table_names):
    query_columns = list()

    for table_name in table_names:
        if table_name in schema_of_tables:
            cols = schema_of_tables[table_name]

            for col_name in cols:
                query_columns.append(table_name +"."+col_name)
        else:
            return []

    return query_columns

# get either column_names or aggreget function and respective function.
# if both are there then throw error.
def get_col_aggr(cols,column,aggr):
    cols = cols.split(",")
    both = 0
    for col in cols:
        c = col.strip()
        if c.lower()[:3] in ["max", "min", "sum", "avg"]:
            aggr.append(c.split("(")[0])
            column.append(c[4:len(c)-1])
            both = 1
        else:
            if both == 1:
                print "ERROR : aggregate columns used with normal columns"
                sys.exit()
            else:
                column.append(c)

# main function which takes query as argument and call respective handlers and find final output.
'''
    command : select 
    column : [T1.x, T2.y] => list
    table_names : T1,T2 => string
    condition : T1.x = a and T2.y > b => string
'''
def process_query(query):
    
    global all_columns_in_sequence,result

    parsed_query = sqlparse.parse(query)[0].tokens
    command = sqlparse.sql.Statement(parsed_query).get_type()

    if command.lower() != 'select':
        print "ERROR: invalid query"
        sys.exit()

    components = list()
    c = sqlparse.sql.IdentifierList(parsed_query).get_identifiers()
    for i in c:
        components.append(str(i))

    dist = 0
    where = 0
    table_names = ""
    condition = ""
    cols = ""
    table_flag = 0
    for component in components:
        if component.lower() == "distinct":
            dist+=1
        elif component.lower() == "from":
            table_flag = 1
        elif table_flag == 1:
            table_names = component
            table_flag = 0
        elif component.lower().startswith('where'):
            where=1
            condition = component[6:].strip()

    if(dist>1):
        print "ERROR : syntax error with the usage of distinct"
        sys.exit()

    if where ==1 and len(condition.strip())==0:
        print "ERROR : syntax error in where clause"
        sys.exit()

    if len(components)> 5 and where ==0 :
        print "ERROR : syntax error"
        sys.exit()

    if len(components)== 5 and where ==0 and dist==0:
        print " ERROR : syntax error"
        sys.exit()

    if dist == 1:
        cols = components[2]
    else:
        cols = components[1]

    column = list()
    aggr = list()
    get_col_aggr(cols,column,aggr)
    
    

    table_names = table_names.split(",")
    table_names = map(str.strip,table_names)
    all_columns_in_sequence = find_queried_columns(table_names)

    result = list()
    heading = ""

    result = join(table_names,dist)

    if condition != "":
        result = process_where(condition)
        if len(aggr)>0:
            co = select_columns(column)
            for i in xrange(len(co)):
                heading+=aggr[i]+"("+all_columns_in_sequence[co[i]]+"),"
            heading = heading[:-1]
            heading = heading+'\n'
        process_natural_join(condition)


    ans = ""
    if len(aggr) == 0:
        res_col = select_columns(column)
        if len(res_col) == 0:
            print "ERROR : result is null "
            sys.exit()
        heading = list()
        for i in res_col:
            heading.append(all_columns_in_sequence[i])
        heading = ",".join(heading)
        heading += '\n'

        for i in xrange(len(result)):
            for j in xrange(len(res_col)):
                ans+=str(result[i][res_col[j]])+"\t"

            ans+='\n'

    else:
        try:
            heading = ""
            co = select_columns(column)
            for i in xrange(len(co)):
                heading+=aggr[i]+"("+all_columns_in_sequence[co[i]]+"),"
            heading = heading[:-1]
            heading = heading+'\n'

            if len(heading)>0:
                ans+=process_agg_func(column,aggr)
            else:
                ans = 'null'
        except IndexError:
            print "Syntax error"

    if dist == 1:
        ans = process_distinct(ans)

    if ans == "":
        print "Empty"
    else:
        print heading+ans


# Reading metadata from file and stores in schema_of_tables dictionary 
# in form of { TABLE_NAME : [ "c1", "c2", "c3" ] }
def read_matadata():
    tablename = ""
    flag = 0
    with open(metadata,"r") as f:
        for line in f:
            if line.strip() == '<begin_table>':
                columns = list()
                flag=1
                continue
            if flag==1:
                tablename=line.strip()
                flag=0
                continue
            if line.strip() == '<end_table>':
                schema_of_tables[tablename] = columns
                continue
            columns.append(line.strip())


def main():
    read_matadata()
    query = sys.argv[1]
    if query[-1] != ";":
        print "ERROR : Semicolon missing"
        sys.exit()
    process_query(query.split(";")[0].strip())


main()