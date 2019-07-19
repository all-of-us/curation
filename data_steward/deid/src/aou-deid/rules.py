"""
    Health Information Privacy Lab
    Brad. Malin, Weiyi Xia, Steve L. Nyemba

    The de-identification code applies to both the projection and filters (relational algebra terminology).
    The code will apply to both relational tables or meta-tables (alternative to relational modeling).
    
    Rules are expressed in JSON format with limited vocabulary and largely based on templates for every persistent data-store
    We have rules stored in one-place and the application of rules in another. This will allow for rules to be able to be shared
    
    Shifting:
        We shift dates given a random function or given a another table that has the number of days for the shift
    Generalization
        Rules for generalization are applied on a projected set of tuples and can have an conditions
    Suppression:
        Suppression rules apply to tuples provided a relational table and/or rows 
    Compute :
        We have added this feature as a result of uterly poor designs we have encountered that has computed fields stored
"""
import numpy as np
import pandas as pd
from parser import Parse
import logging
import json
class Rules :
    # COMPUTE={
    #     "year":"EXTRACT (YEAR FROM :FIELD) AS :FIELD",
    #     "month":"EXTRACT (MONTH FROM :FIELD) AS :FIELD",
    #     "day":"EXTRACT (DAY FROM :FIELD) AS :FIELD",
    #     "id":"SELECT :FIELD FROM :table where :key_field = :key_value"
    # }
    def __init__(self,**args):
        self.cache = {}
        self.store_syntax = {

            "sqlite":{
                "apply":{"REGEXP":"LOWER(:FIELD) REGEXP LOWER(':VAR')","COUNT":"SELECT COUNT(DISTINCT :FIELD) FROM :TABLE WHERE :KEY=:VALUE"},
                "cond_syntax":{"IF":"CASE WHEN","OPEN":"", "THEN":"THEN","ELSE":"ELSE","CLOSE":"END"},
                "random":"random() % 365 "
            },
            "bigquery":{
                "apply":{"REGEXP":"REGEXP_CONTAINS (LOWER(:FIELD), LOWER(':VAR'))","COUNT":"SELECT COUNT (DISTINCT :KEY) FROM :TABLE WHERE :KEY=:VALUE"},
                "cond_syntax":{"IF":"IF","OPEN":"(","THEN":",","ELSE":",","CLOSE":")"},
                "random":"CAST( (RAND() * 364) + 1 AS INT64)"
            },
            "postgresql":{
                
                "cond_syntax":{"IF":"CASE WHEN","OPEN":"","THEN":"THEN","ELSE":"ELSE","CLOSE":"END"},
                "shift":{"date":"FIELD INTERVAL 'SHIFT DAY' ","datetime":"FIELD INTERVAL 'SHIFT DAY'"},
                "random":"(random() * 364) + 1 :: int"
            }
        }
        # self.cache['compute'] = Rules.COMPUTE
        self.pipeline   = args['pipeline']
        self.cache      = args['rules']
        self.parent     = args['parent']
        #--
        
    def set(self,key, id, **args) :
        if key not in ['generalize','suppress','compute','shift'] :
            raise (key + " is Unknown, [suppress,generalize,compute,shift] are allowed")
        if key not in self.cache :
            self.cache[key] = {}
        if id not in self.cache[key] :
            self.cache[key][id] = []
        
        self.cache[key][id].append(args)
        
    def get (self,key,id) :
        return self.cache[key][id]
   
    def validate(self,id,entry):
        """
        Validating if a the application of a rule relative to a table is valid
        """
        p = id in self.cache
        
        # if not p :
        #     return False
        q = []
        # r = []  #-- payload
        
        for row in entry :
            
            if 'rules' in row  :
                if not isinstance(row['rules'],list) and row['rules'].startswith('@')  and "into" in row:
                    #
                    # Making sure the expression is {apply,into} suggesting a rule applied relative to an attribute
                    # finding the rules that need to be applied to a given attribute
                    _id,_key = row['rules'].replace('@','').split('.')
                    q.append( (_id == id) and (_key in self.cache[id]) )
                else :

                    q.append(1)
                # args = dict({"into",row['into']},**{"apply":self.cache[id][_key]})
                # args =  dict({"into":row['into']} ,**{"apply":self.cache[id][_key]})
                # r.append({"pointer":getattr(self,id),"args":args})
            elif 'rules' not in row and isinstance(row,list) or not p:
                #
                # assuming we are dealing with a list of strings applied
                # and dealign with self contained rules
                
                q.append(1)
                # r.append(row['apply'])
            else:
                if 'on' in row and 'values' in row and 'qualifier' in row :
                    q.append(1)
       
        q = sum(q) == len(q)
        
        return (p and q) or (not p and q)

class deid (Rules):
    """
    This class is designed to apply rules to structured data. For this to work we consider the following:
        - a rule can be applied to many fields and tables (broadest sense)
        - a field can have several rules applied to it:

    """
    def __init__(self,**args):
        Rules.__init__(self,**args)

    def validate(self,id,info):
        payload = None
        
        if Rules.validate(self,id,info) :
            
            payload = {}
            payload = {"args":[]}
            payload["pointer"]=getattr(self,id)
            for row in info :
                
                #
                # @TODO: Insure that an error is thrown if the rule associated is not found
                
                p = getattr(Parse,id)(row,self.cache) 
                payload['args'] += [p]   
        
             
        return payload
    def aggregate(self,sql,**args):
        pass
    def log (self,**args):
        # print (args)      
        logging.info(json.dumps(args))  
    def generalize(self,**args):
        """
        This function will apply generalization given a set of rules provided. the rules apply to both meta tables and relational tables
        :fields list of target fields
        :rules  list of rules to be applied
        :label  context of what is being generalized
        """
        fields = args['fields'] if 'fields' in args else [args['value_field']]
        label = args['label']
        rules = args['rules']
        
       
        store_id = args['store'] if 'store' in args else 'sqlite'
        SYNTAX = self.store_syntax[store_id]['cond_syntax']
        APPLY_FN = self.store_syntax[store_id]['apply'] if 'apply' in self.store_syntax[store_id] else {}
        # COND_PREFIX = 'CASE WHEN'
        # COND_SUFFIX = 'END'
        r = {}
        out = []
        label = args['label']
        for name in fields :
            syntax = []
            cond = []
            for rule in rules :
                qualifier = rule['qualifier'] if 'qualifier' in rule else ''
                if 'apply' in rule :
                    #
                    # This will call a built-in SQL function (non-aggregate)'
                    # qualifier = rule['qualifier'] if 'qualifier' in rule else ''
                    filter = args['filter'] if 'filter' in args else name
                    self.log(module='generalize',label=label.split('.')[1],on=name,type=rule['apply'])
                    if 'apply' not in self.store_syntax[store_id] :
                        #
                        #
                        
                        regex =  [rule['apply'],"(",filter, " , '","|".join(rule['values']), "') ",qualifier] 
                        # (rule['values'])
                        # cond += [ " ".join([SYNTAX['IF'],regex])+SYNTAX['OPEN'],SYNTAX['THEN'],_into]
                                      
                    else :

                        TEMPLATE = self.store_syntax[store_id]['apply'][rule['apply']]
                        regex = TEMPLATE.replace(':FIELD',filter).replace(':FN',rule['apply'])
                        if ':VAR' in TEMPLATE :
                            regex = regex.replace(":VAR","|".join(rule['values']))
                        if rule['apply'] in ['COUNT','AVG','SUM'] :
                            #
                            # We are dealing with an aggregate expression. At this point it is important to know what we are counting
                            # count(:field) from :table [where filter]
                            #
                            
                            
                            regex = regex.replace(':TABLE',args['table']).replace(':KEY',args['key_field']).replace(':VALUE',args['value_field'])
                            # regex = regex.replace(':TABLE',args['table']).replace(':KEY',args['key_field']).replace(':VALUE',args['value_field'])
                            
                            if 'on' in rule and 'key_row' in args :
                                
                                if 'qualifier' in rule :                                    
                                    regex += ' AND '+args['key_row'] +  " IN ('"+"','".join(rule['on'])+"')"
                                else:
                                    regex += ' WHERE '+args['key_row'] +  " IN ('"+"','".join(rule['on'])+"')"
                                
                            regex = ' '.join(['(',regex,')',qualifier])
                        else:
                            regex = ' '.join([regex,qualifier])
                        #
                        # Is there a filter associated with the aggregate function or not
                        # 
                        

                    
                    # _into = "".join(["'",rule['into'],"'"])       
                    _into = rule['into'] if 'into' not in args else args['into']
                    
                    if type(_into) == str :                         
                        _into = "'"+_into+"'"                    
                    else:
                        _into = str(_into)
                    regex = "".join(regex)
                    cond += [ " ".join([SYNTAX['IF'],SYNTAX['OPEN'],regex,SYNTAX['THEN'],_into]) ]
                    # cond += [ " ".join([SYNTAX['IF'],regex])+SYNTAX['OPEN'],SYNTAX['THEN'],_into]
                    
                    if rules.index(rule) % 2 == 0 or rules.index(rule) % 3:
                        cond += [SYNTAX['ELSE']]
                        
                    # cond += [_into]
                    # break
                    
                else:
                    #
                    # We are just processing a generalization given a list of values with no overhead of an aggregate function
                    # @TODO: Document what is going on here
                    #   - We are attempting to have an if or else type of generalization given a list of values or function
                    #   - IF <filter> IN <values>, THEN <generalized-value> else <attribute>
                    self.log(module='generalize',label=label.split('.')[1],on=name,type='inline')
                    key_field = args['key_field'] if 'key_field' in args else name
                    filter = args['filter'] if 'filter' in args else name
                    qualifier = rule['qualifier']
                    if not type(rule['values']) == str :
                        values = [str(value) for value in rule['values']]
                        values = '(' + ','.join(values)+')'
                    else:
                        values = "('" + "','".join(rule['values']) +"')"                    
                    
                        
                    statement = " ".join([key_field,qualifier, values])
                    _into = rule['into'] if 'into' not in args else args['into']
                    # common = set("0123456789.") & set(_into)
                    regex = " ".join([filter,qualifier,values])
                    if type(_into) == str :
                        _into = "'"+_into+"'"
                    else:
                        _into = str(_into)
                    cond += [ " ".join([SYNTAX['IF'],SYNTAX['OPEN'],regex,SYNTAX['THEN'],_into]) ]
                    # cond += [ " ".join([SYNTAX['IF'],regex])+SYNTAX['OPEN'],SYNTAX['THEN'],_into]
                    
                    if rules.index(rule) % 2 == 0 or rules.index(rule) % 3:
                        cond += [SYNTAX['ELSE']]
                    pass
        
               
            #
            # Let's build the syntax here to make it sound for any persistence storage
            cond += [name]
            cond_counts = sum([1 for xchar in cond if SYNTAX['IF'] in xchar]) 
            cond += np.repeat(SYNTAX['CLOSE'],cond_counts).tolist()
            cond += ['AS', name]
            # r[name] =  (" " .join(cond))
            result = {"name":name,"apply":" ".join(cond),"label":label}
            if 'on' in args :
                result['on'] = args['on']
            out.append(result)
        #
        # This will return the fields that need generalization as specified.
        #
        
        return out
            
            
        pass
    def suppress(self,**args):
        """
        We should be able to suppress the columns and/or rows provided specification
        NOTE: Non-sensical specs aren't handled for instance
        
        """
        
        rules = args['rules'] if 'rules' in args else {}
       
        label  = args['label']
        fields = args['fields'] if 'fields' in args else []
        
        store_id = args['store']
        APPLY_FN = self.store_syntax[store_id]['apply'] if 'apply' in self.store_syntax[store_id] else {}
        SYNTAX = self.store_syntax[store_id]['cond_syntax']
        cond = []
        rows = {}
        columns = {}
        out = []
        
        if fields and 'on' not in args:
            #
            # This applies on a relational table's columns, it is simple we just nullify the fields
            #
            for name in fields :
                if not rules :
                    #
                    # This scenario, we know the fields upfront and don't have a rule for them
                    # We just need them removed (simple/basic case)
                    #
                    value = ('NULL AS '+name)if '_id' in name else ("'' AS "+name) #-- This will prevent accidental type changing from STRING TO INTEGER 
                    out.append({"name":name,"apply":value,"label":label})
                    self.log(module='suppression',label=label.split('.')[1],type='columns')
                else:
                    #
                    # If we have alist of fields to be removed, The following code will figure out which ones apply
                    # This will apply to all tables that are passed through this engine
                    #

                    for rule in rules :
                        if 'apply' not in rules :
                            
                            if name in rule['values'] :
                                value = ('NULL AS '+name)if '_id' in name else ("'' AS "+name) #-- This will prevent accidental type changing from STRING TO INTEGER 
                                out.append({"name":name,"apply":(value),"label":label})
            self.log(module='suppress',label=label.split('.')[1],on=fields,type='columns')

        else:
            #
            # In this case we are just removing the entire row we will be expecting :
            #   - filter    as the key field to match the filter
            #   - The values of the filter are provided by the rule
            #
            # self.log(module='suppress',label=label.split('.')[1],on='*',type='rows'labellabellabellabellabellabellabellabellabellabellabellabellabellabellabellabellabellabellabellabellabellabellabellabellabellabellabellabellabellabellabellabel)
            APPLY= {'IN':'NOT IN','=':'<>','NOT IN':'IN','<>':'=','':'IS FALSE','TRUE':'IS FALSE'}
            if not rules:
                #
                # A row suppression rule has been provided in the form of an SQL filter
                # For now we assume a simple scenario on="field qualifier ..."
                # The qualifier needs to be flipped ...
                #
                if ' in ' in args['on'] or ' IN ' in args :
                    filter = args['on'].replace(' IN ',' NOT IN ').replace(' in ',' NOT IN ')
                elif ' not in ' in args['on'] or ' NOT IN ' in args :
                    filter = args['on'].replace(' NOT IN ',' IN ').repleace(' not in ',' IN ')
                elif ' = ' in args['on']  :
                    filter = args['on'].replace(' = ',' <> ')
                elif ' <> ' in args['on']  :
                    filter = args['on'].replace(' <> ',' = ')
                filter = {"filter":filter,"label":"suppress.ROWS"}
                found = [ 1*(filter == row) for row in self.cache['suppress']['FILTERS'] ]
                if np.sum(found) == 0:
                    
                    self.cache['suppress']['FILTERS']               +=  [filter]                
                    self.parent.deid_rules['suppress']['FILTERS']   =   self.cache['suppress']['FILTERS']
                return [] #{"filter":filter,"label":"suppress.FILTERS"}]
                
            for rule in rules :
                qualifier = args['qualifier'] if 'qualifier' in args else ''
                
                
                if 'apply' in rule and rule['apply'] in APPLY_FN :
                    
            
                    TEMPLATE = self.store_syntax[store_id]['apply'][rule['apply']]            
                    key_field  = args['filter'] if 'filter' in args else args['on']
                    expression = TEMPLATE.replace(':VAR',"|".join(rule['values']) ).replace(':FN',rule['apply']).replace(':FIELD', key_field)
                    self.cache['suppress']['FILTERS'].append({"filter": expression +' '+ qualifier,"label":label})
                elif 'on' in args:
                    #
                    # If we have no application of a function, we will assume an expression of type <attribute> IN <list>
                    # we have a basic SQL statement here 
                    qualifier = 'IN' if qualifier == '' else qualifier
                    qualifier = APPLY[qualifier]
                    if 'values' in rule :
                        
                        expression   = " ".join([args['on'],qualifier,"('"+ "','".join(rule['values'])+"')"])
                    else:
                        
                        expression = args['on']
                    
                    self.cache['suppress']['FILTERS'].append({"filter": expression ,"label":label})
                    # out.append({"filter": expression +' '+ APPLY[qualifier],"label":label})
                    # self.cache['suppress']['FILTERS'].append({"filter": expression +' '+ APPLY[qualifier],"label":label})
                
        
        return out
        
        
    def shift(self,**args):
        #
        # Shifting will always occur on a column, either the column is specified or is given a field with conditional values
        #   - simply secified are physical date/datetime columns (nothing special here)
        #   - But if we are dealing with a meta table, a condition must be provided. {key,value}
        
        store_id = args['store']  #if 'store' in args else 'sqlite'
        label   = args['label']
        #
        # we can not shift dates on records where filters don't apply
        #
        if not self.cache['suppress']['FILTERS'] :
            return []
       
        SHIFT_CONFIG = self.cache['shift']
        COND_SYNTAX = self.store_syntax[store_id]['cond_syntax']
        # SHIFT_DAYS = 'SELECT shift FROM :idataset.deid_map map_user WHERE map_user.person_id = :table.person_id'
        # SHIFT_DAYS = SHIFT_DAYS.replace(":idataset",self.idataset).replace(":table",self.tablename)
        out = []
        
        if 'fields' in args :
            fields = args['fields']
            
            for name in fields :
                rules = args['rules']
                result = {"apply":rules.replace(':FIELD',name),"label":label,"name":name}
                if 'on' in args :
                    result['on'] = args['on']
                    xchar = ' AS ' if ' AS ' in result['apply'] else ' as '
                    suffix = xchar +result['apply'].split(xchar)[-1]     
                              
                    result['apply'] = ' '.join(['CAST(',result['apply'].replace(suffix,''),'AS STRING ) ',suffix])
                out.append(result)                
              
        else:
            
            key_fields = args['key_field']
            values = args['values']
            value_field = args['value_field']
            #
            # we are dealing with a meta table here 
            #
            pass
        return out

    def compute(self,**args):
        """
        Compute functions are simple applications of aggregate functions on a field (SQL Standard)
        Additionally we add a value from another table thus an embedded query with a table{key_field,key_value,table,field} as folldows:
            field       projection of the embedded query
            key_field   field used in the join/filter
            key_value   external field that will hold the value of key_field
            table       external table
        """
        fields = args['fields'] if 'fields' in args else [args['key_field']]
        value_field = args['value_field']
        # r = {'label':args['label']}
        label = args['label']
        out = []
        
        statement = args['rules'].replace(':FIELD',fields[0]).replace(':value_field',value_field)
        if 'key_field' in args :
            statement = statement.replace(':key_field',args['key_field'])
        if 'table' in args :
            statement = statement.replace(':table',args['table'])
        out .append({"apply":statement, "name":fields[0],"label":label})

        # for name in fields :
        #     if 'from' not in args :
        #         statement = args['rules'].replace(':FIELD',name)
        #         if 'key_field' in args :
        #             statement = statement.replace(':key_field',args['key_field'])
        #         out.append({"name":name,"apply":statement,"label":label})
        #     else:
        #         #
        #         # @TODO Account for conditions ...
        #         table = args['from']
        #         statement = args['rules'].replace(':FIELD',table['field']).replace(':key_field',table['key_field'])
        #         statement = '(' + statement.replace(':key_value',table['key_value']).replace(':table',table['table']) + ') AS '+name
        #         out.append({"name":name,"apply":statement,"label":label})
        return out
    def apply(self,info,store_id = 'sqlite') :
        """
        :info    is a specification of a table and the rules associated
        """
        out = []
        r = {}
        ismeta = info['info']['type'] if 'info' in info and 'type' in info['info'] else False
        
        for id in self.pipeline: #['generalize','compute','suppress','shift'] :
            
            if id in info :
                r =  self.validate(id,info[id])
                
                if r :
                    r = dict(r,**{'ismeta':ismeta})
                    pointer = r['pointer']
                    tmp = [pointer(** dict(args,**{"store":store_id})) for args in r['args']]
                    if tmp :
                        for _item in tmp :
                            if _item :
                                if type(_item) == dict :
                                    out.append(_item)
                                else:
                                    out += _item
                #
                #
                        
        #
        # we should try to consolidate here

        
        return out
# handler = deid()
# handler.set('generalize','race',values=['Native','Middle-Eastern'],into='Other',apply='REGEXP')
# handler.set('generalize','race',values=['Hispanic','Latino'],into='Non Hispanic-Latino',apply='REGEXP', qualifier='IS FALSE')
# handler.set('suppress','noise',values=['sports','story'],apply='IN')

# handler.set('suppress','demographics',values=['zip'])
# handler.set('shift','expiration',values=['expiration'],apply='REGEXP',**{"from":{"table":"seed","shift_field":"shift","key_field":"seed.id","value_field":"sample.id"}})
# # handler.set('generalize','race',aggregate='count(distinct :field) from :dataset.:table where :key_field = :key_value',qualifier='> 1',into='multi-racial')
# info = {
# # "generalize":[{"rules":"@generalize.race","into":"race","filter":"key_field"}],
# # "suppress":[{"rules":"@suppress.noise","fields":["key_field"]},{"fields":['zip','gender']}],
# # "compute":[{"rules":"@compute.year","fields":["dob"]},{"rules":"@compute.id","fields":["id"],"from":{"table":"seed","field":"alt_id","key_field":"id","key_value":"sample.id"}}],
# # "shift":[{"fields":['dob'],'into':'date', "from":{"table":"seed","shift_field":"shift","key_field":"seed.id","value_field":"sample.id"}},{"rules":"@shift.expiration" ,"into":"date","fields":['value'],"filter":"observation_source_value"}]
# }

# print handler.apply({"suppress":[{"rules":"@suppress.demographics","fields":['foo','zoo','zip'],"qualifier":"IN"}]})

# 
# print handler.get('generalize','race')
