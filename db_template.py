#Author: Boris Bauermeister
#Email: Boris.Bauermeister@gmail.com
#Simple MongoDB access class specified to readout and manipluate a data field.
#HANDLE WITH CARE

import pymongo
import os
import json

class DBManager():
    def __init__(self):
        self.db_mongodb_user=None
        self.db_mongodb_pw=None
        self.taddress=None
        self.db_extern = None
        self.db_projection = {"_id":True, "data": True, "name": True, "number":True, "start":True, 'detector': True}
        self.db_name = None
        self.db_coll = None
        self.path = './'
        
    def set_user(self, user):
        self.db_mongodb_user = user
    
    def set_password(self, password):
        self.db_mongodb_pw = password
        
    def set_address(self, adr):
        self.taddress = adr
        
    def set_dump_path(self, path):
        #path to dump the database entry what is going to be overwritten by your changes
        self.path = path
        if not os.path.exists(self.path):
            os.makedirs(self.path)
    
    def set_database_name(self, name):
        self.db_name = name
        
    def set_database_coll(self, coll):
        self.db_coll = coll
    
    def dump(self, run_name, entry):
        with open(os.path.join(self.path, 'dmp_{0}.json'.format(run_name)), 'w') as fp:
                json.dump(entry, fp, default=str)

    def load(self,run_name):
        with open(os.path.join(self.path, 'dmp_{0}.json'.format(run_name)), 'r') as fp:
            datastore = json.load(fp)
        return datastore

    def dbConnect(self):
        db_mongodb_string = f'mongodb://{self.db_mongodb_user}:{self.db_mongodb_pw}@{self.taddress}'
        p = pymongo.MongoClient(db_mongodb_string)
        self.db_extern = p[self.db_name][self.db_coll]
    
    def GetAllRuns(self):
        query = {}
        own_projection = {'_id':True, 'name':True, 'number':True, 'start':True, 'detector':True}
        return list(self.db_extern.find(query, projection=own_projection ))
    
    def GetAllBasicProjection(self):
        query = {}
        return list(self.db_extern.find(query, projection=self.db_projection ))
    
    def GetRunByID(self, run_id):
        query = {"_id": run_id}
        return list(self.db_extern.find(query, projection=self.db_projection ))
    
    def GetDocByName(self, name, detector='tpc'):
        try:
            return list(self.db_extern.find( {"name":name, "detector":detector}, projection=self.db_projection))[0]
        except:
            return []
    
    def GetDocWith(self, data_field_marker=None):
        
        coll = self.db_extern.aggregate(
                            [
                                    {"$project": {"name":1,
                                                  "number": 1,
                                                  "data":1}},
                                    {"$unwind": "$data"},
                                                  #"host": "$data.host",
                                                  #"location": "$data.location",
                                                  #"status": "$data.status",
                                                  #}
                                                  
                                    {"$match": { "data.host": "rucio-catalogue"}},
                            ]
                        )
        return coll
        
    
    def GetDocByNumber(self, number, detector='tpc'):
        try:
            return list(self.db_extern.find( {"number":number, "detector":detector}, projection=self.db_projection))[0]
        except:
            return []

    def GetDataByName(self, name, detector='tpc'):
        
        return list(self.db_extern.find( {"name":name, "detector":detector}, projection=self.db_projection))[0]['data']

    def GetDataByNumber(self, number, detector='tpc'):
        return list(self.db_extern.find( {"number":int(number), "detector":detector}, projection=self.db_projection))[0]['data']

    def UpdateData(self, name, old=None, new=None):
        
        k = list(self.db_extern.find( {"name":name}, projection=self.db_projection))[0]
        for ik in k['data']:
            if ik == old:
                print(self.db_extern.pupp)
        
        return 1  
    
    def RemoveDatafield(self, id_field, rem_dict, _dump=True):
        run = self.GetRunByID(id_field)[0]

        old_data = run['data']
        
        if _dump==True:
            self.dump( run['name'], old_data)
        
        for i_d in old_data:
            print("-", i_d.get('host'), i_d.get('location'))
        print("-")
        for i_d in old_data:
            if i_d == rem_dict:
                print("remove:", i_d.get('host'), i_d.get('location'), )
                index_tmp = old_data.index(rem_dict)
                del(old_data[index_tmp])

        for i_d in old_data:
            print("-", i_d.get('host'), i_d.get('location'))

        self.db_extern.find_one_and_update({"_id": id_field},
                                        {"$set": {"data": old_data}})

    def AddDatafield(self, id_field, new_dict):
        run = self.GetRunByID(id_field)[0]

        old_data = run['data']
        old_data.append(new_dict)

        #print("NEW", old_data)
        self.db_extern.find_one_and_update({"_id": id_field},
                                        {"$set": {"data": old_data}})
        
    def ShowDataField(self, id_field, type=None, host=None):
        #Test if a dictionary exists in a list for a specific combination of type and host
        run = self.GetRunByID(id_field)[0]

        for i_run in run['data']:
            if i_run['type'] != type:
                continue
            if i_run['host'] != host:
                continue
            print(" [-> ", i_run['type'], "/", i_run['host'], " <-]")
            if 'rse' in i_run:
                print(" [-> ", i_run['rse'])
            else:
                print(" [->  No RSE information")
            if 'status' in i_run:
                print(" [-> ", i_run['status'])
            else:
                print(" [->  No status information")
            if 'meta' in i_run:
                print(" [-> ", i_run['meta'])
            else:
                print(" [->  No meta information")
    def SetDataField(self, id_field, type=None, host=None,
                     key=None, value=None, new=False):
        #This function is database entry specific:
        #It changes a list (data) which contains dictionaries
        #In particular you can change here the destination field
        run = self.GetRunByID(id_field)[0]

        new_data = run['data']
        if type != None and host!=None and key != None:
            for i_run in new_data:
                if i_run['type'] != type:
                    continue
                if i_run['host'] != host:
                    continue
                if i_run['type'] == type and i_run['host'] == host and key in i_run:
                    i_run[key] = value
                if i_run['type'] == type and i_run['host'] == host and new==True:
                    i_run[key] = value

        self.db_extern.find_one_and_update({"_id": id_field},
                                        {"$set": {"data": new_data}})
