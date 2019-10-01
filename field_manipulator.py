import os
import sys
import datetime
import argparse
import pymongo
import copy
import configparser


#This scripts registers runs to the runDB which
from db_template import DBManager


def main():

   
    parser = argparse.ArgumentParser(description="Field manipulator")

    parser.add_argument('--run-name', dest='run_name', type=str, default=None,
                        help="Specify run name")    
    parser.add_argument('--run-number', dest='run_number', type=str, default=None,
                        help="Specify run number")
    parser.add_argument('--key', dest='key', type=str, default=None,
                        help="Specify the key of the data field which you like to manipulate.\nAlternative use --key show-only")
    parser.add_argument('--value', dest='value', type=str, default=None,
                        help="Specify the value of the data field which you like to manipulate.")
    parser.add_argument('--apply', dest='apply', type=str, default=None,
                        help="Enforce change by adding --apply YES")
    parser.add_argument('--run-type', dest='type', type=str, default=None,
                        help="Specify run type (plugin)")
    parser.add_argument('--run-status', dest='status', type=str, default=None,
                        help="Specify run status")
    parser.add_argument('--run-location', dest='location', type=str, default=None,
                        help="Specify run location")
    parser.add_argument('--run-destination', dest='destination', type=str, default=None,
                        help="Specify run destination")
    parser.add_argument('--run-host', dest='host', type=str, default=None,
                        help="Specify run host")
    parser.add_argument('--database', dest='database', type=str, default='XENON1T',
                        help="Select: XENON1T (standard), XENONnT (not yet implemented), testDB (the test run database)")
    
    args = parser.parse_args()
    
    
    #db selection:
    config = configparser.ConfigParser()
    config.sections()
    config.read('config.ini')
    if args.database not in config.sections():
        print("Your selected choice of MongoDB is not pre-configured in config.ini")
        exit()

    if config[args.database]['password'] == "None":
        #try to get from OS environments (MONGO_PASSWORD)
        pw_ = os.environ['MONGO_PASSWORD']
    else:
        #alternative, use simply what is written in config.ini file
        pw_ = config[args.database]['password']
        
    #set up the database connection:
    db = DBManager()
    db.set_user(config[args.database]['user'])
    db.set_password(pw_)
    db.set_address(config[args.database]['address'])
    db.set_database_name(config[args.database]['database_name'])
    db.set_database_coll(config[args.database]['database_collection'])
    db.dbConnect()
    
    doc = None
    if args.run_name == None and args.run_number != None:
        try:
            doc = db.GetDocByNumber(args.run_number)
        except TypeError as e:
            print("TypeError: {0} for input {1}".format(e, args.run_number))
            exit()
    elif args.run_name != None and args.run_number == None:
        try:
            doc = db.GetDocByName(args.run_name)
        except TypeError as e:
            print("TypeError: {0} for input {1}".format(e, args.run_name))
            exit()
    else:
        print("Specify a correct run name (--run-name) or run number (--run-number)")
    
    if doc == None or len(doc) == 0:
        print("Database search was not successful for {0}/{1}".format(args.run_name, args.run_number))
        exit()
    else:
        print("Found entry {0}/{1} in database".format(doc.get('name'), doc.get('number')))
    
    if doc.get('data') == None or len(doc.get('data')) == 0:
        print("Data field is not there or it has no entries! FIX IT")
        exit()
    else:
        pass
    
    print("\n\n\n")
    print("Manipulate data entry:")
    print("----------------------")
    print()
    
    if args.key == 'status':
        manipulate_status(doc, args)
    elif args.key == 'destination':
        manipulate_destination(doc, args)
    elif args.key == 'location':
        manipulate_location(doc, args)
    elif args.key == 'rse':
        manipulate_rse(doc, args)
    elif args.key == 'show-only':
        show_only(doc, args)
    else:
        print("Choose a valid --key argument to manipulate it...")
        
    exit(0)

def manipulate_rse(doc, args):
    print("--Manipulate rse field")

    #safe key and value first:
    _key = args.key
    _apply = args.apply
    _val = args.value
        
    if args.value == None:
        print("You need to specify the location before hand by --value YOURSTATUS")
        exit(1)
    
    #reduce the input dictinary to important values only:
    reduced_arg = vars(args)
    reduced_arg.pop('run_name', None)
    reduced_arg.pop('run_number', None)
    reduced_arg.pop('key', None)
    reduced_arg.pop('value', None)
    reduced_arg.pop('apply', None)
    
    #Remove all None arguments!
    for i_key in list(reduced_arg.keys()):
        if reduced_arg[i_key] == None:
            del reduced_arg[i_key]
    
    
    count_elements=0
    print("Test data field first:")
    print(" host/type/location/status/destination/rse")
    for i_data in doc['data']:
            
        k = all(item in i_data.items() for item in reduced_arg.items())
        
        if k == False:
            continue
        print("field: ", i_data.get('host'),"/", i_data.get('type'),"/", i_data.get('location'),"/", i_data.get('status'), "/",i_data.get('destination'),"/", i_data['rse'])
        count_elements+=1
    print()
    
    if count_elements!= 1:
        print("Your selection IS NOT unique! -- Select more (or less) --run-XXXXX to make it unique")
        exit(1)
    
    print("Your selection is unique! Going to change the LOCATION into {0}".format(_val))

    #The change itself is done here and is unique to changing the rse
    count=0
    for i_data in doc['data']:
        k = all(item in i_data.items() for item in reduced_arg.items())
        if k == False:
            continue
        k_new = copy.deepcopy(i_data)
        k_new['rse'] = _val
                
        if k_new != i_data:
            print("  Change into new entry:", k_new['rse'])
            if _apply == 'YES':
                db.AddDatafield(doc['_id'], k_new)
                db.RemoveDatafield(doc['_id'], i_data)
                print("  Change done...")
            else:
                print("   -Can not apply changes! (Switch on by --apply YES)")
        count+=1
        if count>=1:
            break
    return 1

def manipulate_location(doc, args):
    print("--Manipulate location field")

    #safe key and value first:
    _key = args.key
    _apply = args.apply
    _val = args.value
        
    if args.value == None:
        print("You need to specify the location before hand by --value YOURSTATUS")
        exit(1)
    
    #reduce the input dictinary to important values only:
    reduced_arg = vars(args)
    reduced_arg.pop('run_name', None)
    reduced_arg.pop('run_number', None)
    reduced_arg.pop('key', None)
    reduced_arg.pop('value', None)
    reduced_arg.pop('apply', None)
    
    #Remove all None arguments!
    for i_key in list(reduced_arg.keys()):
        if reduced_arg[i_key] == None:
            del reduced_arg[i_key]
    
    
    count_elements=0
    print("Test data field first:")
    print(" host/type/location/status/destination")
    for i_data in doc['data']:
            
        k = all(item in i_data.items() for item in reduced_arg.items())
        
        if k == False:
            continue
        print("field: ", i_data.get('host'),"/", i_data.get('type'),"/", i_data.get('location'),"/", i_data.get('status'), i_data.get('destination'))
        count_elements+=1
    print()
    
    if count_elements!= 1:
        print("Your selection IS NOT unique! -- Select more (or less) --run-XXXXX to make it unique")
        exit(1)
    
    print("Your selection is unique! Going to change the LOCATION into {0}".format(_val))

    #The change itself is done here and is unique to changing the status
    count=0
    for i_data in doc['data']:
        k = all(item in i_data.items() for item in reduced_arg.items())
        if k == False:
            continue
        k_new = copy.deepcopy(i_data)
        k_new['location'] = _val
                
        if k_new != i_data:
            print("  Change into new entry:", k_new['location'])
            if _apply == 'YES':
                db.AddDatafield(doc['_id'], k_new)
                db.RemoveDatafield(doc['_id'], i_data)
                print("  Change done...")
            else:
                print("   -Can not apply changes! (Switch on by --apply YES)")
        count+=1
        if count>=1:
            break
    return 1

 
def show_only(doc,args):
    import texttable as tt
    
    print("Show only fields")
    
    tab = tt.Texttable()
    
    headings = ['host', 'location', 'status', 'rse', 'destination']
    
    len_headings = {}
    len_headings_defalut = [15, 40, 12, 30, 30]
    for i in headings:
        len_headings[i] = len(i)
        
    tab.header(headings)

    for i_data in doc['data']:
        
        klist = []
        for j_data in headings:
            klist.append(i_data.get(j_data))
            
            if isinstance(i_data.get(j_data), str) == True and len_headings[j_data] < len(i_data.get(j_data)):
                len_headings[j_data] = len(i_data.get(j_data))
            elif isinstance(i_data.get(j_data), list) == True:
                list_to_str = '\n'.join(i_data.get(j_data))
                if len_headings[j_data] < len(list_to_str):
                    len_headings[j_data] = len(list_to_str)
                    
            elif i_data.get(j_data) == None:
                pass
            else:
                pass
            
        tab.add_row( klist )
        
    
    tab.set_cols_align( ["l" for i in range(len(headings))] )
    #tab.set_cols_width(len_headings.values())
    tab.set_cols_width(len_headings_defalut)
    
    s = tab.draw()
    print (s)
    
def manipulate_destination(doc, args):
    print("--Manipulate destination field")

    #safe key and value first:
    _key = args.key
    _apply = args.apply
    _val = args.value
        
    if args.value == None:
        print("You need to specify the destination before hand by --value YOURSTATUS")
        exit(1)
    
    #before we go on we need to parse the description string:
    if _val == 'empty':
        _val = []
    else:
        _val = []
        for i_dest in args.value.split(","):
            _val.append(i_dest)
        
    #reduce the input dictinary to important values only:
    reduced_arg = vars(args)
    reduced_arg.pop('run_name', None)
    reduced_arg.pop('run_number', None)
    reduced_arg.pop('key', None)
    reduced_arg.pop('value', None)
    reduced_arg.pop('apply', None)
    
    #Remove all None arguments!
    for i_key in list(reduced_arg.keys()):
        if reduced_arg[i_key] == None:
            del reduced_arg[i_key]
    
    
    count_elements=0
    print("Test data field first:")
    print(" host/type/location/status/destination")
    for i_data in doc['data']:
            
        k = all(item in i_data.items() for item in reduced_arg.items())
        
        if k == False:
            continue
        print("field: ", i_data.get('host'),"/", i_data.get('type'),"/", i_data.get('location'),"/", i_data.get('status'), i_data.get('destination'))
        count_elements+=1
    print()
    
    if count_elements!= 1:
        print("Your selection IS NOT unique! -- Select more (or less) --run-XXXXX to make it unique")
        exit(1)
    
    print("Your selection is unique! Going to change the DESTINATION into {0}".format(_val))
    
    #The change itself is done here and is unique to changing the status
    count=0
    for i_data in doc['data']:
        k = all(item in i_data.items() for item in reduced_arg.items())
        if k == False:
            continue
        k_new = copy.deepcopy(i_data)
        k_new['destination'] = _val
                
        if k_new != i_data:
            print("  Change into new entry:", k_new['status'])
            if _apply == 'YES':
                db.AddDatafield(doc['_id'], k_new)
                db.RemoveDatafield(doc['_id'], i_data)
                print("  Change done...")
            else:
                print("   -Can not apply changes! (Switch on by --apply YES)")
        count+=1
        if count>=1:
            break
    return 1
    
def manipulate_status(doc, args):
    print("--Manipulate status field")

    #safe key and value first:
    _key = args.key
    _val = args.value
    _apply = args.apply
    
    if _val == None:
        print("You need to specify the status before hand by --value YOURSTATUS")
        exit(1)
        
    #reduce the input dictinary to important values only:
    reduced_arg = vars(args)
    reduced_arg.pop('run_name', None)
    reduced_arg.pop('run_number', None)
    reduced_arg.pop('key', None)
    reduced_arg.pop('value', None)
    reduced_arg.pop('apply', None)
    
    #Remove all None arguments!
    for i_key in list(reduced_arg.keys()):
        if reduced_arg[i_key] == None:
            del reduced_arg[i_key]
    
    
    count_elements=0
    print("Test data field first:")
    print(" host/type/location/status")
    for i_data in doc['data']:
            
        k = all(item in i_data.items() for item in reduced_arg.items())
        
        if k == False:
            continue
        print("field: ", i_data.get('host'),"/", i_data.get('type'),"/", i_data.get('location'),"/", i_data.get('status'))
        count_elements+=1
    print()
    
    if count_elements!= 1:
        print("Your selection IS NOT unique! -- Select more (or less) --run-XXXXX to make it unique")
        exit(1)
    
    print("Your selection is unique! Going to change the STATUS into {0}".format(_val))
    
    #The change itself is done here and is unique to changing the status
    count=0
    for i_data in doc['data']:
        k = all(item in i_data.items() for item in reduced_arg.items())
        if k == False:
            continue
        k_new = copy.deepcopy(i_data)
        k_new['status'] = _val
                
        if k_new != i_data:
            print("  Change into new entry:", k_new['status'])
            if _apply == 'YES':
                db.AddDatafield(doc['_id'], k_new)
                db.RemoveDatafield(doc['_id'], i_data)
                print("  Change done...")
            else:
                print("   -Can not apply changes! (Switch on by --apply YES)")
        count+=1
        if count>=1:
            break
    return 1

    



    
if __name__ == "__main__":
    main()
