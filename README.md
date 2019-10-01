# moderator
a simple commandline MongODb watchER and mAnipulaTOR for XENON


# Usage:
```
(development)[:] python field_manipulator.py --help
usage: field_manipulator.py [-h] [--run-name RUN_NAME]
                            [--run-number RUN_NUMBER] [--key KEY]
                            [--value VALUE] [--apply APPLY] [--run-type TYPE]
                            [--run-status STATUS] [--run-location LOCATION]
                            [--run-destination DESTINATION] [--run-host HOST]
                            [--database DATABASE]

Field manipulator

optional arguments:
  -h, --help            show this help message and exit
  --run-name RUN_NAME   Specify run name
  --run-number RUN_NUMBER
                        Specify run number
  --key KEY             Specify the key of the data field which you like to
                        manipulate. Alternative use --key show-only
  --value VALUE         Specify the value of the data field which you like to
                        manipulate.
  --apply APPLY         Enforce change by adding --apply YES
  --run-type TYPE       Specify run type (plugin)
  --run-status STATUS   Specify run status
  --run-location LOCATION
                        Specify run location
  --run-destination DESTINATION
                        Specify run destination
  --run-host HOST       Specify run host
  --database DATABASE   Select: XENON1T (standard), XENONnT (not yet
                        implemented), testDB (the test run database
```

You can select a run by run number or run name. Changing a field entry in the datafield 
is done by selecting it with --key and the value is defined by --value.

## Narrow down the data selection
The data field is defined as a list of dictionaries. Therefore you are allowed to change only one
dictionary entry at once. You can run selection to narrow down the data selection with:
  * --run-type: The plugin type
  * --run-status: The transferring status the according plugin
  * --run-destination: The destination field of the according plugin
  * --run-host: The host of the according plugin
  
Changes are only applied with the commandline argument --apply YES (mind the capital letters)

## Show only case
Selecting a run by number or name and choose '--key show-only' returns a simple overview table

#Configuration
  1) Rename config_example.ini to config.ini
  2) Adjust your settings for database selection in XENON
  3) You can write the password into the config.ini file or define it 'export MONGO_PASSWORD=yourpassword' in your current bash. You have set the password to "None" in the config.ini file in that case.
  