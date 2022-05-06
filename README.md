# EMS Loader
The EMS Loader can be used to load ABAP Export, csv or Excel files from the File Storage Manager into the EMS.
## Prerequisites
- grant the necessary permissions to the MLWB App-Key. (If your MLWB is called test and App-key with the name: "test (Workbench)" will show up in the permission menus.)
  - (Process Analytics: all if still applicable)
  - Event Collection: all
  - File Storage Manager: Read, List
  - Machine Learning: all
- open a Terminal window (red circle) by lanuching it from the Launcher (blue circle)<br/>![Open Terminal window example](images/10.png?raw=true "Open Terminal window")
- enter git clone https://github.com/jschukat/EMS-Loader.git in the console and run the command<br/>![Run git clone example](images/20.png?raw=true "Run git clone")
- access the newly created EMS folder
## 1. Run EMS Loader
open the RUN_ME.ipynb file and execute the cells displayed (don't worry if it is only one).<br/>
![run RUN_ME.ipynb example](images/30.png?raw=true "run RUN_ME.ipynb")
## 2. Enter URL
- The URL to be entered is the url you can see in the destination Data Pool. Go to the Event Collection
- Choose your Data Pool
- In the Data Pool, if you want to load your data into the global schema, use the Overview URL<br/>![Pool overview example](images/40.png?raw=true "Pool overview")
- Or if it is supposed to go into a connection schema, go to Data Connections and click on the desired target connection, the use the URL depicted<br/>![Connection details example](images/50.png?raw=true "Connection details")
- And paste either URL in the top most text field:<br/>![RUN_ME config example](images/60.png?raw=true "RUN_ME config")



## 3. Last configuration steps
1. agree to the terms
2. skip vs reload:<br/>
if you choose to skip the tables the loader will use pycelonis to connect to the schema and check for tables that are already in the schema. if you choose to reload this check will be skipped and all the tables will be loaded irrespective of them being in the schema.
3. all string vs auto:<br/>
the all string option will, not try to determine data types of csv files but rather load all columns as string. This has the advantage of needing less memory and not leading to errors if the data type a million rows down doesn't deviate from the one determined by pandas. The auto option will try to automatically determine the correct data type.
4. full vs delta:<br/>
the full load will replace already existing tables, delta load will lead to appending the loaded data to already existing tables.
## 4. Done!
**Note**: If you do have any questions, don't hesitate to reach out to the CoE Tiger Team. Especially if you find a bug, don't hesitate to report it so we can fix it ASAP.

