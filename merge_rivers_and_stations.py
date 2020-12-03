import pandas as pd
import pandasql as sql
from pathlib import Path


data_folder = Path(r'C:\_data\wiski\2020-11-streamsStorages')
stations = 'RiverModel and StationsCollected.xlsx'
outfile = 'StreamsStorages - merged.output.xlsx'

full_path_infile = data_folder / stations

stations = 'stations'
rivers = 'rivers'


dfStations = pd.read_excel(full_path_infile, sheet_name=stations)
dfRiver = pd.read_excel(full_path_infile, sheet_name=rivers)

# union below because sqllite does not do outer joins ( or right joins)

select = """
 select 
    rivers.[station-txt] ,
    rivers.[raw row] ,
    rivers.[order in river] ,
    rivers.[wra-valley] ,
    rivers.[wra-river] ,
    rivers.[site-catchment] ,
    rivers.[row type] ,
    rivers.[outflow point] ,
    rivers.[description]	 ,
    rivers.[usstationname],
    rivers.[usdistancefromstart] ,
    rivers.[dsstation] ,
    rivers.[dsstationname] ,
    rivers.[dsdistancefromstart] ,
    rivers.[sectionlength],
    stations.[account],
	stations.[siteid],
	stations.[siteid-txt],
	stations.[stname],
	stations.[shortname],
	stations.[stntype],
	stations.[longitude],
	stations.[latitude]
 from dfRiver rivers 
      left join dfStations stations
         on  rivers.[station-txt] = stations.[siteid-txt]
 union all
  select 
    rivers.[station-txt] ,
    rivers.[raw row] ,
    rivers.[order in river] ,
    rivers.[wra-valley] ,
    rivers.[wra-river] ,
    rivers.[site-catchment] ,
    rivers.[row type] ,
    rivers.[outflow point] ,
    rivers.[description]	 ,
    rivers.[usstationname],
    rivers.[usdistancefromstart] ,
    rivers.[dsstation] ,
    rivers.[dsstationname] ,
    rivers.[dsdistancefromstart] ,
    rivers.[sectionlength],
    stations.[account],
	stations.[siteid],
	stations.[siteid-txt],
	stations.[stname],
	stations.[shortname],
	stations.[stntype],
	stations.[longitude],
	stations.[latitude]
 from dfStations stations
      left join dfRiver rivers 
         on  rivers.[station-txt] = stations.[siteid-txt]

   """


merged = sql.sqldf(select, globals())
# overtop.head

merged.to_excel(data_folder / outfile)
