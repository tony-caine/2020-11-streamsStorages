import pandas as pd
import pandasql as sql
from pathlib import Path


data_folder = Path(r'C:\_data\wiski\Excel-Python.dev')
stations = 'statewide_stations.xlsx'
outfile ='Wiski Stream Stations.xlsx'

full_path_infile = data_folder / stations

wiski_sheet_1 = 'Stream Stations (init)'
Hydstra_sheet_2 = 'hydstra 141 stations'
Sections_sheet_3 = 'WRA Stream Sections'


stationsWiski = pd.read_excel(full_path_infile, sheet_name=wiski_sheet_1)
stationsHydstra = pd.read_excel(full_path_infile, sheet_name=Hydstra_sheet_2)
sections = pd.read_excel(full_path_infile, sheet_name=Sections_sheet_3)


select2 ="""
 select 
        "StreamStations" as [Site number],
        "Stream Stations" as [Site name],

        Hydstra.[Station Number],
        Hydstra.[Station Name],
        Hydstra.[Long Name],
        
        Hydstra.[Latitude],
        Hydstra.[Longitude],
        Hydstra.[Station Catchment Area],
        
        Hydstra.[Catchment Number],
        Hydstra.[Catchment Name],
        
        Sections.[Catchment],
        Sections.Stream as Stream,
        Sections.[Section Order] as [Stream Order],
        Sections.Distance as [Distance to Downstream Gauge m],
        Sections.[Downstream StationID], as [DS Gauge Id],
        Sections.[Climate Station] as [Associated Climate Id]
        
 from stationsHydstra Hydstra 
     left join stationsWiski Wiski
         on  Hydstra.[Station number] = Wiski.[Station number]
     left join Sections
         on Hydstra.[Station number] = Sections.[StationID]

   """
   

together=sql.sqldf(select2,globals())
#overtop.head
 
together.to_excel(data_folder / outfile)   