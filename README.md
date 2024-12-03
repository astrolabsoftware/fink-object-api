# Fink object API

This repository contains the code source of the Fink REST API used to access object data stored in tables in Apache HBase.

## Installation

## Deployment

## Tests

## Profiling a route

To profile a route, simply use:

```bash
./profile_route.sh --route apps/routes/<route>
```

Depending on the route, you will see the details of the timings and a summary similar to:

```python
File: /home/centos/fink-object-api/apps/routes/objects/utils.py
Function: extract_object_data at line 24

Line #      Hits         Time  Per Hit   % Time  Line Contents
==============================================================
    24                                           @profile                                                             
    25                                           def extract_object_data(payload: dict) -> pd.DataFrame:              
    26                                               """Extract data returned by HBase and format it in a Pandas data…
    27                                                                                                                
    28                                               Data is from /api/v1/objects                                     
    29                                                                                                                
    30                                               Parameters                                                       
    31                                               ----------                                                       
    32                                               payload: dict                                                    
    33                                                   See https://fink-portal.org/api/v1/objects                   
    34                                                                                                                
    35                                               Return                                                           
    36                                               ----------                                                       
    37                                               out: pandas dataframe                                            
    38                                               """                                                              
    39         1          1.4      1.4      0.0      if "columns" in payload:                                         
    40                                                   cols = payload["columns"].replace(" ", "")                   
    41                                               else:                                                            
    42         1          0.6      0.6      0.0          cols = "*"                                                   
    43                                                                                                                
    44         1          0.8      0.8      0.0      if "," in payload["objectId"]:                                   
    45                                                   # multi-objects search                                       
    46                                                   splitids = payload["objectId"].split(",")                    
    47                                                   objectids = [f"key:key:{i.strip()}" for i in splitids]       
    48                                               else:                                                            
    49                                                   # single object search                                       
    50         1          2.3      2.3      0.0          objectids = ["key:key:{}".format(payload["objectId"])]       
    51                                                                                                                
    52         1          0.4      0.4      0.0      if "withcutouts" in payload and str(payload["withcutouts"]) == "…
    53                                                   withcutouts = True                                           
    54                                               else:                                                            
    55         1          0.4      0.4      0.0          withcutouts = False                                          
    56                                                                                                                
    57         1          1.5      1.5      0.0      if "withupperlim" in payload and str(payload["withupperlim"]) ==…
    58         1          0.3      0.3      0.0          withupperlim = True                                          
    59                                               else:                                                            
    60                                                   withupperlim = False                                         
    61                                                                                                                
    62         1          0.4      0.4      0.0      if cols == "*":                                                  
    63         1          0.3      0.3      0.0          truncated = False                                            
    64                                               else:                                                            
    65                                                   truncated = True                                             
    66                                                                                                                
    67         1    3241740.4    3e+06     79.7      client = connect_to_hbase_table("ztf")                           
    68                                                                                                                
    69                                               # Get data from the main table                                   
    70         1          0.7      0.7      0.0      results = {}                                                     
    71         2          2.9      1.4      0.0      for to_evaluate in objectids:                                    
    72         2     189018.6  94509.3      4.6          result = client.scan(                                        
    73         1          0.5      0.5      0.0              "",                                                      
    74         1          0.4      0.4      0.0              to_evaluate,                                             
    75         1          0.5      0.5      0.0              cols,                                                    
    76         1          0.4      0.4      0.0              0,                                                       
    77         1          0.4      0.4      0.0              True,                                                    
    78         1          0.2      0.2      0.0              True,                                                    
    79                                                   )                                                            
    80         1      89598.4  89598.4      2.2          results.update(result)                                       
    81                                                                                                                
    82         1       1104.8   1104.8      0.0      schema_client = client.schema()                                  
    83                                                                                                                
    84         2     334126.7 167063.3      8.2      pdf = format_hbase_output(                                       
    85         1          0.5      0.5      0.0          results,                                                     
    86         1          0.6      0.6      0.0          schema_client,                                               
    87         1          0.8      0.8      0.0          group_alerts=False,                                          
    88         1          0.2      0.2      0.0          truncated=truncated,                                         
    89                                               )                                                                
    90                                                                                                                
    91         1          0.5      0.5      0.0      if withcutouts:                                                  
    92                                                   # Default `None` returns all 3 cutouts                       
    93                                                   cutout_kind = payload.get("cutout-kind", "All")              
    94                                                                                                                
    95                                                   if cutout_kind == "All":                                     
    96                                                       cols = [                                                 
    97                                                           "b:cutoutScience_stampData",                         
    98                                                           "b:cutoutTemplate_stampData",                        
    99                                                           "b:cutoutDifference_stampData",                      
   100                                                       ]                                                        
   101                                                       pdf[cols] = pdf[["i:objectId", "i:candid"]].apply(       
   102                                                           lambda x: pd.Series(download_cutout(x.iloc[0], x.ilo…
   103                                                           axis=1,                                              
   104                                                       )                                                        
   105                                                   else:                                                        
   106                                                       colname = "b:cutout{}_stampData".format(cutout_kind)     
   107                                                       pdf[colname] = pdf[["i:objectId", "i:candid"]].apply(    
   108                                                           lambda x: pd.Series(                                 
   109                                                               [download_cutout(x.iloc[0], x.iloc[1], cutout_ki…
   110                                                           ),                                                   
   111                                                           axis=1,                                              
   112                                                       )                                                        
   113                                                                                                                
   114         1          0.3      0.3      0.0      if withupperlim:                                                 
   115         1      71884.1  71884.1      1.8          clientU = connect_to_hbase_table("ztf.upper")                
   116                                                   # upper limits                                               
   117         1          0.9      0.9      0.0          resultsU = {}                                                
   118         2          3.7      1.9      0.0          for to_evaluate in objectids:                                
   119         2      15889.3   7944.6      0.4              resultU = clientU.scan(                                  
   120         1          0.6      0.6      0.0                  "",                                                  
   121         1          0.8      0.8      0.0                  to_evaluate,                                         
   122         1          0.9      0.9      0.0                  "*",                                                 
   123         1          0.7      0.7      0.0                  0,                                                   
   124         1          0.4      0.4      0.0                  False,                                               
   125         1          0.2      0.2      0.0                  False,                                               
   126                                                       )                                                        
   127         1        305.0    305.0      0.0              resultsU.update(resultU)                                 
   128                                                                                                                
   129                                                   # bad quality                                                
   130         1      50285.9  50285.9      1.2          clientUV = connect_to_hbase_table("ztf.uppervalid")          
   131         1          0.8      0.8      0.0          resultsUP = {}                                               
   132         2          2.1      1.1      0.0          for to_evaluate in objectids:                                
   133         2      30262.9  15131.4      0.7              resultUP = clientUV.scan(                                
   134         1          0.6      0.6      0.0                  "",                                                  
   135         1          0.7      0.7      0.0                  to_evaluate,                                         
   136         1          0.4      0.4      0.0                  "*",                                                 
   137         1          0.3      0.3      0.0                  0,                                                   
   138         1          0.3      0.3      0.0                  False,                                               
   139         1          0.2      0.2      0.0                  False,                                               
   140                                                       )                                                        
   141         1        243.3    243.3      0.0              resultsUP.update(resultUP)                               
   142                                                                                                                
   143         1       1729.8   1729.8      0.0          pdfU = pd.DataFrame.from_dict(hbase_to_dict(resultsU), orien…
   144         1       1379.2   1379.2      0.0          pdfUP = pd.DataFrame.from_dict(hbase_to_dict(resultsUP), ori…
   145                                                                                                                
   146         1        564.8    564.8      0.0          pdf["d:tag"] = "valid"                                       
   147         1        514.7    514.7      0.0          pdfU["d:tag"] = "upperlim"                                   
   148         1        462.0    462.0      0.0          pdfUP["d:tag"] = "badquality"                                
   149                                                                                                                
   150         1         33.3     33.3      0.0          if "i:jd" in pdfUP.columns:                                  
   151                                                       # workaround -- see https://github.com/astrolabsoftware/…
   152         2          5.5      2.7      0.0              mask = nparray(                                          
   153         2        595.8    297.9      0.0                  [                                                    
   154                                                               False if float(i) in pdf["i:jd"].to_numpy() else…
   155         1        171.4    171.4      0.0                      for i in pdfUP["i:jd"].to_numpy()                
   156                                                           ]                                                    
   157                                                       )                                                        
   158         1        415.6    415.6      0.0              pdfUP = pdfUP[mask]                                      
   159                                                                                                                
   160                                                   # Hacky way to avoid converting concatenated column to float 
   161         1        482.1    482.1      0.0          pdfU["i:candid"] = -1  # None                                
   162         1        417.1    417.1      0.0          pdfUP["i:candid"] = -1  # None                               
   163                                                                                                                
   164         1      24291.7  24291.7      0.6          pdf_ = pd.concat((pdf, pdfU, pdfUP), axis=0)                 
   165                                                                                                                
   166                                                   # replace                                                    
   167         1          8.9      8.9      0.0          if "i:jd" in pdf_.columns:                                   
   168         1        454.0    454.0      0.0              pdf_["i:jd"] = pdf_["i:jd"].astype(float)                
   169         1       4052.8   4052.8      0.1              pdf = pdf_.sort_values("i:jd", ascending=False)          
   170                                                   else:                                                        
   171                                                       pdf = pdf_                                               
   172                                                                                                                
   173         1       3326.7   3326.7      0.1          clientU.close()                                              
   174         1       1364.8   1364.8      0.0          clientUV.close()                                             
   175                                                                                                                
   176         1       1946.0   1946.0      0.0      client.close()                                                   
   177                                                                                                                
   178         1          0.7      0.7      0.0      return pdf                                                       


  0.00 seconds - /home/centos/fink-object-api/apps/utils/utils.py:49 - download_cutout
  0.00 seconds - /home/centos/fink-object-api/apps/utils/client.py:101 - create_or_update_hbase_table
  0.04 seconds - /home/centos/fink-object-api/apps/utils/decoding.py:175 - extract_rate_and_color
  0.07 seconds - /home/centos/fink-object-api/apps/utils/decoding.py:144 - hbase_to_dict
  0.32 seconds - /home/centos/fink-object-api/apps/utils/client.py:28 - initialise_jvm
  0.33 seconds - /home/centos/fink-object-api/apps/utils/decoding.py:40 - format_hbase_output
  3.36 seconds - /home/centos/fink-object-api/apps/utils/client.py:48 - connect_to_hbase_table
  4.07 seconds - /home/centos/fink-object-api/apps/routes/objects/utils.py:24 - extract_object_data
```

## Adding a new route

You find a [template](apps/routes/template) route to start a new route. Just copy this folder, and modify it with your new route. Alternatively, you can see how other routes are structured to get inspiration. Do not forget to add tests in the [test folder](tests/)!
