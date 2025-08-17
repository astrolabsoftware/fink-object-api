# Recommender system

Here are the first applications for the graph component in Fink.

## Tag-based analysis

Idea: users tag objects. From these tags, they can search for closest objects in the graph. We start from a few tags (the seed), and as we acquire more tags, the search should become more personnalised (from seed to tree).

Question: 
- how many tags are required to get relevant results?
- how many tags should be available?
- how to handle different tags for the same object?
- can we also automatise some tagging mechanism (i.e. from manual tag assignment to automatic science module)?

## Classification transfer

Goal: compare two classification schemes (how C0 and C1 relates to each other).

## Todo

todo:
- [ ] `recommender` route in the API to get similar objects
- [ ] New HBase table to store obj/tags
- [ ] Add tag button in web portal
- [ ] Add tag as classification mechanism based on tags in graph tools
- [ ] Contact pilot teams to test capabilities

## Profiling

```python
Total time: 0.26651 s
File: /opt/fink-object-api/apps/utils/client.py
Function: connect_to_graph at line 60

Line #      Hits         Time  Per Hit   % Time  Line Contents
==============================================================
    60                                           @profile                                                         
    61                                           def connect_to_graph():                                          
    62                                               """Return a client connected to a graph"""                   
    63         1       5578.6   5578.6      2.1      config = extract_configuration("config.yml")                 
    64         1        240.1    240.1      0.1      gateway = JavaGateway(auto_convert=True)                     
    65                                                                                                            
    66         1     256935.3 256935.3     96.4      jc = gateway.jvm.com.Lomikel.Januser.JanusClient(config["PRO…
    67                                                                                                            
    68                                               # TODO: add definition of IP/PORT/TABLE/SCHEMA here in new v…
    69         1       2468.6   2468.6      0.9      gr = gateway.jvm.com.astrolabsoftware.FinkBrowser.Januser.Fi…
    70                                                                                                            
    71         1       1287.6   1287.6      0.5      return gr, gateway.jvm.com.astrolabsoftware.FinkBrowser.Janu…


Total time: 1.60666 s
File: /opt/fink-object-api/apps/routes/v1/recommender/utils.py
Function: extract_similar_objects at line 24

Line #      Hits         Time  Per Hit   % Time  Line Contents
==============================================================
    24                                           @profile                                                         
    25                                           def extract_similar_objects(payload: dict) -> pd.DataFrame:      
    26                                               """Extract similar objects returned by JanusGraph and format…
    27                                                                                                            
    28                                               Data is from /api/v1/recommender                             
    29                                                                                                            
    30                                               Parameters                                                   
    31                                               ----------                                                   
    32                                               payload: dict                                                
    33                                                   See https://api.fink-portal.org                          
    34                                                                                                            
    35                                               Return                                                       
    36                                               ----------                                                   
    37                                               out: pandas dataframe                                        
    38                                               """                                                          
    39         1          1.2      1.2      0.0      if "n" not in payload:                                       
    40         1          0.5      0.5      0.0          nobjects = 10                                            
    41                                               else:                                                        
    42                                                   nobjects = int(payload["n"])                             
    43                                                                                                            
    44         1          0.2      0.2      0.0      if "classifier" not in payload:                              
    45         1          0.2      0.2      0.0          classifier_name = "FINK_PORTAL"                          
    46                                               else:                                                        
    47                                                   classifier_name = payload["classifier"]                  
    48                                                                                                            
    49         1       7097.0   7097.0      0.4      user_config = extract_configuration("config.yml")            
    50                                                                                                            
    51         1     266750.0 266750.0     16.6      gr, classifiers = connect_to_graph()                         
    52                                                                                                            
    53                                               # Classify source                                            
    54         1        377.3    377.3      0.0      func = getattr(classifiers, classifier_name)                 
    55         2    1110560.4 555280.2     69.1      gr.classifySource(                                           
    56         1          0.7      0.7      0.0          func,                                                    
    57         1          1.3      1.3      0.0          payload["objectId"],                                     
    58         2         26.3     13.2      0.0          '{}:{}:{}'.format(                                       
    59         1          1.5      1.5      0.0              user_config["HBASEIP"],                              
    60         1          1.7      1.7      0.0              user_config["ZOOPORT"],                              
    61         1          1.0      1.0      0.0              user_config["SCHEMAVER"]                             
    62                                                   ),                                                       
    63         1          1.0      1.0      0.0          False,                                                   
    64         1          0.6      0.6      0.0          None                                                     
    65                                               )                                                            
    66                                                                                                            
    67         2     206912.5 103456.3     12.9      closest_sources = gr.sourceNeighborhood(                     
    68         1          2.3      2.3      0.0          payload["objectId"],                                     
    69         1          0.7      0.7      0.0          classifier_name                                          
    70                                               )                                                            
    71         1          3.0      3.0      0.0      out = {"i:objectId": [], "v:distance": [], "v:classification…
    72         1        871.1    871.1      0.1      for index, (oid, distance) in enumerate(closest_sources.item…
    73                                                   if index > nobjects:                                     
    74                                                       break                                                
    75                                                                                                            
    76                                                   r = requests.post(                                       
    77                                                       "https://api.fink-portal.org/api/v1/objects",        
    78                                                       json={                                               
    79                                                           "objectId": oid,                                 
    80                                                           "output-format": "json"                          
    81                                                       }                                                    
    82                                                   )                                                        
    83                                                   out["v:classification"].append(r.json()[0]["v:classifica…
    84                                                   out["i:objectId"].append(oid)                            
    85                                                   out["v:distance"].append(distance)                       
    86                                                                                                            
    87         1       1211.7   1211.7      0.1      pdf = pd.DataFrame(out)                                      
    88                                                                                                            
    89         1      12835.5  12835.5      0.8      gr.close()                                                   
    90                                                                                                            
    91         1          3.8      3.8      0.0      return pdf                                                   


  0.27 seconds - /opt/fink-object-api/apps/utils/client.py:60 - connect_to_graph
  1.61 seconds - /opt/fink-object-api/apps/routes/v1/recommender/utils.py:24 - extract_similar_objects
```
