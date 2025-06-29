# bufr_filter_gts
A python3 toolbox for filtering GTS messages in BUFR format

This toolbox implements a simple python3 module for filtering BUFR-synop messages from GTS into single BUFR files, taking into account all duplications, corrections etc. It is based on the python module *eccodes*.

Alex Deckmyn, 2020  
Royal Meteorological Institute  
alex.deckmyn@meteo.be  

This repository contains
1. a python3 module bufr_filter_gts which should made available to python
   - PYTHONPATH=<.../bufr_filter_gts/module>
   - or copy the bufr_toolbox module to python library path
2. a sample script that calls this module to make a BUFR extraction
3. some outdated documentation

CONFIGURATION:
--------------

You will probably have to modify a few routines in **synop_extractor.py**, depending on how incoming GTS data is stored, the time window and the general location of observations.
- **update_sqlite()** :
in this function, you may need to adapt the directory structure for GTS input. 
At RMI, it is organised in hourly directories "YYYYMMDDHH"
(this is the time that the GTS message arrives and may be quite different from the date to which it refers internally!)
Just look for the *newdir* and *gtsdir* lines.
- Default is to create *hourly BUFR files* ([time - 29', time+30'])
You can modify the window size *obs_window_size=60* in the main call to *update_sqlite()* (see example).
But to change the centering, you will have to modify the function **obs_window()**.
- The function **gts_filter(gtsheader)** is a first filter based simply on GTS headers. It limits the number of files that are actually parsed. By default, it keeps only those marked as BUFR-SYNOP (*TT = IS*) for Europe, Northern hemisphere etc. (*AA[1] in (A, D, N, X)*). This may need to be changed if you want e.g. observations over Africa, Asia...

---

Copyright 2020 Alex Deckmyn (Royal Meteorological Institute)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
