"""
2
Model exported as python.
3
Name : model
4
Group : 
5
With QGIS : 32202
6
"""
7
 
8
 
9
 
10
 
11
import os
12
import glob
13
import shutil
14
 
15
import sys
16
#sys.path.insert(0,"/usr/lib/python3/dist-packages")
17
 
18
 
19
import pandas as pd
20
import ipdb
21
 
22
 
23
 
24
 
25
from osgeo import gdal
26
from osgeo import ogr
27
from osgeo import osr
28
 
29
 
30
import multiprocessing
31
import subprocess
32
 
33
 
34
 
35
 
36
 
37
print('---------------------------')
38
 
39
#input path to tree crown polygons folder
40
inPath = '/dbfs/mnt/treecrownclips/'
41
noforest = gpd.read_file"/dbfs/mnt/strukturparametre/non-forrest.shp"
42
gdf_nofor = gpd.GeoDataFrame(noforest, crs="EPSG:25832")
43
 
44
#read paths to trees
45
treepaths = glob.glob(f"{inPath}/merged*.gpkg")
46
print(treepaths, 'treepaths')
47
 
48
#Main process
49
 
50
def delineation_process():
51
 
52
    st= time.time()
53
 
54
    #acquiring filename for each tree crown polygon clip
55
    filen = os.path.splitext(os.path.basename(filep))[0] 
56
    
57
 
58
    print(filep)
59
 
60
# creating directories for each clip's intermediate processing files according to filename, needed to distinguish the first buffer chunks so they can be merged by clip and then deleted by clip.
61
 
62
    #tree crowns chunks selection directory
63
    directorysel = f"{filen}_sel" 
64
    #1st buffered chunks
65
    directorych = f'{filen}_chunks'
66
    #rest of temporary intermediate files
67
    directorytmp = f'{filen}_temp'
68
    
69
# Parent Directory path
70
    parent_dir = "/dbfs/mnt/temp/"
71
 
72
# Paths
73
    pathsel = os.path.join(parent_dir, directorysel)
74
    pathch = os.path.join(parent_dir, directorych)
75
    pathtmp = os.path.join(parent_dir, directorytmp)
76
 
77
# Create the directory
78
#
79
# '/home / User / Documents'
80
    os.mkdir(pathsel)
81
    os.mkdir(pathch)
82
    os.mkdir(pathtmp)
83
 
84
 
85
 
86
 
87
 
88
    results = {}
89
    outputs = {}
90
    
91
    treecrowns = gpd.read_file(f{'filep'})
92
    gdf_trees = gpd.GeoDataFrame(treecrowns, crs="EPSG:25832")
93
    
94
#extracting extent for each clip
95
 
96
    extent = treecrowns.total_bounds
97
 
98
    #clip non forest layer to extent of current clip
99
    
100
    nonfor_clip = gdf_nonfor.overlay(extent, how='intersection')
101
 
102
    #Difference_1 erasing tree crowns by non forest overlay
103
 
104
    treesdif = gdf_trees.overlay(nonfor_clip, how='difference')
105
    
106
    #Processing grid creation for chunks       
107
 
108
    minX, minY, maxX, maxY = total_bounds
109
 
110
    # Create a grid to the extent of input tree crowns
111
    x, y = (minX, minY)
112
    geom_array = []
113
 
114
    # Polygon Size
115
    square_size = 1000
116
    while y <= maxY:
117
        while x <= maxX:
118
            geom = geometry.Polygon([(x,y), (x, y+square_size), (x+square_size, y+square_size), (x+square_size, y), (x, y)])
119
            geom_array.append(geom)
120
            x += square_size
121
        x = minX
122
        y += square_size
123
 
124
    grid = gpd.GeoDataFrame(geom_array, columns=['geometry']).set_crs('EPSG:25832')
125
 
126
    gdf_trees = treesdif.assign(trees='tree')
127
    
128
    gdf_trees =gdf_trees.to_crs(epsg=25832)
129
    
130
    gdf_grid =gdf_grid.to_crs(epsg=25832)
131
 
132
    #extract by location so that processing grid is only where there are tree crown polygons
133
 
134
 
135
 
136
    #field calcluator to number row for new grid id
137
 
138
    gdf_grid['row'] = np.arange(len(gdf_grid)))
139
 
140
    
141
   # create spatial join trees with grid for grid id that will determine processing chunks
142
 
143
    dfsjoin = gpd.sjoin(gdf_trees, gdf_grid) #Spatial join Points to polygons
144
    
145
    dfpivot = pd.pivot_table(dfsjoin,index='row',columns='trees',aggfunc={'trees':len})  
146
    
147
    dfpivot.columns = dfpivot.columns.droplevel()  
148
 
149
 
150
    dfpolynew = gdf_grid.merge(dfpivot, how='left', on='row')
151
 
152
    grid_with_trees = dfpolynew.dropna( how='any', subset=['tree'])
153
    
154
    grid_with_trees['grid_id'] = np.arange(len(grid_with_trees))
155
    
156
    trees_id = gpd.sjoin(gdf_trees, grid_with_trees)
157
    
158
    trees_id = trees_id[["geometry", "grid_id"]]
159
 
160
    
161
    # #count features of grid gpkg
162
    featureCount =  len(grid_with_trees['grid_id'])
163
 
164
    list_chunks=[]
165
 
166
    #creation of chunks from selection by grid id iteration and then buffer out 
167
 
168
    def conc_chunks():
169
        
170
        #extract by attribute
171
            
172
        outputs['Selection'] = gdf_trees[gdf_trees['grid_id'] == f'{i}]
173
 
174
        #buffer out 10 m
175
        
176
        outputs[f'buffer_{i}'] = outputs['Selection'].buffer(10,resolution=2 )
177
 
178
            print(i,filen)
179
 
180
        list_chunks.append(outputs[f'{filen}_buffer_{1}'])
181
        
182
        #ipdb.set_trace()
183
 
184
    # Multiprocessing by grid_id on chunk creation
185
 
186
    iprocesses = []
187
    
188
    for i in range(featureCount):
189
      
190
        iprocess = multiprocessing.Process(target=conc_chunks, args=())
191
        iprocess.start()
192
        iprocesses.append(iprocess)
193
 
194
    for iprocess in iprocesses:
195
        iprocess.join()    
196
    
197
    #subrpocess to OGR merge chunks
198
 
199
    #wd = os.getcwd()
200
    #os.chdir(f"/home/martin/Michail/Temp/{filen}_chunks/")
201
    #subprocess.run(["ogrmerge.py", "-single", "-f", "GPKG", "-o", f"/home/martin/Michail/Temp/{filen}_temp/merged_chunks.gpkg", "*.gpkg"])
202
    #os.chdir(wd)
203
                                         
204
    
205
    
206
 
207
    print('merge chunks')
208
 
209
 
210
 
211
    #Fix_geometries
212
 
213
 
214
 
215
 
216
 
217
 
218
    # Buffer_20_in_1
219
    alg_params = {
220
        'DISSOLVE': True,
221
        'DISTANCE': -20,
222
        'END_CAP_STYLE': 0,  # Round
223
        'INPUT': f'/home/martin/Michail/Temp/{filen}_temp/Forest_fx_gm.gpkg',
224
        'JOIN_STYLE': 0,  # Round
225
        'MITER_LIMIT': 2,
226
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/bfr_in_20_1.gpkg',
227
        'SEGMENTS': 5,
228
        
229
    }
230
    outputs['Buffer_10_in_1'] = processing.run('native:buffer', alg_params,  is_child_algorithm=True)
231
 
232
    print('buffer_20_in_1',filen)
233
 
234
 
235
    # Multipart to singleparts
236
    alg_params = {
237
        'INPUT': outputs['Buffer_10_in_1']['OUTPUT'],
238
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/sngl_in_20_1.gpkg'
239
        
240
    }
241
    outputs['MultipartToSingleparts'] = processing.run('native:multiparttosingleparts', alg_params,  is_child_algorithm=True)
242
 
243
 
244
    # Buffer
245
    alg_params = {
246
        'DISSOLVE': True,
247
        'DISTANCE': 20,
248
        'END_CAP_STYLE': 0,  # Round
249
        'INPUT': outputs['MultipartToSingleparts']['OUTPUT'],
250
        'JOIN_STYLE': 0,  # Round
251
        'MITER_LIMIT': 2,
252
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/bfr_20_out_1.gpkg',
253
        'SEGMENTS': 5,
254
        
255
    }
256
    outputs['Buffer'] = processing.run('native:buffer', alg_params,  is_child_algorithm=True)
257
 
258
 
259
    # Single_out_20
260
    alg_params = {
261
        'INPUT': outputs['Buffer']['OUTPUT'],
262
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/sngl_2_bfr_out_1.gpkg'
263
        
264
    }
265
    outputs['Single_out_20'] = processing.run('native:multiparttosingleparts', alg_params,  is_child_algorithm=True)
266
 
267
 
268
    # Buffer_in_10_2
269
    alg_params = {
270
        'DISSOLVE': True,
271
        'DISTANCE': -10,
272
        'END_CAP_STYLE': 0,  # Round
273
        'INPUT': outputs['Single_out_20']['OUTPUT'],
274
        'JOIN_STYLE': 0,  # Round
275
        'MITER_LIMIT': 2,
276
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/bfr_in_10_2.gpkg',
277
        'SEGMENTS': 5,
278
        
279
    }
280
    outputs['Buffer_in_10_2'] = processing.run('native:buffer', alg_params,   is_child_algorithm=True)
281
 
282
 
283
    # # Single_in_10_2
284
    alg_params = {
285
        'INPUT': f'/home/martin/Michail/Temp/{filen}_temp/bfr_in_10_2.gpkg',
286
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_sngl_in_10_2.gpkg'
287
        
288
    }
289
    outputs['Single_in_10_2'] = processing.run('native:multiparttosingleparts', alg_params,   is_child_algorithm=True)
290
 
291
 
292
 
293
    # Buffer_out_10_2
294
    alg_params = {
295
        'DISSOLVE': True,
296
        'DISTANCE': 10,
297
        'END_CAP_STYLE': 0,  # Round
298
        'INPUT': outputs['Single_in_10_2']['OUTPUT'],
299
        'JOIN_STYLE': 0,  # Round
300
        'MITER_LIMIT': 2,
301
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/bfr_out_3_10.gpkg',
302
        'SEGMENTS': 5,
303
        
304
    }
305
    outputs['Buffer_out_10_2'] = processing.run('native:buffer', alg_params,   is_child_algorithm=True)
306
 
307
 
308
 
309
    # Single_buff_out_10_2
310
    alg_params = {
311
        'INPUT': outputs['Buffer_out_10_2']['OUTPUT'],
312
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/sngl_bfr_out_10_2.gpkg'
313
        
314
    }
315
    outputs['Single_buff_out_10_2'] = processing.run('native:multiparttosingleparts', alg_params,   is_child_algorithm=True)
316
 
317
 
318
 
319
    # Buffer_in_10_3
320
    alg_params = {
321
        'DISSOLVE': True,
322
        'DISTANCE': -10,
323
        'END_CAP_STYLE': 0,  # Round
324
        'INPUT': outputs['Single_buff_out_10_2']['OUTPUT'],
325
        'JOIN_STYLE': 0,  # Round
326
        'MITER_LIMIT': 2,
327
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/bfr_in_10_3.gpkg',
328
        'SEGMENTS': 5,
329
        
330
    }
331
    outputs['Buffer_in_10_3'] = processing.run('native:buffer', alg_params,   is_child_algorithm=True)
332
 
333
 
334
    
335
    # Single_buff_in_10_3
336
    alg_params = {
337
        'INPUT': outputs['Buffer_in_10_3']['OUTPUT'],
338
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/sngl_bfr_in_10_3.gpkg'
339
        
340
    }
341
    outputs['Single_buff_in_10_3'] = processing.run('native:multiparttosingleparts', alg_params,   is_child_algorithm=True)
342
 
343
            # Difference_final
344
    alg_params = {
345
        'INPUT': f'/home/martin/Michail/Temp/{filen}_temp/sngl_bfr_in_10_3.gpkg',
346
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/difference_final.gpkg',
347
        'OVERLAY': f'/home/martin/Michail/Temp/{filen}_temp/No_forest_clipped.gpkg'
348
        
349
    }
350
    outputs['Difference_final'] = processing.run('native:difference', alg_params, is_child_algorithm=True)
351
 
352
    #fix geometries
353
    alg_params = {
354
        'INPUT': outputs['Difference_final']['OUTPUT'],
355
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/fx_gm_fin_dif_gm.gpkg'
356
        
357
    }
358
    outputs['FixGeometries_fin_dif'] = processing.run('native:fixgeometries', alg_params,   is_child_algorithm=True)
359
 
360
 
361
    print('difference_final')
362
 
363
        # Buffer_in_10_4
364
    alg_params = {
365
        'DISSOLVE': True,
366
        'DISTANCE': -10,
367
        'END_CAP_STYLE': 0,  # Round
368
        'INPUT': outputs['FixGeometries_fin_dif']['OUTPUT'],
369
        'JOIN_STYLE': 0,  # Round
370
        'MITER_LIMIT': 2,
371
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/bfr_in_10_4.gpkg',
372
        'SEGMENTS': 5,
373
        
374
    }
375
    outputs['Buffer_in_10_4'] = processing.run('native:buffer', alg_params,   is_child_algorithm=True)
376
 
377
        # Single_buff_in_10_4
378
    alg_params = {
379
        'INPUT': outputs['Buffer_in_10_4']['OUTPUT'],
380
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/sngl_bfr_in_10_4.gpkg'
381
        
382
    }
383
    outputs['Single_buff_in_10_3'] = processing.run('native:multiparttosingleparts', alg_params,   is_child_algorithm=True)
384
 
385
 
386
    # Buffer_out_10_3
387
    alg_params = {
388
        'DISSOLVE': True,
389
        'DISTANCE': 10,
390
        'END_CAP_STYLE': 0,  # Round
391
        'INPUT': outputs['Single_buff_in_10_3']['OUTPUT'],
392
        'JOIN_STYLE': 0,  # Round
393
        'MITER_LIMIT': 2,
394
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/bfr_out_10_3.gpkg',
395
        'SEGMENTS': 5,
396
        
397
    }
398
    outputs['Buffer_out_10_3'] = processing.run('native:buffer', alg_params,   is_child_algorithm=True)
399
 
400
            # Single_buff_out_10_3
401
    alg_params = {
402
        'INPUT': outputs['Buffer_out_10_3']['OUTPUT'],
403
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/sngl_bfr_out_10_3.gpkg'
404
        
405
    }
406
    outputs['Single_buff_out_10_3'] = processing.run('native:multiparttosingleparts', alg_params,   is_child_algorithm=True)
407
 
408
 
409
 
410
    # Field calculator
411
    alg_params = {
412
        'FIELD_LENGTH': 10,
413
        'FIELD_NAME': 'area_ha',
414
        'FIELD_PRECISION': 3,
415
        'FIELD_TYPE': 0,  # Float
416
        'FORMULA': '$area/10000',
417
        'INPUT': outputs['Single_buff_out_10_3']['OUTPUT'],
418
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/area_ha.gpkg',
419
        
420
    }
421
    outputs['FieldCalculator'] = processing.run('native:fieldcalculator', alg_params,   is_child_algorithm=True)
422
 
423
 
424
 
425
    # Extract by attribute
426
    alg_params = {
427
        'FIELD': 'area_ha',
428
        'INPUT': outputs['FieldCalculator']['OUTPUT'],
429
        'OPERATOR': 3,  # ≥
430
        'VALUE': '0.5',
431
        'OUTPUT': f'/home/martin/Michail/Results_clips/Final_output_{filen}.gpkg'
432
    }
433
    outputs['ExtractByAttribute'] = processing.run('native:extractbyattribute', alg_params,   is_child_algorithm=True)
434
    results['Delineation001'] = outputs['ExtractByAttribute']['OUTPUT']
435
    
436
    files = f'/home/martin/Michail/Temp/{filen}_temp'
437
    chanks = f'/home/martin/Michail/Temp/{filen}_chunks'
438
    sels = f'/home/martin/Michail/Temp/{filen}_sel'
439
    
440
    shutil.rmtree(files)
441
    shutil.rmtree(chanks)
442
    shutil.rmtree(sels)
443
 
444
 
445
    et = time.time()
446
    total_time = et - st
447
    print(total_time, 'elapsed_time')
448
    return results
449
 
450
 
451
 
452
#multiprocessing the delineation process for filepath in polygon clips 
453
 
454
fprocesses = []
455
    
456
for filep in treepaths:
457
    # layer = QgsVectorLayer(f'/home/martin/Michail/Temp/concave_hull_chunks/Outputs_82_18_conc_hull_{i}.gpkg', f"Outputs_82_18_conc_hull_{i}", "ogr") #when crahes
458
    # if not layer.isValid():   
459
    #
460
    fprocess = multiprocessing.Process(target=delineation_process, args=())
461
    fprocess.start()
462
    fprocesses.append(fprocess)
463
 
464
for fprocess in fprocesses:
465
    fprocess.join()  
466
 
467
# ipdb.set_trace()
468
 
