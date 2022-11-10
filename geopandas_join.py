"""
Model exported as python.
Name : model
Group : 
With QGIS : 32202
"""


 

import os
import glob
import shutil

import sys



import pandas as pd
import ipdb




from osgeo import gdal
from osgeo import ogr
from osgeo import osr


import multiprocessing
import subprocess





print('---------------------------')

#input path to tree crown polygons folder
inPath = '/dbfs/mnt/treecrownclips/'
noforest = gpd.read_file"/dbfs/mnt/strukturparametre/non-forrest.shp"
gdf_nofor = gpd.GeoDataFrame(noforest, crs="EPSG:25832")

#read paths to trees
treepaths = glob.glob(f"{inPath}/merged*.gpkg")
print(treepaths, 'treepaths')

#Main process

def delineation_process():

    st= time.time()

    #acquiring filename for each tree crown polygon clip
    filen = os.path.splitext(os.path.basename(filep))[0] 
    

    print(filep)

# creating directories for each clip's intermediate processing files according to filename, needed to distinguish the first buffer chunks so they can be merged by clip and then deleted by clip.

    #tree crowns chunks selection directory
    directorysel = f"{filen}_sel" 
    #1st buffered chunks
    directorych = f'{filen}_chunks'
    #rest of temporary intermediate files
    directorytmp = f'{filen}_temp'
    
# Parent Directory path
    parent_dir = "/dbfs/mnt/temp/"

# Paths
    pathsel = os.path.join(parent_dir, directorysel)
    pathch = os.path.join(parent_dir, directorych)
    pathtmp = os.path.join(parent_dir, directorytmp)

# Create the directory
#
# '/home / User / Documents'
    os.mkdir(pathsel)
    os.mkdir(pathch)
    os.mkdir(pathtmp)





    results = {}
    outputs = {}
    
    treecrowns = gpd.read_file(f{'filep'})
    gdf_trees = gpd.GeoDataFrame(treecrowns, crs="EPSG:25832")
    
#extracting extent for each clip

    extent = treecrowns.total_bounds

    #clip non forest layer to extent of current clip
    
    nonfor_clip = gdf_nonfor.overlay(extent, how='intersection')

    #Difference_1 erasing tree crowns by non forest overlay

    treesdif = gdf_trees.overlay(nonfor_clip, how='difference')
    
    #Processing grid creation for chunks       

    minX, minY, maxX, maxY = total_bounds

    # Create a grid to the extent of input tree crowns
    x, y = (minX, minY)
    geom_array = []

    # Polygon Size
    square_size = 1000
    while y <= maxY:
        while x <= maxX:
            geom = geometry.Polygon([(x,y), (x, y+square_size), (x+square_size, y+square_size), (x+square_size, y), (x, y)])
            geom_array.append(geom)
            x += square_size
        x = minX
        y += square_size

    grid = gpd.GeoDataFrame(geom_array, columns=['geometry']).set_crs('EPSG:25832')

    gdf_trees = treesdif.assign(trees='tree')
    
    gdf_trees =gdf_trees.to_crs(epsg=25832)
    
    gdf_grid =gdf_grid.to_crs(epsg=25832)

    #extract by location so that processing grid is only where there are tree crown polygons



    #field calcluator to number row for new grid id

    gdf_grid['row'] = np.arange(len(gdf_grid))

    
   # create spatial join trees with grid for grid id that will determine processing chunks

    dfsjoin = gpd.sjoin(gdf_trees, gdf_grid) #Spatial join Points to polygons
    
    dfpivot = pd.pivot_table(dfsjoin,index='row',columns='trees',aggfunc={'trees':len})  
    
    dfpivot.columns = dfpivot.columns.droplevel()  
 

    dfpolynew = gdf_grid.merge(dfpivot, how='left', on='row')

    grid_with_trees = dfpolynew.dropna( how='any', subset=['tree'])
    
    grid_with_trees['grid_id'] = np.arange(len(grid_with_trees))
    
    trees_id = gpd.sjoin(gdf_trees, grid_with_trees)
    
    trees_id = trees_id[["geometry", "grid_id"]]

    
    # #count features of grid gpkg
    featureCount =  len(grid_with_trees['grid_id'])

    list_chunks=[]

    #creation of chunks from selection by grid id iteration and then buffer out 

    def conc_chunks():
        
        #extract by attribute
            
        outputs['Selection'] = gdf_trees[gdf_trees['grid_id'] == f'{i}]

        #buffer out 10 m
        
        outputs[f'buffer_{i}'] = outputs['Selection'].buffer(10,resolution=2 )

            print(i,filen)

        list_chunks.append(outputs[f'{filen}_buffer_{1}'])
        
        #ipdb.set_trace()

    # Multiprocessing by grid_id on chunk creation

    iprocesses = []
    
    for i in range(featureCount):
      
        iprocess = multiprocessing.Process(target=conc_chunks, args=())
        iprocess.start()
        iprocesses.append(iprocess)

    for iprocess in iprocesses:
        iprocess.join()    
    
    #subrpocess to OGR merge chunks

    #wd = os.getcwd()
    #os.chdir(f"/home/martin/Michail/Temp/{filen}_chunks/")
    #subprocess.run(["ogrmerge.py", "-single", "-f", "GPKG", "-o", f"/home/martin/Michail/Temp/{filen}_temp/merged_chunks.gpkg", "*.gpkg"])
    #os.chdir(wd)
                                         
    
    

    print('merge chunks')



    #Fix_geometries






    # Buffer_20_in_1
    buffer_in_20_1 = fix_union_utm.buffer(-20,resolution=2 )
        


    print('buffer_20_in_1',filen)


    # Multipart to singleparts
    alg_params = {
        'INPUT': outputs['Buffer_10_in_1']['OUTPUT'],
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/sngl_in_20_1.gpkg'
        
    }
    outputs['MultipartToSingleparts'] = processing.run('native:multiparttosingleparts', alg_params,  is_child_algorithm=True)


    # Buffer
    alg_params = {
        'DISSOLVE': True,
        'DISTANCE': 20,
        'END_CAP_STYLE': 0,  # Round
        'INPUT': outputs['MultipartToSingleparts']['OUTPUT'],
        'JOIN_STYLE': 0,  # Round
        'MITER_LIMIT': 2,
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/bfr_20_out_1.gpkg',
        'SEGMENTS': 5,
        
    }
    outputs['Buffer'] = processing.run('native:buffer', alg_params,  is_child_algorithm=True)


    # Single_out_20
    alg_params = {
        'INPUT': outputs['Buffer']['OUTPUT'],
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/sngl_2_bfr_out_1.gpkg'
        
    }
    outputs['Single_out_20'] = processing.run('native:multiparttosingleparts', alg_params,  is_child_algorithm=True)


    # Buffer_in_10_2
    alg_params = {
        'DISSOLVE': True,
        'DISTANCE': -10,
        'END_CAP_STYLE': 0,  # Round
        'INPUT': outputs['Single_out_20']['OUTPUT'],
        'JOIN_STYLE': 0,  # Round
        'MITER_LIMIT': 2,
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/bfr_in_10_2.gpkg',
        'SEGMENTS': 5,
        
    }
    outputs['Buffer_in_10_2'] = processing.run('native:buffer', alg_params,   is_child_algorithm=True)


    # # Single_in_10_2
    alg_params = {
        'INPUT': f'/home/martin/Michail/Temp/{filen}_temp/bfr_in_10_2.gpkg',
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_sngl_in_10_2.gpkg'
        
    }
    outputs['Single_in_10_2'] = processing.run('native:multiparttosingleparts', alg_params,   is_child_algorithm=True)



    # Buffer_out_10_2
    alg_params = {
        'DISSOLVE': True,
        'DISTANCE': 10,
        'END_CAP_STYLE': 0,  # Round
        'INPUT': outputs['Single_in_10_2']['OUTPUT'],
        'JOIN_STYLE': 0,  # Round
        'MITER_LIMIT': 2,
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/bfr_out_3_10.gpkg',
        'SEGMENTS': 5,
        
    }
    outputs['Buffer_out_10_2'] = processing.run('native:buffer', alg_params,   is_child_algorithm=True)



    # Single_buff_out_10_2
    alg_params = {
        'INPUT': outputs['Buffer_out_10_2']['OUTPUT'],
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/sngl_bfr_out_10_2.gpkg'
        
    }
    outputs['Single_buff_out_10_2'] = processing.run('native:multiparttosingleparts', alg_params,   is_child_algorithm=True)



    # Buffer_in_10_3
    alg_params = {
        'DISSOLVE': True,
        'DISTANCE': -10,
        'END_CAP_STYLE': 0,  # Round
        'INPUT': outputs['Single_buff_out_10_2']['OUTPUT'],
        'JOIN_STYLE': 0,  # Round
        'MITER_LIMIT': 2,
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/bfr_in_10_3.gpkg',
        'SEGMENTS': 5,
        
    }
    outputs['Buffer_in_10_3'] = processing.run('native:buffer', alg_params,   is_child_algorithm=True)


    
    # Single_buff_in_10_3
    alg_params = {
        'INPUT': outputs['Buffer_in_10_3']['OUTPUT'],
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/sngl_bfr_in_10_3.gpkg'
        
    }
    outputs['Single_buff_in_10_3'] = processing.run('native:multiparttosingleparts', alg_params,   is_child_algorithm=True)

            # Difference_final
    alg_params = {
        'INPUT': f'/home/martin/Michail/Temp/{filen}_temp/sngl_bfr_in_10_3.gpkg',
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/difference_final.gpkg',
        'OVERLAY': f'/home/martin/Michail/Temp/{filen}_temp/No_forest_clipped.gpkg'
        
    }
    outputs['Difference_final'] = processing.run('native:difference', alg_params, is_child_algorithm=True)

    #fix geometries
    alg_params = {
        'INPUT': outputs['Difference_final']['OUTPUT'],
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/fx_gm_fin_dif_gm.gpkg'
        
    }
    outputs['FixGeometries_fin_dif'] = processing.run('native:fixgeometries', alg_params,   is_child_algorithm=True)


    print('difference_final')

        # Buffer_in_10_4
    alg_params = {
        'DISSOLVE': True,
        'DISTANCE': -10,
        'END_CAP_STYLE': 0,  # Round
        'INPUT': outputs['FixGeometries_fin_dif']['OUTPUT'],
        'JOIN_STYLE': 0,  # Round
        'MITER_LIMIT': 2,
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/bfr_in_10_4.gpkg',
        'SEGMENTS': 5,
        
    }
    outputs['Buffer_in_10_4'] = processing.run('native:buffer', alg_params,   is_child_algorithm=True)

        # Single_buff_in_10_4
    alg_params = {
        'INPUT': outputs['Buffer_in_10_4']['OUTPUT'],
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/sngl_bfr_in_10_4.gpkg'
        
    }
    outputs['Single_buff_in_10_3'] = processing.run('native:multiparttosingleparts', alg_params,   is_child_algorithm=True)


    # Buffer_out_10_3
    alg_params = {
        'DISSOLVE': True,
        'DISTANCE': 10,
        'END_CAP_STYLE': 0,  # Round
        'INPUT': outputs['Single_buff_in_10_3']['OUTPUT'],
        'JOIN_STYLE': 0,  # Round
        'MITER_LIMIT': 2,
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/bfr_out_10_3.gpkg',
        'SEGMENTS': 5,
        
    }
    outputs['Buffer_out_10_3'] = processing.run('native:buffer', alg_params,   is_child_algorithm=True)

            # Single_buff_out_10_3
    alg_params = {
        'INPUT': outputs['Buffer_out_10_3']['OUTPUT'],
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/sngl_bfr_out_10_3.gpkg'
        
    }
    outputs['Single_buff_out_10_3'] = processing.run('native:multiparttosingleparts', alg_params,   is_child_algorithm=True)



    # Field calculator
    alg_params = {
        'FIELD_LENGTH': 10,
        'FIELD_NAME': 'area_ha',
        'FIELD_PRECISION': 3,
        'FIELD_TYPE': 0,  # Float
        'FORMULA': '$area/10000',
        'INPUT': outputs['Single_buff_out_10_3']['OUTPUT'],
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/area_ha.gpkg',
        
    }
    outputs['FieldCalculator'] = processing.run('native:fieldcalculator', alg_params,   is_child_algorithm=True)



    # Extract by attribute
    alg_params = {
        'FIELD': 'area_ha',
        'INPUT': outputs['FieldCalculator']['OUTPUT'],
        'OPERATOR': 3,  # ≥
        'VALUE': '0.5',
        'OUTPUT': f'/home/martin/Michail/Results_clips/Final_output_{filen}.gpkg'
    }
    outputs['ExtractByAttribute'] = processing.run('native:extractbyattribute', alg_params,   is_child_algorithm=True)
    results['Delineation001'] = outputs['ExtractByAttribute']['OUTPUT']
    
    files = f'/home/martin/Michail/Temp/{filen}_temp'
    chanks = f'/home/martin/Michail/Temp/{filen}_chunks'
    sels = f'/home/martin/Michail/Temp/{filen}_sel'
    
    shutil.rmtree(files)
    shutil.rmtree(chanks)
    shutil.rmtree(sels)


    et = time.time()
    total_time = et - st
    print(total_time, 'elapsed_time')
    return results



#multiprocessing the delineation process for filepath in polygon clips 

fprocesses = []
    
for filep in treepaths:
    # layer = QgsVectorLayer(f'/home/martin/Michail/Temp/concave_hull_chunks/Outputs_82_18_conc_hull_{i}.gpkg', f"Outputs_82_18_conc_hull_{i}", "ogr") #when crahes
    # if not layer.isValid():   
    #
    fprocess = multiprocessing.Process(target=delineation_process, args=())
    fprocess.start()
    fprocesses.append(fprocess)

for fprocess in fprocesses:
    fprocess.join()  

# ipdb.set_trace()
