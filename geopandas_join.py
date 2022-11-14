"""
Model exported as python.
Name : model
Group : 
With QGIS : 32202
"""


import geopandas as gpd
import pandas as pd
import glob, os
from shapely.geometry import Point, Polygon, LineString
from shapely.geometry import shape
from shapely import geometry
import numpy as np
import shutil
import time

import multiprocessing


print('---------------------------')

#input path to tree crown polygons folder
inPath = "/dbfs/mnt/strukturparametre/test_1/"
noforest = gpd.read_file("/dbfs/mnt/strukturparametre/eraser_1.gpkg")
gdf_nofor = gpd.GeoDataFrame(noforest, crs="EPSG:25832")



gpkg_pattern = os.path.join(inPath, '*.gpkg')
treepaths = glob.glob(gpkg_pattern)
print(treepaths, 'treepaths')

#Main process

def delineation_process():

    st= time.time()

    #acquiring filename for each tree crown polygon clip
    filen = os.path.splitext(os.path.basename(filep))[0] 
    

    print(filep)


    
    
    treecrowns = gpd.read_file(filep)
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
            
        chunk = gpd.GeoDataFrame(trees_id[trees_id['grid_id'] == i])

        #buffer out 10 m
        
        buffered = chunk.to_crs(25832).buffer(10)
        
        buffered= gpd.GeoDataFrame(gpd.GeoSeries(buffered))
         
        buffered=  buffered.rename(columns={0:'geometry'}).set_geometry('geometry')
        
        buffered = gpd.geoseries.GeoSeries([geom for geom in buffered.unary_union.geoms])

        buffered =  buffered.set_crs('epsg:25832')
        print(i)

        list_chunks.append(buffered)
        
        #ipdb.set_trace()

    # Multiprocessing by grid_id on chunk creation

    iprocesses = []
    
    for i in range(featureCount):
      
        iprocess = multiprocessing.Process(target=conc_chunks, args=())
        iprocess.start()
        iprocesses.append(iprocess)

    for iprocess in iprocesses:
        iprocess.join()    
    
    
    merge = pd.concat(list_chunks)
    
    merged_gdf = gpd.GeoDataFrame(gpd.GeoSeries(merge))
    
    merged_gdf = merged_gdf.rename(columns={0:'geometry'}).set_geometry('geometry')
    
    bfr_in_20_1 = merged_gdf.to_crs(25832).buffer(-20)
    

    print('buffer_20_in_1',filen)


    # Multipart to singleparts
    single_bfr_in_1 = bfr_in_20_1.explode(index_parts=True)


    # Buffer
    bfr_out_20_2 = single_bfr_in_1.to_crs(25832).buffer(20)
    
    un_2 = gpd.geoseries.GeoSeries([geom for geom in bfr_out_20_2.unary_union.geoms])
    
    # Single_out_20
    un_2_single = un_2.explode(index_parts=True)
    
    un_2_single = un_2_single.set_crs(25832)
    # Buffer_in_10_2
    bfr_in_10_2 = un_2_single.to_crs(25832).buffer(-10)
    
    #single in 10 2

    single_bfr_in_2 = bfr_in_10_2.explode(index_parts=True)

    single_bfr_in_2 = single_bfr_in_2.set_crs(25832)
    
    #Buffer out 10 3
    
    bfr_out_10_3 = single_bfr_in_2.to_crs(25832).buffer(10)
    
    un_3 = gpd.geoseries.GeoSeries([geom for geom in bfr_out_10_3.unary_union.geoms])

    single_un_3 = un_3.explode(index_parts=True)
    
    single_un_3 = single_un_3.set_crs(25832)

    # Buffer_in_10_3

    bfr_in_10_3 = single_un_3.to_crs(25832).buffer(-10)
    # Single_buff_in_10_3
    single_in_10_3 = bfr_in_10_3.explode(index_parts=True)
    
    gdf_in_10_3 = gpd.GeoDataFrame(gpd.GeoSeries(single_in_10_3))
    
    gdf_in_10_3 = gdf_in_10_3.rename(columns={0:'geometry'}).set_geometry('geometry')
    gdf_in_10_3 = gdf_in_10_3.set_crs(25832)
    
    
            # Difference_final
    dif_final = gdf_in_10_3.overlay(nonfor_clip, how='difference')
    
    print('difference_final')

        # Buffer_in_10_4
    bfr_in_10_4 = dif_final.to_crs(25832).buffer(-10)
    single_in_10_4 = bfr_in_10_4.explode(index_parts=True)
    gdf_in_10_4 = gpd.GeoDataFrame(gpd.GeoSeries(single_in_10_4))
    gdf_in_10_4 = gdf_in_10_4.rename(columns={0:'geometry'}).set_geometry('geometry')
    gdf_in_10_4 = gdf_in_10_4.set_crs(25832)
    

    # Buffer_out_10_3
    gdf_out_10_4 = gdf_in_10_4.to_crs(25832).buffer(10)
    un_4 = gpd.geoseries.GeoSeries([geom for geom in gdf_out_10_4.unary_union.geoms])
    single_out_4 =un_4.explode(index_parts=True)
    gdf_out_10_4 = gpd.GeoDataFrame(gpd.GeoSeries(single_in_10_4))
    gdf_out_10_4 = gdf_out_10_4.rename(columns={0:'geometry'}).set_geometry('geometry')
    gdf_out_10_4 = gdf_out_10_4.set_crs(25832)
            # Single_buff_out_10_3

    # Field calculator
    gdf_out_10_4['area_ha'] = gdf_out_10_4.area/10000
    # Extract by attribute
    forest = gdf_out_10_4[gdf_out_10_4['area_ha'] >= 0.5]
    
    forest.to_file(f"/tmp/forest_{filen}.gpkg", layer=f'forest_{filen}', driver="GPKG")
    shutil.move(f"/tmp/forest_{filen}.gpkg", f"/dbfs/mnt/resultblocks/foresr_{filen}.gpkg" )



    et = time.time()
    total_time = et - st
    print(total_time, 'elapsed_time')
    



#multiprocessing the delineation process for filepath in polygon clips 

fprocesses = []

for filep in treepaths:

    fprocess = multiprocessing.Process(target=delineation_process, args=())
    fprocess.start()
    fprocesses.append(fprocess)

for fprocess in fprocesses:
    fprocess.join()  


