
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
#sys.path.insert(0,"/usr/lib/python3/dist-packages")
import qgis
import json
import pandas as pd
import ipdb
# set up system paths
qspath = '/home/martin/Michail/qgis_sys_paths.csv'
# provide the path where you saved this file.
paths = pd.read_csv(qspath).paths.tolist()
sys.path += paths
# set up environment variables
qepath = '/home/martin/Michail/qgis_env.json'
js = json.loads(open(qepath, 'r').read())
for k, v in js.items():
    os.environ[k] = v




import PyQt5.QtCore
from osgeo import gdal
from osgeo import ogr
from osgeo import osr
import qgis.PyQt.QtCore
from qgis.core import *
from qgis.core import QgsProcessing
from qgis.core import QgsProcessingAlgorithm
from qgis.core import QgsProcessingMultiStepFeedback
from qgis.core import QgsProcessingParameterVectorLayer
from qgis.core import QgsProcessingParameterFeatureSink
from qgis.core import QgsProcessingParameterDefinition
from qgis.core import QgsProcessingParameterMultipleLayers
from qgis.utils import *
from qgis.core import (QgsApplication,
                    QgsProcessingFeedback,
                    QgsProcessingRegistry,
                    QgsProject)
from qgis.core import QgsVectorLayer                     
from qgis.analysis import QgsNativeAlgorithms
import time

import multiprocessing
import subprocess


# feedback = QgsProcessingFeedback()

# Supply path to qgis install location
QgsApplication.setPrefixPath(js['HOME'], True) #ĺib/qgis

# Seems to need "application path" to be initialized before importing

#print(QgsApplication.prefixPath())

# Create a reference to the QgsApplication.  Setting the
# second argument to False disables the GUI.
qgs = QgsApplication([], False)

# Load providers
qgs.initQgis()

from processing.core.Processing import Processing
Processing.initialize()
import processing

QgsApplication.processingRegistry().addProvider(QgsNativeAlgorithms())

algs = dict()
for alg in QgsApplication.processingRegistry().algorithms():
    algs[alg.displayName()] = alg.id()
#print(algs)



print('---------------------------')

#input path to tree crown polygons folder
inPath = '/home/martin/Michail/trees_input/'

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
    parent_dir = "/home/martin/Michail/Temp/"

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
    
#extracting extent for each clip
    context = QgsProcessingContext()
    context.setProject(QgsProject.instance())
    input = QgsProcessingUtils.mapLayerFromString(f'{filep}', context)
    #grid="/yourpath/grid.shp"
    xmin = (input.extent().xMinimum()) #extract the minimum x coord from our layer
    xmax = (input.extent().xMaximum()) #extract our maximum x coord from our layer
    ymin = (input.extent().yMinimum()) #extract our minimum y coord from our layer
    ymax = (input.extent().yMaximum())
    extent = str(xmin)+ ',' + str(xmax)+ ',' +str(ymin)+ ',' +str(ymax)

    #clip non forest layer to extent of current clip

    alg_params = {
        'CLIP': True,
        'EXTENT': extent,
        'INPUT': '/home/martin/Michail/Non_forest_eraser.gpkg',
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/No_forest_clipped.gpkg'
    }
    outputs['ExtractclipByExtent'] = processing.run('native:extractbyextent', alg_params, is_child_algorithm=True)

    #fix geometries for crown trees polygons
    alg_params = {
        'INPUT': f'{filep}',
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/fx_gm_trees.gpkg'
        
    }
    outputs['FixGeometries_vertices'] = processing.run('native:fixgeometries', alg_params,   is_child_algorithm=True)


    #Difference_1 erasing tree crowns by non forest overlay
    alg_params = {
        'INPUT': f'/home/martin/Michail/Temp/{filen}_temp/fx_gm_trees.gpkg',
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/difference_1.gpkg',
        'OVERLAY': f'/home/martin/Michail/Temp/{filen}_temp/No_forest_clipped.gpkg'
        
    }
    outputs['Difference_1'] = processing.run('native:difference', alg_params, is_child_algorithm=True)
    print('difference_1')

    #Processing grid creation for chunks       

    alg_params = {
            "TYPE": 2,
            "EXTENT": extent,
            "HSPACING": 1000,
            "VSPACING": 1000,
            "CRS": 'EPSG:25832',
            "OUTPUT": f"/home/martin/Michail/Temp/{filen}_temp/grid_1000.gpkg"
                    
    }



    outputs['Grid'] = processing.run('qgis:creategrid', alg_params, is_child_algorithm=True)
    

    #extract by location so that processing grid is only where there are tree crown polygons

    alg_params = {
            "INPUT": outputs['Grid']['OUTPUT'],
            "OUTPUT": f"/home/martin/Michail/Temp/{filen}_temp/grid_contain_trees.gpkg",
            "PREDICATE": 1,
            "INTERSECT": f'/home/martin/Michail/Temp/{filen}_temp/difference_1.gpkg'
        }
    outputs['Extractgridlocation'] = processing.run('qgis:extractbylocation', alg_params, is_child_algorithm=True)


    #field calcluator to number row for new grid id

    # Field calculator
    alg_params = {
        'FIELD_LENGTH': 10,
        'FIELD_NAME': 'grid_id',
        'FIELD_PRECISION': 0,
        'FIELD_TYPE': 1,  # integer
        'FORMULA': '@row_number',
        'INPUT': outputs['Extractgridlocation']['OUTPUT'],
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/grid_250_contain_trees_id.gpkg',
        
    }
    outputs['FieldCalculator_grid_id'] = processing.run('native:fieldcalculator', alg_params,   is_child_algorithm=True)

    
    
    
    
    # create spatial join trees with grid for grid id that will determine processing chunks

    
    
    
    
    alg_params = {
        'DISCARD_NONMATCHING': False,
        "INPUT": f'/home/martin/Michail/Temp/{filen}_temp/difference_1.gpkg',
        "JOIN": f'/home/martin/Michail/Temp/{filen}_temp/grid_250_contain_trees_id.gpkg',
        'JOIN_FIELDS': ['grid_id'],
        'METHOD': 1,
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/trees_in_grid.gpkg',
        'PREDICATE': [5],  # within
        'PREFIX': '' 


    }


    
    

    outputs['Vertices_grid'] = processing.run('native:joinattributesbylocation', alg_params, is_child_algorithm=True)



    #fix geometries polygons
    alg_params = {
        'INPUT': f'/home/martin/Michail/Temp/{filen}_temp/trees_in_grid.gpkg',
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/Trees_in_grid_fx_gm.gpkg'
        
    }
    outputs['FixGeometries_vertices'] = processing.run('native:fixgeometries', alg_params,   is_child_algorithm=True)


    
    # #count features of grid gpkg

    dagpkg = f"/home/martin/Michail/Temp/{filen}_temp/grid_contain_trees.gpkg"
    driver = ogr.GetDriverByName('GPKG')

    dataSource = driver.Open(dagpkg, 1) # 0 means read-only. 1 means writeable.

    # Check to see if geopackage is found.
    if dataSource is None:
        print('Could not open %s' % (dagpkg))
    else:
        print('Opened %s' % (dagpkg))
        layer = dataSource.GetLayer()
        featureCount = layer.GetFeatureCount()
        print ("Number of features in %s: %d" % (os.path.basename(dagpkg),featureCount))

    #chunks=[]

    #creation of chunks from selection by grid id iteration and then buffer out 

    def conc_chunks():
        


        alg_params = {
            "INPUT": f'/home/martin/Michail/Temp/{filen}_temp/Trees_in_grid_fx_gm.gpkg',
            "OUTPUT": f"/home/martin/Michail/Temp/{filen}_sel/selection_{i}.gpkg",
            "OPERATOR": 0,
            "FIELD": "grid_id",
            "VALUE": f"{i}"
        }
        

        

    
        
        try:
            
            outputs['Selection'] = processing.run('qgis:extractbyattribute', alg_params, is_child_algorithm=True)

        except Exception as _:
            
            time.sleep(2)

        
        #ipdb.set_trace()


        

        

        alg_params = {
            'DISSOLVE': True,
            'DISTANCE': 10,
            'END_CAP_STYLE': 0,  # Round
            'INPUT': f"/home/martin/Michail/Temp/{filen}_sel/selection_{i}.gpkg",
            'JOIN_STYLE': 0,  # Round
            'MITER_LIMIT': 2,
            'OUTPUT': f'/home/martin/Michail/Temp/{filen}_chunks/forests{i}.gpkg',
            'SEGMENTS': 5,
            
        }
        
        try:
            
            outputs['Trees_Buffer'][i] = processing.run('native:buffer', alg_params,  is_child_algorithm=True)
        except Exception as _:

            print(i,filen)


        
        #ipdb.set_trace()

    # Multiprocessing by grid_id on chunk creation

    iprocesses = []
    
    for i in range(featureCount):
        #if crashes next lines to restart process
        # layer = QgsVectorLayer(f'/home/martin/Michail/Temp/concave_hull_chunks/Outputs_82_18_conc_hull_{i}.gpkg', f"Outputs_82_18_conc_hull_{i}", "ogr") #when crahes
        # if not layer.isValid():   
        #
        iprocess = multiprocessing.Process(target=conc_chunks, args=())
        iprocess.start()
        iprocesses.append(iprocess)

    for iprocess in iprocesses:
        iprocess.join()    
    
    #subrpocess to OGR merge chunks

    wd = os.getcwd()
    os.chdir(f"/home/martin/Michail/Temp/{filen}_chunks/")
    subprocess.run(["ogrmerge.py", "-single", "-f", "GPKG", "-o", f"/home/martin/Michail/Temp/{filen}_temp/merged_chunks.gpkg", "*.gpkg"])
    os.chdir(wd)
    

    print('merge chunks')



    #Fix_geometries
    alg_params = {
        'INPUT': f"/home/martin/Michail/Temp/{filen}_temp/merged_chunks.gpkg",
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/Forest_fx_gm.gpkg'
        
    }
    outputs['Fix_geometries_forest'] = processing.run('native:fixgeometries', alg_params, is_child_algorithm=True)
    print('fix_geom_forest')





    # Buffer_20_in_1
    alg_params = {
        'DISSOLVE': True,
        'DISTANCE': -20,
        'END_CAP_STYLE': 0,  # Round
        'INPUT': f'/home/martin/Michail/Temp/{filen}_temp/Forest_fx_gm.gpkg',
        'JOIN_STYLE': 0,  # Round
        'MITER_LIMIT': 2,
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/bfr_in_20_1.gpkg',
        'SEGMENTS': 5,
        
    }
    outputs['Buffer_10_in_1'] = processing.run('native:buffer', alg_params,  is_child_algorithm=True)

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
        'OUTPUT': f'/home/martin/Michail/Temp/{filen}_temp/sngl_in_10_2.gpkg'
        
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

qgs.exitQgis()
