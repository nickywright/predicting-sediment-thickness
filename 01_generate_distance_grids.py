
from call_system_command import call_system_command
import math
import multiprocessing
import os, shutil
import sys
from datetime import datetime
import yaml
try:
    from yaml import Cloader as Loader
except ImportError:
    from yaml import Loader

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
""" ---------- Part 1 of the predicting-sediment-thickness workflow -----------
This script creates grids (netcdfs) of the mean distance to passive margins through time

Requirements & Inputs:
    - Python  
    - Python scripts: ocean_basin_proximity.py, 
    - GMT 5 (or later)
    - Files associated with a tectonic model: agegrids, rotation file, plate boundaries, topologies

Outputs:
    - directory named 'distances_%sd' % resolution, with mean distance grids (in metres) through time.

2020-02-14: Added comments, removed hardcoded names (for rotation file, etc) from definitions
2022-08-29: Added more comments (NW)
"""

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# ------------------------------------------
# --- Set paths and various parameters
# ------------------------------------------
today = datetime.today().strftime('%Y%m%d')

try:
    config_file = sys.argv[1]
    print("*** Parameters set from %s ***" % config_file)
    with open(config_file) as f:
        PARAMS = yaml.load(f, Loader=Loader)

    # ------------------------------------------
    # --- Set directories and input files ------
    model_name = PARAMS["InputFiles"]["model_name"]
    paleobathymetry_main_output_dir = PARAMS["OutputFiles"]["paleobathymetry_main_output_dir"]
    include_date_in_output_dir = PARAMS["OutputFiles"]["include_date_in_output_dir"]
    date = PARAMS["OutputFiles"]["date"]

    sediment_thickness_output_dir = PARAMS["OutputFiles"]["sediment_thickness_output_dir"]
    sediment_thickness_within_main_output_dir = PARAMS["OutputFiles"]["sediment_thickness_within_main_output_dir"]

    # --- agegrids
    agegrid_dir = PARAMS["InputFiles"]["agegrid_dir"]
    agegrid_filename = PARAMS["InputFiles"]["agegrid_filename"]
    agegrid_filename_ext = PARAMS["InputFiles"]["agegrid_filename_ext"]

    # --- input file
    proximity_features_files = [PARAMS["InputFiles"]["sediment_thickness_features"]]
    # --- Plate model files
    plate_model_dir = PARAMS["InputFiles"]["model_dir"]
    rotation_filenames = [os.path.join(plate_model_dir, i) for i in PARAMS['InputFiles']['rotation_files']]
    topology_filenames = [os.path.join(plate_model_dir, i) for i in PARAMS['InputFiles']['topology_files']]
    coastline_filename = '%s/%s' % (plate_model_dir, PARAMS['InputFiles']['coastline_file'])

    # --- grid spacing
    grid_spacing = PARAMS["GridParameters"]["distance_grid_spacing"]
    lon_min = PARAMS["GridParameters"]["lon_min"]
    lon_max = PARAMS["GridParameters"]["lon_max"]
    lat_min = PARAMS["GridParameters"]["lat_min"]
    lat_max = PARAMS["GridParameters"]["lat_max"]

    # --- time parameters
    min_time = int(PARAMS["TimeParameters"]["time_min"])           # Not truly a min_time, - parts 1 and 3 require a 0 Ma shapefile
    # oldest time to reconstruct to (will default to 0 Ma for min time)
    max_time = int(PARAMS["TimeParameters"]["time_max"])
    time_step = int(PARAMS["TimeParameters"]["time_step"])      # Myrs to increment age by in loop
    
    # running parameters
    num_cpus = PARAMS["Parameters"]["number_of_cpus"] # number of cpus to use. Reduce if required!

except IndexError:
    print('*** No yaml file given, using parameters set in script itself ***')

    model_name = 'trunk2022_v2'
    paleobathymetry_main_output_dir = './paleobathymetry_output_trunk2022_v2/'
    include_date_in_output_dir = 'yes'
    date = 'today'

    sediment_thickness_output_dir = './sediment_thickness_D17'
    sediment_thickness_within_main_output_dir = 'yes'

    # --- location of gplates files on your computer
    # DON'T FORGET TO UPDATE ocean_basin_proximity.py!
    # --- agegrids
    agegrid_dir = '/Users/nickywright/Data/Age/Muller2019-Young2019-Cao2020_Agegrids/Muller2019-Young2019-Cao2020_netCDF'   # change folder name if needed
    agegrid_filename = 'Muller2019-Young2019-Cao2020_AgeGrid-'    # everything before 'time'
    agegrid_filename_ext = '.nc'   # Everything after the time. Generally 'nc', but sometimes is 'grd'

    # --- input files
    proximity_features_files = [
    	'input_data/Global_EarthByte_GeeK07_COBLineSegments_2016_v4.gpmlz', # this is included in this repository
    ]

    # --- Plate model files
    # --- topologies and other files
    plate_model_dir = '/Users/nickywright/repos/usyd/EarthBytePlateMotionModel-ARCHIVE/Global_Model_WD_Internal_Release_2022_v2'
    rotation_filenames = [
        '%s/Global_250-0Ma_Rotations.rot' % plate_model_dir,
        '%s/Global_410-250Ma_Rotations.rot' % plate_model_dir,
        '%s/Alps_Mesh_Rotations.rot' % plate_model_dir,
        '%s/Andes_Flat_Slabs_Rotations.rot' % plate_model_dir,
        '%s/Andes_Rotations.rot' % plate_model_dir,
        '%s/Australia_Antarctica_Mesh_Rotations.rot' % plate_model_dir,
        '%s/Australia_North_Zealandia_Rotations.rot' % plate_model_dir,
        '%s/Eurasia_Arabia_Mesh_Rotations.rot' % plate_model_dir,
        '%s/North_America_Flat_Slabs_Rotations.rot' % plate_model_dir,
        '%s/North_America_Mesh_Rotations.rot' % plate_model_dir,
        '%s/North_China_Mesh_Rotations.rot' % plate_model_dir,
        '%s/South_Atlantic_Rotations.rot' % plate_model_dir,
        '%s/South_China_DeformingModel.rot' % plate_model_dir,
        '%s/Southeast_Asia_Rotations.rot' % plate_model_dir,
    ]

    topology_filenames = [
        '%s/Alps_Deforming_Mesh.gpml' % plate_model_dir,
        '%s/Alps_Mesh_Topologies.gpml' % plate_model_dir,
        '%s/America_Anyui_Deforming_Mesh.gpml' % plate_model_dir,
        '%s/America_Anyui_Mesh_Topologies.gpml' % plate_model_dir,
        '%s/Andes_Deforming_Mesh.gpml' % plate_model_dir,
        '%s/Andes_Flat_Slabs_Topologies.gpml' % plate_model_dir,
        '%s/Andes_Mesh_Topologies.gpml' % plate_model_dir,
        '%s/Arctic_Eurasia_Deforming_Mesh.gpml' % plate_model_dir,
        '%s/Arctic_Eurasia_Mesh_Topologies.gpml' % plate_model_dir,
        '%s/Australia_Antarctica_Deforming_Mesh.gpml' % plate_model_dir,
        '%s/Australia_Antarctica_Mesh_Topologies.gpml' % plate_model_dir,
        '%s/Australia_North_Zealandia_Deforming_Mesh.gpml' % plate_model_dir,
        '%s/Australia_North_Zealandia_Mesh_Topologies.gpml' % plate_model_dir,
        '%s/Baja_Deforming_Mesh.gpml' % plate_model_dir,
        '%s/Coral_Sea_Deforming_Mesh.gpml' % plate_model_dir,
        '%s/Coral_Sea_Topologies.gpml' % plate_model_dir,
        '%s/East_African_Rift_Deforming_Mesh_and_Topologies.gpml' % plate_model_dir,
        '%s/East-West_Gondwana_Deforming_Mesh_and_Topologies.gpml' % plate_model_dir,
        '%s/Ellesmere_Deforming_Mesh.gpml' % plate_model_dir,
        '%s/Eurasia_Arabia_Deforming_Mesh_and_Topologies.gpml' % plate_model_dir,
        '%s/Global_Mesozoic-Cenozoic_PlateBoundaries.gpml' % plate_model_dir,
        '%s/Global_Paleozoic_PlateBoundaries.gpml' % plate_model_dir,
        '%s/Greater_India_Deforming_Mesh.gpml' % plate_model_dir,
        '%s/Greater_India_Mesh_Topologies.gpml' % plate_model_dir,
        '%s/Inactive_Meshes_and_Topologies.gpml' % plate_model_dir,
        '%s/North_America_Mesh_Topologies.gpml' % plate_model_dir,
        '%s/North_Atlantic_Deforming_Mesh.gpml' % plate_model_dir,
        '%s/North_Atlantic_Mesh_Topologies.gpml' % plate_model_dir,
        '%s/North_China_Mesh_Topologies.gpml' % plate_model_dir,
        '%s/Northern_Andes_Deforming_Mesh.gpml' % plate_model_dir,
        '%s/Northern_Andes_Mesh_Topologies.gpml' % plate_model_dir,
        '%s/Papua_New_Guinea_Deforming_Meshes.gpml' % plate_model_dir,
        '%s/Papua_New_Guinea_Mesh_Topologies.gpml' % plate_model_dir,
        '%s/Scotia_Deforming_Mesh_and_Topologies.gpml' % plate_model_dir,
        '%s/Siberia_Eurasia_Deforming_Mesh.gpml' % plate_model_dir,
        '%s/Siberia_Eurasia_Mesh_Topologies.gpml' % plate_model_dir,
        '%s/South_Atlantic_Deforming_Mesh.gpml' % plate_model_dir,
        '%s/South_Atlantic_Mesh_Topologies.gpml' % plate_model_dir,
        '%s/South_China_Mesh_Topologies.gpml' % plate_model_dir,
        '%s/South_China_DeformingElements.gpml' % plate_model_dir,
        '%s/South_Zealandia_Deforming_Mesh.gpml' % plate_model_dir,
        '%s/South_Zealandia_Mesh_Topologies.gpml' % plate_model_dir,
        '%s/Southeast_Asia_Deforming_Mesh.gpml' % plate_model_dir,
        '%s/Southeast_Asia_Mesh_Topologies.gpml' % plate_model_dir,
        '%s/West_Antarctic_Zealandia_Deforming_Mesh.gpml' % plate_model_dir,
        '%s/West_Antarctica_Zealandia_Mesh_Topologies.gpml' % plate_model_dir,
        '%s/Western_North_America_Deforming_Mesh.gpml' % plate_model_dir,
        '%s/Western_Tethys_Deforming_Mesh.gpml' % plate_model_dir,
        '%s/Western_Tethys_Tectonic_Boundary_Topologies.gpml' % plate_model_dir]
    coastline_filename = '%s/StaticGeometries/Coastlines/Global_coastlines_low_res.shp' % plate_model_dir

    # --- grid spacing
    grid_spacing = 0.2

    min_time = 0
    max_time = 250
    time_step = 1

    num_cpus = multiprocessing.cpu_count() - 1 # number of cpus to use. Reduce if required!

# ------------------------------------
# --- set output directory
if sediment_thickness_within_main_output_dir.lower() in ['true', '1', 't', 'y', 'yes']:
    if include_date_in_output_dir.lower() in ['true', '1', 't', 'y', 'yes']:
        if date == 'today':
            date = today
            output_dir = '%s/%s/%s/distances_%sd' % (paleobathymetry_main_output_dir, date, sediment_thickness_output_dir, grid_spacing)
        else:
            output_dir = '%s/%s/%s/distances_%sd' % (paleobathymetry_main_output_dir, date, sediment_thickness_output_dir, grid_spacing)
    else:
        output_dir = '%s/%s/distances_%sd' % (paleobathymetry_main_output_dir, sediment_thickness_output_dir, grid_spacing)
else:
    output_dir = '%s/distances_%sd' % (sediment_thickness_output_dir, grid_spacing)


proximity_threshold_kms = 3000


# ------------------------------------------
# END USER INPUT
# ------------------------------------------

# print(coastline_filename)
# -----
if not os.path.exists(output_dir):
    print('%s does not exist, creating now... ' % output_dir)
    os.makedirs(output_dir)

# ----- 
def generate_distance_grid(time):
    py_cmd='python3'
    if shutil.which('python3') is None:
        py_cmd = 'python'
    
    command_line = [py_cmd, 'ocean_basin_proximity.py']
    command_line.extend(['-r'])
    command_line.extend('{0}'.format(rotation_filename) for rotation_filename in rotation_filenames)
    command_line.extend(['--coastline_filename', coastline_filename])
    command_line.extend(['-m'])
    command_line.extend('{0}'.format(proximity_features_file) for proximity_features_file in proximity_features_files)
    command_line.extend(['-s'])
    command_line.extend('{0}'.format(topology_filename) for topology_filename in topology_filenames)
    command_line.extend([
            '-g',
            '{0}/{1}{2}{3}'.format(agegrid_dir, agegrid_filename, time, agegrid_filename_ext),
            '-y {0}'.format(time),
            '-n',
            # Use all feature types in proximity file (according to Dietmar)...
            #'-b',
            #'PassiveContinentalBoundary',
            '-x',
            '{0}'.format(max_time),
            '-t',
            '1',
            '-i',
            '{0}'.format(grid_spacing),
            #'-q',
            #str(proximity_threshold_kms),
            #'-d', # output distance with time
            '-j',
            '-w',
            '-c',
            str(1),
            '{0}/distance_{1}_{2}'.format(output_dir, grid_spacing, time)])
    
    print('Time:', time)
    
    #print(' '.join(command_line))
    call_system_command(command_line)
    

    # Clamp the mean distance grids (and remove xy files).
    # Also rename the mean distance grids so that 'time' is at the end of the base filename -
    # this way we can import them as time-dependent raster into GPlates version 2.0 and earlier.
    #
    
    src_mean_distance_basename = '{0}/distance_{1}_{2}_mean_distance'.format(output_dir, grid_spacing, time)
    dst_mean_distance_basename = '{0}/mean_distance_{1}d_{2}'.format(output_dir, grid_spacing, time)
    
    src_mean_distance_xy = src_mean_distance_basename + '.xy'
    if os.access(src_mean_distance_xy, os.R_OK):
        os.remove(src_mean_distance_xy)
    
    src_mean_distance_grid = src_mean_distance_basename + '.nc'
    dst_mean_distance_grid = dst_mean_distance_basename + '.nc'
    
    if os.access(dst_mean_distance_grid, os.R_OK):
        os.remove(dst_mean_distance_grid)
    
    # Clamp mean distances.
    call_system_command(["gmt", "grdmath", "-fg", str(proximity_threshold_kms), src_mean_distance_grid, "MIN", "=", dst_mean_distance_grid])
    os.remove(src_mean_distance_grid)


# Wraps around 'generate_distance_grid()' so can be used by multiprocessing.Pool.map()
# which requires a single-argument function.
def generate_distance_grid_parallel_pool_function(args):
    try:
        return generate_distance_grid(*args)
    except KeyboardInterrupt:
        pass


def low_priority():
    """ Set the priority of the process to below-normal."""

    import sys
    try:
        sys.getwindowsversion()
    except AttributeError:
        isWindows = False
    else:
        isWindows = True

    if isWindows:
        import psutil
        
        p = psutil.Process()
        p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
    else:
        import os

        os.nice(1)


if __name__ == '__main__':
    
    try:
        num_cpus = num_cpus
    except NotImplementedError:
        num_cpus = 1
    
    print('Generating distance grids...')
    
    # Split the workload across the CPUs.
    pool = multiprocessing.Pool(num_cpus, initializer=low_priority)
    pool_map_async_result = pool.map_async(
            generate_distance_grid_parallel_pool_function,
            (
                (
                    time,
                ) for time in range(min_time, max_time + 1, time_step)
                #) for time in range(max_time, min_time - 1, -time_step) # Go backwards (can see results sooner).
            ),
            1) # chunksize

    # Apparently if we use pool.map_async instead of pool.map and then get the results
    # using a timeout, then we avoid a bug in Python where a keyboard interrupt does not work properly.
    # See http://stackoverflow.com/questions/1408356/keyboard-interrupts-with-pythons-multiprocessing-pool
    pool_map_async_result.get(99999)
