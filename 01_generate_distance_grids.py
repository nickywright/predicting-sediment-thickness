
# Try importing 'ptt' first. If that fails then try 'gplately.ptt' (GPlately now contains PlateTectonicTools).
try:
    from ptt.utils.call_system_command import call_system_command
except ImportError:
    from gplately.ptt.utils.call_system_command import call_system_command
import os, sys

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
    - PlateTectonicTools
    - pyGPlates

Outputs:
    - directory named 'distances_0.1d', with mean distance grids (in metres) through time.

2020-02-14: Added comments, removed hardcoded names (for rotation file, etc) from definitions
2022-08-29: Added more comments (NW)
2024-02:    Improved running time and memory usage.
2024-05-09: Modified this script to be compatible with a yaml file
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
    agegrid_age_zero_padding = PARAMS["InputFiles"]["agegrid_age_zero_padding"]
    # --- input file
    proximity_features_files = [PARAMS["InputFiles"]["sediment_thickness_features"]]
    plate_boundary_obstacles = [PARAMS["InputFiles"]["plate_boundary_obstacles_list"]]
    # --- Plate model files
    plate_model_dir = PARAMS["InputFiles"]["model_dir"]
    rotation_filenames = [os.path.join(plate_model_dir, i) for i in PARAMS['InputFiles']['rotation_files']]
    topology_filenames = [os.path.join(plate_model_dir, i) for i in PARAMS['InputFiles']['topology_files']]
    coastline_filename = '%s/%s' % (plate_model_dir, PARAMS['InputFiles']['coastline_file'])

    anchor_plate_id = int(PARAMS["InputFiles"]["anchor_plate_id"])

    # --- grid spacing
    grid_spacing = PARAMS["GridParameters"]["grid_spacing"]
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
    sed_distance_internal_grid_spacing = PARAMS["SedimentThicknessWorfkowParameters"]["sed_distance_internal_grid_spacing"]

    num_cpus = PARAMS["Parameters"]["number_of_cpus"] # number of cpus to use. Reduce if required!
    max_memory_usage_in_gb = PARAMS["SedimentThicknessWorfkowParameters"]["max_memory_usage_in_gb_sedthickness"]
    use_continent_contouring_workflow = PARAMS["SedimentThicknessWorfkowParameters"]["use_continent_contouring_workflow"]
    max_topological_reconstruction_time = PARAMS["SedimentThicknessWorfkowParameters"]["max_topological_reconstruction_time"]
    clamp_mean_proximity_kms = PARAMS["SedimentThicknessWorfkowParameters"]["clamp_mean_proximity_kms"]

except IndexError:
    print('*** No yaml file given. Make sure you specify it ***')

# ------------------------------------------
# ------------------------------------
# --- set output directory

if sediment_thickness_within_main_output_dir.lower() in ['true', '1', 't', 'y', 'yes']:
    if include_date_in_output_dir.lower() in ['true', '1', 't', 'y', 'yes']:
        if date == 'today':
            date = today
            output_dir = '%s/%s/traditional_paleobathymetry/%s/sediment_thickness_D17/distances_%sd' % (paleobathymetry_main_output_dir, date, sediment_thickness_output_dir, grid_spacing)
            base_dir = '%s/%s' % (paleobathymetry_main_output_dir, date)
        else:
            output_dir = '%s/%s/traditional_paleobathymetry/%s/sediment_thickness_D17/distances_%sd' % (paleobathymetry_main_output_dir, date, sediment_thickness_output_dir, grid_spacing)
            base_dir = '%s/%s' % (paleobathymetry_main_output_dir, date)
    else:
        output_dir = '%s/traditional_paleobathymetry/%s/sediment_thickness_D17/distances_%sd' % (paleobathymetry_main_output_dir, sediment_thickness_output_dir, grid_spacing)
        base_dir = paleobathymetry_main_output_dir
else:
    if include_date_in_output_dir.lower() in ['true', '1', 't', 'y', 'yes']:
        if date == 'today':
            date = today
            base_dir = '%s/%s' % (paleobathymetry_main_output_dir, date)
            output_dir = '%s/%s/sediment_thickness_D17/distances_%sd' % (sediment_thickness_output_dir, date, grid_spacing)
        else:
            base_dir = '%s/%s' % (paleobathymetry_main_output_dir, date)
            output_dir = '%s/%s/sediment_thickness_D17/distances_%sd' % (sediment_thickness_output_dir, date, grid_spacing)
    else:
        output_dir = '%s/sediment_thickness_D17/distances_%sd' % (sediment_thickness_output_dir, grid_spacing)
        base_dir = paleobathymetry_main_output_dir

print("... Output directory: " , output_dir)


# ------------------------------------------

# Use all CPUs.
#
# If False then use a single CPU.
# If True then use all CPUs (cores).
# If a positive integer then use that specific number of CPUs (cores).
use_all_cpus = num_cpus

# The maximum amount of memory (in GB) to use (it's divided across the CPUs).
# Set to the amount of physical RAM (or less).
# If set to 'None' then there is no limit.
#max_memory_usage_in_gb = None
max_memory_usage_in_gb = max_memory_usage_in_gb

# The grid spacing used when calculating internal distance grids.
# This parameter significantly affects the time it takes to generate distance grids in this workflow.
# So typically this should have a higher grid spacing (lower resolution) than 'grid_spacing' to make it run faster.
internal_grid_spacing = sed_distance_internal_grid_spacing

# The grid spacing of the final output distance grids.
# The final (higher resolution) distance grids are generated by upscaling the internal distance grids (calculated with 'internal_grid_spacing').
grid_spacing = grid_spacing

# The reference frame to generate the distance grids in.
#
# NOTE: The age grids must also be in this reference frame.
#
# Note: If the proximity/continent features have already been reconstructed
#       (eg, the features are actually time-dependent snapshots of reconstructions
#       generated from the continent contouring workflow) then they should remain
#       in the default reference frame (anchor plate zero in that workflow).
#       This is because they are assigned a plate ID of zero (in that workflow) and so
#       reconstructing them (in this workflow) relative to our anchor plate ID will
#       automatically position them correctly in our reference frame.
anchor_plate_id = anchor_plate_id


# ---- 
if use_continent_contouring_workflow.lower() in ['true', '1', 't', 'y', 'yes']:
    # use the output from the continent contouring workflow for the proximity_features and continent obstacle files
    proximity_features_files = ['%s/continent_contouring/passive_margin_features.gpmlz' % base_dir]
    continent_obstacle_files = ['%s/continent_contouring/continent_contour_features.gpmlz' % base_dir]

    # If continent obstacles are specified then any plate boundary sections with these specified feature types are also added as obstacles
    # (that water, and hence sediment, cannot pass through).
    #
    # This should default to mid-ocean ridges and subduction zones, but you can change this if desired.
    #
    # The format should match the format of http://www.gplates.org/docs/pygplates/generated/pygplates.FeatureType.html#pygplates.FeatureType.get_name .
    # For example, subduction zone is specified as SubductionZone (without the gpml: prefix).
    #
    # Note: These are ignored unless 'continent_obstacle_files' is also specified.
    #
    plate_boundary_obstacles = plate_boundary_obstacles


else:
    # Passive margin files (that distances are calculated relative to).
    #
    # Note: These can be passive margins generated from *contoured* continents (see https://github.com/EarthByte/continent-contouring).
    #       Ensure that the same rotation model is used for contouring (as is used in this workflow).
    proximity_features_files = [proximity_features_files, # this is included in this repository
    ]

    # Optional continent obstacles that the shortest distance path must go around (ie, water flowing around continents, rather than through).
    # If not specifed then distances are minimum straight-line (great circle arc) distances from ocean points to proximity geometries.
    # Obstacles can be both polygons and polylines.
    #
    # Note: These can be *contoured* continents (see https://github.com/EarthByte/continent-contouring).
    #       Ensure that the same rotation model is used for contouring (as is used in this workflow).
    #
    #continent_obstacle_files = None
    continent_obstacle_files = [coastline_filename]


print('... Using proximity_features_files: %s' % proximity_features_files)
print('... Using continent_obstacle_files: %s' % continent_obstacle_files)
print('... Using plate_boundary_obstacles: %s' % plate_boundary_obstacles)


# Age grid files.
# The format string to generate age grid filenames (using the age grid paleo times in the range [min_time, max_time]).
# Use a string section like "{:.1f}" to for the age grid paleo time. The ".1f" part means use the paleo time to one decimal place
# (see Python\'s str.format() function) such that a time of 100 would be substituted as "100.0".
# This string section will get replaced with each age grid time in turn (to generate the actual age grid filenames).
# age_grid_filenames_format = '/Users/nickywright/Data/Age/Muller2019-Young2019-Cao2020_Agegrids/Muller2019-Young2019-Cao2020_netCDF/Muller2019-Young2019-Cao2020_AgeGrid-{:.0f}.nc'
age_grid_filenames_format = '%s/%s{:0>%s.0f}%s' % (agegrid_dir, agegrid_filename, agegrid_age_zero_padding, agegrid_filename_ext)

# For each distance grid do not reconstruct ocean points earlier than 'max_topological_reconstruction_time'
# (each ocean point is reconstructed back to its age grid value or this value, whichever is smaller).
# This limit can be set to the earliest (max) reconstruction time of the topological model.
# If it's 'None' then only the age grid limits how far back each point is reconstructed.
max_topological_reconstruction_time = max_topological_reconstruction_time  # can be None to just use age grid as the limit

# Optionally clamp mean distances to this value (in kms).
#clamp_mean_proximity_kms = None
clamp_mean_proximity_kms = clamp_mean_proximity_kms
# ------------------------------------------
# END parameters
# ------------------------------------------

if not os.path.exists(output_dir):
    print('{} does not exist, creating now... '.format(output_dir))
    os.makedirs(output_dir, exist_ok=True)

# ----- 
def generate_distance_grids(times):
    
    # Calling the ocean basin proximity script.
    command_line = [sys.executable, 'ocean_basin_proximity.py']

    # Rotation files.
    command_line.append('-r')
    command_line.extend('{}'.format(rotation_filename) for rotation_filename in rotation_filenames)

    # Proximity files.
    command_line.append('-m')
    command_line.extend('{}'.format(proximity_features_file) for proximity_features_file in proximity_features_files)
    # Proximity features are non-topological.
    command_line.append('-n')

    # If using continent obstacles.
    if continent_obstacle_files:
        command_line.append('--continent_obstacle_filenames')
        command_line.extend('{}'.format(continent_obstacle_file) for continent_obstacle_file in continent_obstacle_files)
        # Plate boundary obstacles (feature types).
        # Can only be specified if "--continent_obstacle_filenames" is also specified.
        command_line.append('--plate_boundary_obstacle_feature_types')
        command_line.extend('{}'.format(plate_boundary_obstacle) for plate_boundary_obstacle in plate_boundary_obstacles)
    
    # Topological files.
    command_line.append('-s')
    command_line.extend('{}'.format(topology_filename) for topology_filename in topology_filenames)

    # Anchor plate ID.
    command_line.extend(['--anchor', '{}'.format(anchor_plate_id)])

    # Age grid filenames format.
    command_line.extend(['--age_grid_filenames_format', age_grid_filenames_format])
    # Age grid paleo times.
    command_line.append('--age_grid_paleo_times')
    command_line.extend(['{}'.format(time) for time in times])

    # Use all feature types in proximity file (according to Dietmar)...
    #command_line.extend(['-b', 'PassiveContinentalBoundary'])

    # Time increment is 1 Myr (this is for topological reconstruction of the ocean points).
    command_line.extend(['--time_increment', '1'])
    
    # If limiting the max topological reconstruction time.
    if max_topological_reconstruction_time is not None:
        command_line.extend(['-x', '{}'.format(max_topological_reconstruction_time)])
    
    # Internal grid spacing (for internal distance calculations).
    command_line.extend(['--ocean_basin_grid_spacing', '{}'.format(internal_grid_spacing)])
    
    # Grid spacing (for final output mean distance grids).
    command_line.extend(['--upscale_mean_std_dev_grid_spacing', '{}'.format(grid_spacing)])

    # Optionally clamp mean proximity.
    if clamp_mean_proximity_kms:
        command_line.extend(['--clamp_mean_distance', str(clamp_mean_proximity_kms)])

    # Don't output distance grids for all reconstruction times.
    # Only outputting a single "mean" (over all reconstruction times) distance grid.
    #command_line.append('--output_distance_with_time')

    # Output a "mean" (over all reconstruction times) distance grid.
    command_line.append('--output_mean_distance')

    # Don't output "standard deviation" (over all reconstruction times) distance grid.
    #command_line.append('--output_std_dev_distance')

    # Generate grd (".nc") files instead of xyz (".xy") files.
    command_line.append('--output_grd_files')

    if use_all_cpus:
        # If 'use_all_cpus' is a bool (and therefore must be True) then use all available CPUs...
        if isinstance(use_all_cpus, bool):
            num_cpus = None  # use default of all available CPUs
        # else 'use_all_cpus' is a positive integer specifying the number of CPUs to use...
        elif isinstance(use_all_cpus, int) and use_all_cpus > 0:
            num_cpus = use_all_cpus
        else:
            raise TypeError('use_all_cpus: {} is neither a bool nor a positive integer'.format(use_all_cpus))
    else:
        num_cpus = 1
    
    # Number of cores.
    # If None then not specified, and defaults to using all available cores.
    if num_cpus:
        command_line.extend(['--num_cpus', '{}'.format(num_cpus)])
    
    # The maximum amount of memory (in GB) to use (divided across the CPUs).
    if max_memory_usage_in_gb:
        command_line.extend(['--max_memory_usage', '{}'.format(max_memory_usage_in_gb)])

    # Distance grids output directory.
    command_line.append(output_dir)
    
    #print(' '.join(command_line))
    call_system_command(command_line)


if __name__ == '__main__':

    #import time as time_prof

    print('Generating distance grids...')
    
    #tprof_start = time_prof.perf_counter()

    times = range(min_time, max_time + 1, time_step)
    #times = range(max_time, min_time - 1, -time_step) # Go backwards (can see results sooner).

    # Generate the distance grids.
    try:
        generate_distance_grids(times)
    except KeyboardInterrupt:
        pass
    
    #tprof_end = time_prof.perf_counter()
    #print(f"Total time: {tprof_end - tprof_start:.2f} seconds")
