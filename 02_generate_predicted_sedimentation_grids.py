
# Try importing 'ptt' first. If that fails then try 'gplately.ptt' (GPlately now contains PlateTectonicTools).
try:
    from ptt.utils.call_system_command import call_system_command
except ImportError:
    from gplately.ptt.utils.call_system_command import call_system_command
import multiprocessing
import os
import sys

from datetime import datetime
import yaml
try:
    from yaml import Cloader as Loader
except ImportError:
    from yaml import Loader

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
""" ---------- Part 2 of the prediciting-sediment-thickness workflow ----------
This script creates grids (netcdfs) of sediment thickness and sediment rate through time

Requirements amd Inputs:
    - Python
    - Python scripts (predict_sediment_thickness.py, predict sedimentation_rate.py) - should be in this directory
    - GMT 5 (or later)
    - Files associated with a tectonic model, in particular, the agegrids
    - PlateTectonicTools
    - pyGPlates

To modify the sediment thickness relationship (e.g. for a new present-day agegrid or sediment thickness grid),
you will need to relcalculate the polynomial coefficients, and enter them into this script.

The polynomial coefficients are calculated in the folder 'python_notebooks_and_input_data_archive', in the jupyter
notebooks 'sediment_thick.ipynb' and 'sediment_rate.ipynb'. The values printed by the last cell of this notebook
can be entered into lines 215-222 (for sedimentation rate) and 278-284 (for sediment thickness) of this script.

Outputs:
    - folder named 'sedimentation_output' (or desired name, if changed), with 
      subfolders of sediment thickness and sediment rate grids through time.

2020-02-25: Added comments, created folders within the script itself
2022-08-26: Update parameters for GlobSed and latest agegrids. Modify dirs to be consistent with pt1
2024-02:    Improved memory usage.
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

    generate_sediment_thickness_grids = PARAMS["SedimentThicknessWorfkowParameters"]["generate_sediment_thickness_grids"]
    generate_sedimentation_rate_grids = PARAMS["SedimentThicknessWorfkowParameters"]["generate_sedimentation_rate_grids"]

except IndexError:
    print('*** No yaml file given. Make sure you specify it ***')



# ------------------------------------------
# BEGIN USER INPUT
# ------------------------------------------

# Use all CPUs.
#
# If False then use a single CPU.
# If True then use all CPUs (cores).
# If a positive integer then use that specific number of CPUs (cores).
#
use_all_cpus = num_cpus

# Base output directory.
if sediment_thickness_within_main_output_dir.lower() in ['true', '1', 't', 'y', 'yes']:
    if include_date_in_output_dir.lower() in ['true', '1', 't', 'y', 'yes']:
        if date == 'today':
            date = today
            output_base_dir = '%s/%s/traditional_paleobathymetry/%s/sediment_thickness_D17' % (paleobathymetry_main_output_dir, date, sediment_thickness_output_dir)
            base_dir = '%s/%s' % (paleobathymetry_main_output_dir, date)
        else:
            output_base_dir = '%s/%s/traditional_paleobathymetry/%s/sediment_thickness_D17' % (paleobathymetry_main_output_dir, date, sediment_thickness_output_dir)
            base_dir = '%s/%s' % (paleobathymetry_main_output_dir, date)
    else:
        output_base_dir = '%s/traditional_paleobathymetry/%s/sediment_thickness_D17' % (paleobathymetry_main_output_dir, sediment_thickness_output_dir)
        base_dir = paleobathymetry_main_output_dir
else:
    if include_date_in_output_dir.lower() in ['true', '1', 't', 'y', 'yes']:
        if date == 'today':
            date = today
            base_dir = '%s/%s/sediment_thickness_D17' % (paleobathymetry_main_output_dir, date)
            output_base_dir = '%s/%s/sediment_thickness_D17' % (sediment_thickness_output_dir, date)
        else:
            base_dir = '%s/%s/sediment_thickness_D17' % (paleobathymetry_main_output_dir, date)
            output_base_dir = '%s/%s/sediment_thickness_D17' % (sediment_thickness_output_dir, date)
    else:
        output_base_dir = '%s/sediment_thickness_D17' % (sediment_thickness_output_dir)
        base_dir = paleobathymetry_main_output_dir

print("... Output directory: " , output_base_dir)

# Whether to generate predicted sediment thickness grids and/or sedimentation rate grids.
if generate_sediment_thickness_grids.lower() in ['true', '1', 't', 'y', 'yes']:
    generate_sediment_thickness_grids = True
else:
    generate_sediment_thickness_grids = False

if generate_sedimentation_rate_grids.lower() in ['true', '1', 't', 'y', 'yes']:
    generate_sedimentation_rate_grids = True
else:
    generate_sedimentation_rate_grids = False

distance_grid_spacing = 0.1   # grid spacing of input distance grids
grid_spacing = grid_spacing   # grid spacing of output sedimentation grids

# Distance grid files (from part 1)
#     The "{}" parts are substituted here now (in this str.format() call) whereas the escaped "{{...}}" part is subsituted later (with each 'time').
distance_grid_filenames_format = '{0}/distances_{1:.1f}d/mean_distance_{1:.1f}d_{{:.1f}}.nc'.format(output_base_dir, distance_grid_spacing)

# Output directory name.
sediment_output_sub_dir = 'sedimentation_output'

# Age grid files.
#
# The format string to generate age grid filenames (using the age grid paleo times in the range [min_time, max_time]).
# Use a string section like "{:.1f}" to for the age grid paleo time. The ".1f" part means use the paleo time to one decimal place
# (see Python\'s str.format() function) such that a time of 100 would be substituted as "100.0".
# This string section will get replaced with each age grid time in turn (to generate the actual age grid filenames).
age_grid_filenames_format = '%s/%s{:0>%s.0f}%s' % (agegrid_dir, agegrid_filename, agegrid_age_zero_padding, agegrid_filename_ext)

# ------------------------------------------
# END USER INPUT
# ------------------------------------------


# check if the base output directory exists. If it doesn't, create it.
if not os.path.exists(os.path.join(output_base_dir, sediment_output_sub_dir)):
    print('{} does not exist, creating now... '.format(os.path.join(output_base_dir, sediment_output_sub_dir)))
    os.mkdir(os.path.join(output_base_dir, sediment_output_sub_dir))


# ----- 
def generate_predicted_sedimentation_grid(
        time,
        predict_sedimentation_script,
        scale_sedimentation_rate,
        mean_age,
        mean_distance,
        variance_age,
        variance_distance,
        max_age,
        max_distance,
        age_distance_polynomial_coefficients,
        output_file_basename_prefix):
    
    command_line = [
            sys.executable,  # python
            predict_sedimentation_script,
            '-d',
            distance_grid_filenames_format.format(time),
            '-g',
            age_grid_filenames_format.format(time),
            '-i',
            str(grid_spacing),
            '-w',
            '-m',
            str(mean_age),
            str(mean_distance),
            '-v',
            str(variance_age),
            str(variance_distance),
            '-x',
            str(max_age),
            str(max_distance),
            '-f']
    command_line.extend(str(coeff) for coeff in age_distance_polynomial_coefficients)
    # Only sediment rate requires scaling (sediment thickness does not)...
    if scale_sedimentation_rate is not None:
        command_line.extend([
                '-s',
                str(scale_sedimentation_rate)])
    
    output_filename_prefix = '{}_{:.1f}'.format(output_file_basename_prefix, time)
    command_line.extend([
            '--',
            output_filename_prefix])
    
    #print('Time:', time)
    #print(command_line)

    #print(' '.join(command_line))
    
    # Execute the command.
    call_system_command(command_line)


# Wraps around 'generate_predicted_sedimentation_grid()' so can be used by multiprocessing.Pool.map()
# which requires a single-argument function.
def generate_predicted_sedimentation_grid_parallel_pool_function(args):
    try:
        return generate_predicted_sedimentation_grid(*args)
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
        try:
            import psutil
        except ImportError:
            pass
        else:
            p = psutil.Process()
            p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
    else:
        import os

        os.nice(1)



if __name__ == '__main__':

    times = range(min_time, max_time + 1, time_step)

    # Machine learning training parameters.
    # These come from the "sediment_rate_decomp" IPython notebook (using sklearn Python module).

    #
    # Predict sedimentation *rate* results in "sediment_rate_decomp_v5.ipynb" from:
    #     
    #     _, regressor_trained_no_river =  geo_preprocess3.regression(data=data_train, 
    #                                                                     regressor=regressor_no_river, 
    #                                                                     n_splits=n_splits,
    #                                                                     lon_ind=lon_ind, 
    #                                                                     lat_ind=lat_ind, 
    #                                                                     y_ind=-1,
    #                                                                     logy=True)
    #     
    #     print('Mean of age and distance:', regressor_trained_no_river.named_steps['stand'].mean_)
    #     print('Variance of age and distance:', regressor_trained_no_river.named_steps['stand'].var_)
    #     print('Polynomial coefficients:', regressor_trained_no_river.named_steps['linear'].coef_)
    #     print('Polynomial intercept:', regressor_trained_no_river.named_steps['linear'].intercept_)
    #     print('Polynomial feature names:', regressor_trained_no_river.named_steps['poly'].get_feature_names())
    #
    #
    # predict_sedimentation_script = 'predict_sedimentation_rate.py'
    # scale_sedimentation_rate = 10.0  # Scale predicted rate (cm/Ky) to (m/My).
    # mean_age = 57.08516053
    # mean_distance = 2004.37249998
    # variance_age = 1.57169637e+03
    # variance_distance = 2.43160312e+06
    # age_distance_polynomial_coefficients = [
    #         0.0 , -0.52275531, -0.51023915,  0.34082993, -0.08491046, 0.5764176 , -0.0704285 , -0.01460767,  0.1403967 , -0.24019863]

    #
    # Predict sedimentation *rate* results in "sediment_rate_decomp_v5.ipynb" from:
    #
    #     dataq = data[:, [lon_ind, lat_ind, age_ind, passive_dis_ind, sedrate_ind]]
    #     geo_preprocess3.two_feature_analysis(dataq, regressor, 2, 3, 'age', 
    #                                          'distance to passive margin', 'predicted log sedrate',
    #                                          query_size=20)
    #     
    #     print('Mean of age and distance:', regressor.named_steps['stand'].mean_)
    #     print('Variance of age and distance:', regressor.named_steps['stand'].var_)
    #     print('Polynomial coefficients:', regressor.named_steps['linear'].coef_)
    #     print('Polynomial intercept:', regressor.named_steps['linear'].intercept_)
    #     print('Polynomial feature names:', regressor.named_steps['poly'].get_feature_names())
    #
    #
    if generate_sedimentation_rate_grids:
        
        # updated for GlobSed and TRUNK agegrids
        predict_sedimentation_script = 'predict_sedimentation_rate.py'
        #scale_sedimentation_rate = 1.0  # Keep predicted rate in (cm/Ky).
        scale_sedimentation_rate = 10.0  # Scale predicted rate (cm/Ky) to (m/My).
        mean_age = 61.17716597
        mean_distance = 1835.10750592
        variance_age =  1934.78513885
        variance_distance = 1207587.8548734
        max_age = 191.87276
        max_distance = 3000.
        age_distance_polynomial_coefficients = [
                1.350082937086441, -0.26385415, -0.07516542,  0.39197707, -0.15475392,
            0.        , -0.13196083,  0.02481208, -0.        , -0.47570021]
        
        output_dir = os.path.join(output_base_dir, sediment_output_sub_dir, 'predicted_rate')
        output_file_basename_prefix = os.path.join(output_dir, 'sed_rate_{:.1f}d'.format(grid_spacing))
        
        # check if the output dir exists. If not, create
        if not os.path.exists(output_dir):
            print('{} does not exist, creating now... '.format(output_dir))
            os.mkdir(output_dir)

        print('Generating predicted sedimentation rate grids...')

        if use_all_cpus:
        
            # If 'use_all_cpus' is a bool (and therefore must be True) then use all available CPUs...
            if isinstance(use_all_cpus, bool):
                try:
                    num_cpus = multiprocessing.cpu_count()
                except NotImplementedError:
                    num_cpus = 1
            # else 'use_all_cpus' is a positive integer specifying the number of CPUs to use...
            elif isinstance(use_all_cpus, int) and use_all_cpus > 0:
                num_cpus = use_all_cpus
            else:
                raise TypeError('use_all_cpus: {} is neither a bool nor a positive integer'.format(use_all_cpus))
            
            try:
                # Split the workload across the CPUs.
                pool = multiprocessing.Pool(num_cpus, initializer=low_priority)
                pool_map_async_result = pool.map_async(
                        generate_predicted_sedimentation_grid_parallel_pool_function,
                        (
                            (
                                time,
                                predict_sedimentation_script,
                                scale_sedimentation_rate,
                                mean_age,
                                mean_distance,
                                variance_age,
                                variance_distance,
                                max_age,
                                max_distance,
                                age_distance_polynomial_coefficients,
                                output_file_basename_prefix
                            ) for time in times
                        ),
                        1) # chunksize

                # Apparently if we use pool.map_async instead of pool.map and then get the results
                # using a timeout, then we avoid a bug in Python where a keyboard interrupt does not work properly.
                # See http://stackoverflow.com/questions/1408356/keyboard-interrupts-with-pythons-multiprocessing-pool
                pool_map_async_result.get(999999)
            except KeyboardInterrupt:
                # Note: 'finally' block below gets executed before returning.
                pass
            finally:
                pool.close()
                pool.join()

        else:
            for time in times:
                generate_predicted_sedimentation_grid(
                        time,
                        predict_sedimentation_script, scale_sedimentation_rate,
                        mean_age, mean_distance,
                        variance_age, variance_distance,
                        max_age, max_distance,
                        age_distance_polynomial_coefficients,
                        output_file_basename_prefix)
    
    #
    # Predict sediment *thickness* results in "sediment_thick_v5.ipynb" from:
    #
    #     dataq = data[:, [lon_ind, lat_ind, age_ind, passive_dis_ind, sedthick_ind]]
    #     geo_preprocess3.two_feature_analysis(dataq, regressor, 2, 3, 'age', 
    #                                          'distance to passive margin', 'predicted log thickness',
    #                                          query_size=20)
    #     
    #     print('Mean of age and distance:', regressor.named_steps['stand'].mean_)
    #     print('Variance of age and distance:', regressor.named_steps['stand'].var_)
    #     print('Max of age and distance:', np.max(data[:, [age_ind, passive_dis_ind]], axis=0))
    #     print('Polynomial coefficients:', regressor.named_steps['linear'].coef_)
    #     print('Polynomial intercept:', regressor.named_steps['linear'].intercept_)
    #     print('Polynomial feature names:', regressor.named_steps['poly'].get_feature_names())
    #
    #
    if generate_sediment_thickness_grids:
        
        # updated for GlobSed and TRUNK agegrids (NW 20220826)
        predict_sedimentation_script = 'predict_sediment_thickness.py'
        scale_sedimentation_rate = None  # No scaling - we're predicting sediment thickness (not rate).
        mean_age =  61.18406823
        mean_distance = 1835.28118479
        variance_age = 1934.6999014
        variance_distance = 1207521.8995806
        max_age = 191.87276
        max_distance = 3000.
        age_distance_polynomial_coefficients = [
                5.441401190368497,  0.46893096, -0.07320928, -0.24077496, -0.10840657,
            0.00381672,  0.06831728,  0.01179914,  0.01158149, -0.39880562]

        output_dir = os.path.join(output_base_dir, sediment_output_sub_dir, 'predicted_thickness')
        output_file_basename_prefix = os.path.join(output_dir, 'sed_thick_{:.1f}d'.format(grid_spacing))

        # check if the output dir exists. If not, create
        if not os.path.exists(output_dir):
            print('{} does not exist, creating now... '.format(output_dir))
            os.mkdir(output_dir)
        print('Generating predicted sediment thickness grids...')

        if use_all_cpus:

            # If 'use_all_cpus' is a bool (and therefore must be True) then use all available CPUs...
            if isinstance(use_all_cpus, bool):
                try:
                    num_cpus = multiprocessing.cpu_count()
                except NotImplementedError:
                    num_cpus = 1
            # else 'use_all_cpus' is a positive integer specifying the number of CPUs to use...
            elif isinstance(use_all_cpus, int) and use_all_cpus > 0:
                num_cpus = use_all_cpus
            else:
                raise TypeError('use_all_cpus: {} is neither a bool nor a positive integer'.format(use_all_cpus))
            
            try:
                # Split the workload across the CPUs.
                pool = multiprocessing.Pool(num_cpus, initializer=low_priority)
                pool_map_async_result = pool.map_async(
                        generate_predicted_sedimentation_grid_parallel_pool_function,
                        (
                            (
                                time,
                                predict_sedimentation_script,
                                scale_sedimentation_rate,
                                mean_age,
                                mean_distance,
                                variance_age,
                                variance_distance,
                                max_age,
                                max_distance,
                                age_distance_polynomial_coefficients,
                                output_file_basename_prefix
                            ) for time in times
                        ),
                        1) # chunksize

                # Apparently if we use pool.map_async instead of pool.map and then get the results
                # using a timeout, then we avoid a bug in Python where a keyboard interrupt does not work properly.
                # See http://stackoverflow.com/questions/1408356/keyboard-interrupts-with-pythons-multiprocessing-pool
                pool_map_async_result.get(999999)
            except KeyboardInterrupt:
                # Note: 'finally' block below gets executed before returning.
                pass
            finally:
                pool.close()
                pool.join()

        else:
            for time in times:
                generate_predicted_sedimentation_grid(
                        time,
                        predict_sedimentation_script, scale_sedimentation_rate,
                        mean_age, mean_distance,
                        variance_age, variance_distance,
                        max_age, max_distance,
                        age_distance_polynomial_coefficients,
                        output_file_basename_prefix)
