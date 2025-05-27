
import sys
import pandas as pd
import uproot
import os

param_list = ['run', 'event', 'pedestal_run', 'cmos_integral', 'cmos_mean', 'cmos_rms',
              't_DBSCAN', 't_variables', 'lp_len', 't_pedsub', 't_saturation', 't_zerosup',
              't_xycut', 't_rebin', 't_medianfilter', 't_noisered', 'nSc', 'sc_size', 'sc_nhits',
              'sc_integral', 'sc_corrintegral', 'sc_rms', 'sc_energy', 'sc_pathlength',
              'sc_theta', 'sc_length', 'sc_width', 'sc_longrms', 'sc_latrms', 'sc_lfullrms',
              'sc_tfullrms', 'sc_lp0amplitude', 'sc_lp0prominence', 'sc_lp0fwhm', 'sc_lp0mean',
              'sc_tp0fwhm', 'sc_xmean', 'sc_ymean', 'sc_xmax', 'sc_xmin', 'sc_ymax', 'sc_ymin',
              'sc_pearson', 'sc_tgaussamp', 'sc_tgaussmean', 'sc_tgausssigma', 'sc_tchi2',
              'sc_tstatus', 'sc_lgaussamp', 'sc_lgaussmean', 'sc_lgausssigma', 'sc_lchi2', 'sc_lstatus',
              'Lime_pressure', 'Atm_pressure', 'Lime_temperature', 'Atm_temperature', 'Humidity',
              'Mixture_Density', 'sc_redpixIdx','nRedpix']#,
              #'redpix_ix','redpix_iy','redpix_iz']

if len(sys.argv) != 2:
    print("Usage: python process_one.py <run_number>")
    sys.exit(1)

run = int(sys.argv[1])

try:
    print(f"Processing run {run}")
    rname = f"root/reco_run{run:05d}_3D.root"
    file = uproot.open(rname)
    df_run = file["Events"].arrays(param_list, library="pd")
    df_run["run_number"] = run

    os.makedirs("output", exist_ok=True)
    df_run.to_pickle(f"output/run_{run}.pkl")
    #print(f"Finished run {run}")
except Exception as e:
    print(f"Error processing run {run}: {e}")
