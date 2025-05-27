
import os
import sys
import pandas as pd
import numpy as np
import uproot
import awkward as ak
import matplotlib.pyplot as plt

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
              'Mixture_Density',  'sc_redpixIdx','nRedpix']#,'redpix_ix','redpix_iy','redpix_iz']

def convert_awkward_columns_to_lists(df):
    def safe_convert(val):
        if isinstance(val, ak.Array):
            return ak.to_list(val)
        elif isinstance(val, np.ndarray):
            return val.tolist()
        return val

    converted_df = df.copy()
    for col in df.columns:
        if isinstance(df[col].iloc[0], (ak.Array, np.ndarray, list)):
            converted_df[col] = df[col].apply(safe_convert)
    return converted_df

def to_numpy_column(col):
    if isinstance(col.iloc[0], list):
        try:
            return col.apply(np.array)
        except Exception:
            return col.apply(lambda x: np.array(x, dtype=object))
    return col

def main(run_number):
    in_path = f"output/run_{run_number}.pkl"
    out_path = f"output_filtered/run_{run_number}_filtered.parquet"

    if not os.path.exists(in_path):
        print(f"Run {run_number}: File not found.")
        return
    
    if not os.path.exists(out_path):
        os.makedirs(os.path.dirname(out_path), exist_ok=True)

    try:
        df = pd.read_pickle(in_path)
        df = convert_awkward_columns_to_lists(df)
        #df = df[df['run_number'] != df['pedestal_run']]

        sc_columns = [col for col in df.columns if col.startswith('sc_')]

        for _, row in df.iterrows():
            lengths = [len(row[col]) for col in sc_columns if isinstance(row[col], (list, np.ndarray))]
            if lengths and len(set(lengths)) != 1:
                print(f"Run {run_number}: Inconsistent sc_ lengths.")
                return

        df = df.apply(to_numpy_column)

        '''
        new_redpix_ix_all, new_redpix_iy_all, new_redpix_iz_all = [], [], []
        for _, row in df.iterrows():
            markers = np.array(row['sc_redpixIdx'])
            ix, iy, iz = row['redpix_ix'], row['redpix_iy'], row['redpix_iz']
            new_ix, new_iy, new_iz = [], [], []
            for j in range(len(markers)):
                marker = markers[j]
                if marker == -1:
                    new_ix.append([]); new_iy.append([]); new_iz.append([])
                else:
                    start = int(marker)
                    next_marker = next((markers[k] for k in range(j+1, len(markers)) if markers[k] != -1), len(ix))
                    end = int(next_marker) if next_marker else len(ix)
                    new_ix.append(list(ix[start:end]))
                    new_iy.append(list(iy[start:end]))
                    new_iz.append(list(iz[start:end]))
            new_redpix_ix_all.append(new_ix)
            new_redpix_iy_all.append(new_iy)
            new_redpix_iz_all.append(new_iz)

        df['new_redpix_ix'] = new_redpix_ix_all
        df['new_redpix_iy'] = new_redpix_iy_all
        df['new_redpix_iz'] = new_redpix_iz_all


        '''

        #redpix_cols = [col for col in df.columns if col.startswith('new_redpix_')]
        #for col in redpix_cols:
        #    df[col] = df[col].apply(lambda x: list(x) if isinstance(x, np.ndarray) else x)
        cols_to_explode = [col for col in df.columns if col.startswith('sc_')] #+ redpix_cols
        exploded_df = df.explode(cols_to_explode)

        #Debug print per filtro
        #bad_rows = exploded_df[~exploded_df['new_redpix_ix'].apply(lambda x: isinstance(x, (list, np.ndarray)))]
        #print(bad_rows[['run', 'event', 'new_redpix_ix']])

        
        #weird_rows = exploded_df[~exploded_df['new_redpix_ix'].apply(lambda x: isinstance(x, (list, np.ndarray)))]
        #print(weird_rows[['run', 'event', 'new_redpix_ix']].head(10))
        #print(weird_rows['new_redpix_ix'].map(type).value_counts())

        

        #exploded_df = exploded_df[exploded_df['new_redpix_ix'].apply(lambda x: isinstance(x, (list, np.ndarray)) and len(x) > 0)] #exploded_df = exploded_df[exploded_df['new_redpix_ix'].apply(lambda x: len(x) > 0)]
        exploded_df = exploded_df.reset_index(drop=True)
        #exploded_df = exploded_df.drop(columns=['redpix_ix', 'redpix_iy', 'redpix_iz'])


        exploded_df['sc_ymean'] = pd.to_numeric(exploded_df['sc_ymean'], errors='raise')
        exploded_df['sc_xmean'] = pd.to_numeric(exploded_df['sc_xmean'], errors='raise')
        exploded_df['sc_rms'] = pd.to_numeric(exploded_df['sc_rms'], errors='raise')
        exploded_df['sc_tgausssigma'] = pd.to_numeric(exploded_df['sc_tgausssigma'], errors='raise')

        filtered_df = exploded_df #[
            #(np.sqrt((exploded_df['sc_xmean'] - (2304*0.5))**2 + (exploded_df['sc_ymean'] - (2304*0.5))**2) < 800)# &
            #(exploded_df['sc_rms'] > 6) &
            #(exploded_df['sc_tgausssigma']*0.152 > 0.5)
        #]

        filtered_df.to_parquet(out_path, index=False)

        print(f"Run {run_number}: Filtered and saved.")

    except Exception as e:
        print(f"Run {run_number}: Skipped due to error: {e}")

if __name__ == "__main__":
    run = int(sys.argv[1])
    main(run)
