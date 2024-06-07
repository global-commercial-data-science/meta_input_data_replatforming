import DAXBuildDataset as build_ds
import DAXBuildMasterTables as build_master_table
import pandas as pd
import gc
from DAXModelTraining import main as train_model
from cdswritereadfunctions import load_input_file as rjf, write_json_to_file as wjf
from datetime import datetime, timedelta
from DAXGeneratePrediction import main as generate_prediction
from DAXBuildFinalOutput import main as build_output
from DAXBuildAnalyticsDS import main as databackup
import time


def main():

    for i in range(1):
        t0 = time.time()

        input_file = rjf('DAX_ControlFile.json')

        live_segments = [dmp for dmp in input_file['segment'] if input_file['segment'][dmp]['is_main']]
        drifted_segments = [dmp for dmp in input_file['segment'] if not input_file['segment'][dmp]['is_main']]
        print(live_segments)
        template = rjf('DAX_ControlFile_template.json')

        updated_input_file = input_file


        updated_input_file['start_date'] = (datetime.strptime(input_file["start_date"], '%Y-%m-%d') +
                                           timedelta(weeks=1)).strftime('%Y-%m-%d')

        print(updated_input_file['start_date'])
        
        wjf('DAX_ControlFile.json', updated_input_file)
        print(updated_input_file['start_date'], updated_input_file['segment_to_run'])



        print('Building master table')

        build_master_table.main()

        if len(live_segments) > 0:

            # Updating for live segments
            updated_input_file['segment_to_run'] = live_segments

            for segment in live_segments:
                updated_input_file['segment'][segment] = template['segment'][segment]

            wjf('DAX_ControlFile.json', updated_input_file)
            print(updated_input_file['start_date'], updated_input_file['segment_to_run'])
            
            build_ds.main()
            t1 = time.time()
            print(f'Dataset generated in {(t1 - t0)/60} minutes')

            t1 = time.time()

            train_model()
            t2 = time.time()
            print(f'model trained in {(t2 - t1) / 60} minutes')

            t2 = time.time()
            generate_prediction()
            t3 = time.time()
            print(f'model analysed in {(t3 - t2) / 60} minutes')

            t3 = time.time()
            build_output()
            t4 = time.time()
            print(f'final prediction generated in {(t4 - t3) / 60} minutes')

            print(f'complete retraining process took {(t4 - t0) / 60} minutes')
            print('...')


        #===================================prediction of non-drifted models=========================

        if len(drifted_segments) > 0:

            print('prediciton on non-drifted models started...')

            updated_input_file = rjf('DAX_ControlFile.json')

            updated_input_file['segment_to_run'] = drifted_segments
            wjf('DAX_ControlFile.json', updated_input_file)
            print(updated_input_file['start_date'], updated_input_file['segment_to_run'])
    
            print('starting prediction from pretrained model...')
            t6 = time.time()

            build_ds.main()


            t7 = time.time()
            print(f'dataset build in {t7-t6} seconds')

            t7 = time.time()
            generate_prediction()
            t8 = time.time()
            print(f'prediction generated in {t8 - t7} seconds')

            t8 = time.time()
            build_output()
            t9 = time.time()
            print(f'output built in {t9 - t8} seconds')

        updated_input_file = rjf('DAX_ControlFile.json')

        updated_input_file['segment_to_run'] = drifted_segments + live_segments
        wjf('DAX_ControlFile.json', updated_input_file)


       # databackup()

        for name in list(globals().keys()):
            if not name.startswith('_'):  # Don't delete system variables.
                value = globals()[name]
                if isinstance(value, pd.DataFrame):  # Only delete DataFrame objects.
                    del globals()[name]

        gc.collect()



if __name__ == '__main__':
    main()
