import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import os
from pathlib import Path

base_path = Path(__file__).parent
DATA_FILE_PATH= (base_path).resolve()

if __name__ == "__main__":
    filename='UCR_358_mapping_raw.xlsx'
    filename_final = 'UCR_358_mapping_updated.xlsx'
    file = os.path.join(DATA_FILE_PATH, filename)
    xl_parser = pd.ExcelFile(file)

    df_ucr= xl_parser.parse('UCR 358_2011')
    df_ucr=df_ucr[['UCR_358','DESC']]
    df_ucr['DESC'] = df_ucr['DESC'].replace(r'\s+|\\n', ' ', regex=True)
    df_ucr['UCR_358'] = df_ucr['UCR_358'].replace(r'\s+|\\n', ' ', regex=True)
    df_ucr['UCR_358']=df_ucr['UCR_358'].fillna('')

    #take care of wrapped lines - If the next line UCR 358 code is empty then copy the desc and add it o the this line
    df_ucr['DESC']=np.where((df_ucr['UCR_358'].shift(-1).str.strip()==''),(df_ucr['DESC']+df_ucr['DESC'].shift(-1)),df_ucr['DESC'])

    #now remove those empty lines
    df_ucr=df_ucr[df_ucr['UCR_358'].str.strip()!='']

    writer = pd.ExcelWriter(filename_final, engine='xlsxwriter')

    df_ucr.to_excel(writer, sheet_name='2011_UCR_358')
    writer.save()
    writer.close()




