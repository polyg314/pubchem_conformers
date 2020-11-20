import os
import pandas as pd
from .csvsort import csvsort

def load_annotations():
    col_names = ["CID1","LID1","CID2","LID2","ST","CT","Rxx","Rxy","Rxz","Ryx","Ryy","Ryz","Rzx","Rzy","Rzz","Tx","Ty","Tz"]
    file_names = os.path.join(data_folder,'00000001_00500000x00000001_00500000.csv.gz')
    CSV_FILES = os.path.join(data_folder,"CSV_FILES")
    dump_files = []
    interval_value = 1000000
    csv_ints = list(range(0,100000000,interval_value))
    try: 
        os.mkdir(CSV_FILES)
    except: 
        for file in os.scandir(CSV_FILES):
            os.remove(file.path)
        os.rmdir(CSV_FILES)
        os.mkdir(CSV_FILES)
    for x in csv_ints: 
        with open(CSV_FILES + "/" + str(x) + ".csv", "w") as my_empty_csv:
            pass
    pair_dict = {}
    chunksize = 10 ** 6
    smerge_counter = 0; # use merge counter to append file after file is created
    for chunk in pd.read_csv(file_name, names = col_names, usecols = ['CID1','CID2','ST','CT'], chunksize=chunksize, header = 0):
        new_entry = chunk.drop_duplicates()
        ## ST or CT above 95, AND ST < 101
        new_entry = new_entry[((new_entry["ST"] > 95) | (new_entry["CT"] > 95)) & (new_entry["ST"] < 101)]
        new_entry.drop(["ST","CT"], axis=1)
        rev_entry = new_entry.reindex(columns=['CID2','CID1'])
        for x in csv_ints: 
            temp_df = new_entry[(new_entry["CID1"] >= x) & (new_entry["CID1"] < (x + interval_value))]
            temp_df.to_csv(path_or_buf=CSV_FILES + "/" + str(x) + ".csv", index=False, mode='a',header=False)
            temp_df = rev_entry[(rev_entry["CID2"] >= x) & (rev_entry["CID2"] < (x + interval_value))]
            temp_df.to_csv(path_or_buf=CSV_FILES + "/" + str(x) + ".csv", index=False, mode='a',header=False)

    for file in os.scandir(CSV_FILES):
        try: 
            csvsort(file.path,[0],has_header=False)
        except: 
            pass
            # print(file.path)           
    chunksize = 10 ** 6
    dtypes = {'CID1': int, 'CID2': int, 'ST': int, 'CT': int}
    for file in os.scandir(CSV_FILES):
        try: 
            lastid = -1
            final_entry = False
            for chunk in pd.read_csv(file.path, names = ['CID1','CID2','ST','CT'], usecols = ['CID1','CID2'], chunksize=chunksize, header = None, dtype=dtypes):
                for index, row in chunk.iterrows():
                    print(row)
                    id1 = int(row['CID1'])
                    id2 = int(row['CID2'])
                    if(id1 == lastid):
                        final_entry["similar_conformers"].append(id2)
                    else:
                        if(final_entry):
    #                         yield(final_entry)
                            print(final_entry)
                        lastid = id1
                        final_entry = {
                            "_id": id1,
                            "similar_conformers": [id2]
                        }
            if(len(final_entry["similar_conformer"]) == 1):
    #                 yield(final_entry)
                print(final_entry)
        except: 
            print(file.path)