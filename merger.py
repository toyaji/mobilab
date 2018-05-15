import pandas as pd
import numpy as np
from os import listdir, path
from glob import glob
import json
import re


class DataMerger(object):

    def __init__(self, dirpath):
        self.dirpath = dirpath
        self.meta = glob(dirpath + '/meta.txt')
        self.filelist = [f for f in listdir(dirpath) if f.endswith('.csv')]

        if len(self.meta) > 2:
            raise InputError("There is more than 2 meta files!!")
        elif len(self.meta) < 1:
            self.meta = None
        if len(self.filelist) < 1:
            raise InputError("There is no csv files. You should check it.")

    def meta_load_from_txt(self, file=None):
        """
        It load json meta data from txt file in the given directory
        """
        if file: assert self.meta[0]

        with open(self.meta[0]) as f:
            meta = json.loads(f.read())

        self.meta = meta

    @staticmethod
    def filename_parsing(file, layer=False):
        """
        Regex parsing from a given file name

        :param file: each CSV file
        :return: [date, Cl, Br, No, So, sample number]
        """
        regex = ['^20[\d]{2}-[0-1]\d-[0-3]\d \d{6}', '\d+[ ]?(?=NO3[\W])', '\d+[ ]?(?=Cl[\W])', '\d+[ ]?(?=Br[\W])',
                 '\d+[ ]?(?=SO4[\W])', '[VBHW][ ]?\d+ | (?<=Sample \D{2} )\d+ | (?<=Sample )\d+ | (?<=sam )\d+ | [ ]\d\d[ ]']

        result= [re.search(x, file).group(0) if re.search(x, file) else np.nan for x in regex]
        if layer: result[-1] = re.search('\d+', result[-1]).group(0)
        return result

    def metadata_parsing(self, th):
        meta = self.meta[str(th)]
        return meta['nitrate'], meta['bromide'], meta['sulfate'], meta['chloride']

    def main_merge(self, layer=False):

        if self.meta:
            self.meta_load_from_txt()

        total = []
        for f in self.filelist:
            [date, No, Cl, Br, So, sample_n] = self.filename_parsing(f, layer)

            print([date, Cl, Br, No, So, sample_n])

            df = pd.read_csv(path.join(self.dirpath, f), sep='\t', names=['time', 'value'])
            times = df.time.values
            value = df.value.values

            if self.meta:
                No, Br, So, Cl = self.metadata_parsing(int(sample_n))

            total.append([date, No, Br, So, Cl, times, value, sample_n, f])

        tf = pd.DataFrame(total, columns=['date', 'No', 'Br', 'So', 'Cl', 'times', 'values', 'th-sample', 'file_name'])

        return tf


class InputError(Exception):

    def __init__(self, message):
        self.message = message



if __name__ == '__main__':


    file_path = '/home/paul/Documents/mobilab_test_data/pynb/pickled/'
    files = listdir(file_path)

    concatlist = []
    for f in files:
        cl = DataMerger(path.join(file_path, f))
        df = cl.main_merge()
        df['directory'] = f
        concatlist.append(df)

        """
        # These lines for fixed option. e.i. Br: 500
        r['So'] = r['So'].apply(lambda x: 35 if pd.isnull(x) else x)
        r['No'] = r['No'].apply(lambda x: 96 if pd.isnull(x) else x)
        r['Br'] = 500
        """
    tf = pd.concat(concatlist)
    tf[['Br', 'So', 'No', 'Cl']] = tf[['Br', 'So', 'No', 'Cl']].astype('float', copy=False)
    tf.to_csv('./total_data.csv')
    tf.to_pickle('./total_data')

