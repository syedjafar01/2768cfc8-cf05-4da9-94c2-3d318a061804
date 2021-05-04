import razor.flow as rf
import pandas as pd
from PIL import Image
import numpy as np
import typing as t
import random



@rf.block
class Input_Block:
    __publish__ = True
    __label__ = 'Read_Images'

    def run(self):
        path = str
        data: rf.SeriesOutput[t.Any]
            
        df = pd.read_csv(path + '/Data/paths.csv')
        
        for i in range(len(df)):
            image = Image.open(path + df.loc[i, 'path'])

            image = image.resize((100, 100))
            image = np.asarray(image)
            image = image / 255.0
            label = [0, 1] if df.loc[i, 'label'] == 1 else [1, 0]

            self.data.put({"images": image, "labels": np.array(label)})