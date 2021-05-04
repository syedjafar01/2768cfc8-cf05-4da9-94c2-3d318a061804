import razor.flow as rf
import numpy as np

from rztdl.backends import backend

backend.tensorflow()

from rztdl.modules.model import Model
from rztdl.modules.layers.inputlayer import InputLayer
from rztdl.modules.layers.dense import Dense
from rztdl.modules.layers.batch_normalization import BatchNormalization
from rztdl.modules.layers.convolution import Conv2D
from rztdl.modules.layers.pooling import MaxPool2D
from rztdl.modules.layers.dropout import Dropout
from rztdl.modules.losses import CategoricalCrossentropy
from rztdl.modules.optimizers import Adam
from rztdl.modules.operators import Argmax
from rztdl.modules.metrics import Accuracy
from rztdl.modules.layers.flatten import Flatten
from rztdl.backends.utils.string_constants import *
from rztdl.modules.helpers import Relu, Softmax

import razor.flow as rf



@rf.block
class LoadImagesAndTrain:
    __publish__ = True
    __label__ = 'LoadAndTrain'    
    
    path: str
    no_of_examples: int
    
    def run(self):
        self.logger.debug('Running Load Images')
        
        npy = np.load(self.path)
        
        def generator():
            for image, label in zip(npy['images'][:self.no_of_examples], npy['labels'][:self.no_of_examples]):
                image = image.astype('float32') / 255.0
                label = np.eye(10)[[label]].reshape(-1)
                image = np.reshape(image, (image.shape[0], image.shape[1], 1))

                yield {"images":image, "labels":label}
                
        labels = InputLayer(shape=[10], name='labels')
        images = InputLayer(shape=(28, 28, 1), name='images')

        batchnorm1 = BatchNormalization()(input=images)

        conv1 = Conv2D(filters=64, kernel_size=(4, 4), padding=Padding.SAME, activation=Relu(), name='conv1')(input=batchnorm1)
        maxpool1 = MaxPool2D(pool_size=(2, 2), name='maxpool1')(input=conv1)
        dropout1 = Dropout(rate=0.1, name='dropout1')(input=maxpool1)

        conv2 = Conv2D(filters=64, kernel_size=(4, 4), activation=Relu(), name='conv2')(input=dropout1)
        maxpool2 = MaxPool2D(pool_size=(2, 2), name='maxpool2')(input=conv2)
        dropout2 = Dropout(rate=0.3, name='dropout2')(input=maxpool2)

        flatten = Flatten(name='flatten')(input=dropout2)

        dense1 = Dense(units=256, use_bias=True, activation=Relu(), name='dense1')(input=flatten)
        dropout4 = Dropout(rate=0.5, name='dropout4')(input=dense1)
        dense2 = Dense(units=64, use_bias=True, activation=Relu(), name='dense2')(input=dropout4)
        batchnorm2 = BatchNormalization()(input=dense2)
        dense3 = Dense(units=10, use_bias=True, activation=Softmax(), name='dense3')(input=batchnorm2)

        cce = CategoricalCrossentropy(name='cce')(labels=labels, predictions=dense3)
        adam1 = Adam(name='adam1')(input=cce)

        label = Argmax(name='argmax1')(input=labels)
        prediction = Argmax(name='argmax')(input=dense3)
        acc = Accuracy(name='acc')(predictions=prediction, labels=label)
        
        model = Model(outputs=[dense3])

        # fit method
        model.fit_generator(data=generator,
                            epochs=2,
                            batch_size=256,
                            optimizers=[adam1],
                            metrics=[acc, cce],
                            verbose=True,
                            lr=[0.001],
                            model_save_path='/home/aios/projectspace/model_saved_here')
        
            
        self.logger.debug('Completed Load Images')