import razor.flow as rf
import random


@rf.block
class RandomNoGen:
    random_number_list: rf.SeriesOutput[int]

    def run(self):
        for i in range(0,100):
            n = random.randint(0, 1000)
            self.random_number_list.put(n)
