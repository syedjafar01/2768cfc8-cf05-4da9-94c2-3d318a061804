import razor.flow as rf


@rf.block
class SumNumbers:
    input_numbers: rf.SeriesInput[int]
    sum_res: rf.Output[int]

    def run(self):
        sum = 0
        for number in self.input_numbers:
            sum = sum + number

        self.sum_res.put(sum)
