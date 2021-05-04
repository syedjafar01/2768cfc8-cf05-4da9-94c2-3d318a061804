import razor.flow as rf


@rf.block
class PrintRes:
    res_input: int

    def run(self):
        print(self.res_input)