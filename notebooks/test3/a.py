import razor.flow as rf

@rf.block
class Dataset_Block:
    __publish__ = True
    __label__ = "Dataset test 2"
    
    input_numbers: list = [1,2,3,4,5,5]
    a: int
    result: rf.Output[int] = rf.Output(label='Result')

    def run(self):
        self.logger.info(f'Input values: {self.input_numbers}')
        
        from razor.api import datasets
        ds = datasets('tr_dataset_3') # Engine uses versioned manifest json
        df = ds['test_file'].read_bytes()
        
        self.logger.info(f'Titanic df: {df}')