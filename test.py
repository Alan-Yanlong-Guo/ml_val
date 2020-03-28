# #%%
#
from data.build_xy import run_load_xy
from global_settings import TRAIN_YEAR, CROSS_YEAR, TEST_YEAR

print(TRAIN_YEAR)
run_load_xy(TRAIN_YEAR, 'tr')
run_load_xy(CROSS_YEAR, 'val')
run_load_xy(TEST_YEAR, 'ts')






