#%%

from data.build_xy import build_xy_from_years
from global_settings import TRAIN_YEAR, CROSS_YEAR, TEST_YEAR

print(TRAIN_YEAR)
build_xy_from_years(TRAIN_YEAR, 'tr', save_file=True)
build_xy_from_years(CROSS_YEAR, 'val', save_file=True)
# build_xy_from_years(TEST_YEAR, 'ts', save_file=True)

#%%




