from settings import settings
from utils import utils
from services import stock_data_ex

print(utils.read_from(settings.PATHS['INIT_PATH'], False))

# begin
try:
    stock_data_ex.main()
except:
    print('Exception running main script!')
