from config import *

check_dates = True
check_period = True
check_transfer = False
async_download = False
async_check = False
async_transfer = False
id_user_message = 0

async_download_storage = False

# States
class Form(StatesGroup):
    start_state = State()
    date_start_state = State()
    date_end_state = State()
    period_state = State()
    start_help_state = State()
    transfer_state = State()
    transfer_state_start = State()
    kill_state = State()
    transfer_auto_state = State()
    transfer_manual_state = State()
    answer_state_month = State()
    answer_state_go = State()
    clear_logs = State()

    storage1_state_logistics = State()
    storage2_state_logistics = State()
    storage3_state_logistics = State()
    kill_state_logist = State()


# Путь к таблице на вертике
vertica_table = f'db.check_table_vert'

# Получатели логов
recipients_for_log = ['feelingalive_da@mail.ru']

# Категории 1
category1_list = [i for i in range(1, 11)]
