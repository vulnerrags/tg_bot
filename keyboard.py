from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

menu_start = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="Подробная информация", callback_data='help'),
            InlineKeyboardButton(text="Отмена", callback_data='cancel')
        ]
    ], resize_keyboard=True
)

menu_help = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="1) Create material", callback_data='download'),
            InlineKeyboardButton(text='2) Transfer material', callback_data='transfer'),
            InlineKeyboardButton(text='3) Checking', callback_data='answer'),
        ],
        [
            InlineKeyboardButton(text='4) Logs materal', callback_data='log_vertica'),
            InlineKeyboardButton(text='5) Logs transfer', callback_data='log_transfer'),
            InlineKeyboardButton(text='6) Logs checking', callback_data='log_check')
        ],
        [
            InlineKeyboardButton(text='7) Check material', callback_data='check_fact'),
            InlineKeyboardButton(text='8) Check transfer', callback_data='check_transfer')
        ],
        [
            InlineKeyboardButton(text='9) Clear logs', callback_data='clear_logs'),
            InlineKeyboardButton(text='10) Kill process', callback_data='kill')
        ],
        [
            InlineKeyboardButton(text='Other', callback_data='logistics')
        ],
        [
            InlineKeyboardButton(text='Cancel', callback_data='cancel')
        ],
    ], resize_keyboard=True
)

menu_clear = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="Clear logs material", callback_data='clear_log_vertica')
        ],
        [
            InlineKeyboardButton(text="Clear logs transfer", callback_data='clear_log_transfer')
        ],
        [
            InlineKeyboardButton(text="Clear logs checking", callback_data='clear_log_check')
        ],
        [
            InlineKeyboardButton(text='Cancel', callback_data='cancel')
        ]
    ], resize_keyboard=True
)

menu_download = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="Ok", callback_data='ok'),
            InlineKeyboardButton(text="Cancel", callback_data='cancel')
        ]
    ], resize_keyboard=True
)

menu_date = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="Try again", callback_data='ok'),
            InlineKeyboardButton(text="Cancel", callback_data='cancel')
        ]
    ], resize_keyboard=True
)

menu_ok = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="Ок", callback_data='ok')
        ]
    ], resize_keyboard=True
)

menu_auto = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="Auto", callback_data='auto'),
            InlineKeyboardButton(text="Manually", callback_data='manual'),
        ],
        [
            InlineKeyboardButton(text='Cancel', callback_data='cancel')
        ]
    ], resize_keyboard=True
)

menu_logist_main = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="1) Make Storage", callback_data='download_storage_logist'),
            InlineKeyboardButton(text="2) Make Main", callback_data='download_logist')
        ],
        [
            InlineKeyboardButton(text="3) Logs Storage", callback_data='download_storage_lost_logist'),
            InlineKeyboardButton(text="4) Logs Transfer", callback_data='download_logs_logist')
        ],
        [
            InlineKeyboardButton(text="5) Clear logs", callback_data='clear_logs_logist'),
            InlineKeyboardButton(text="6) Kill process", callback_data='kill_process_logist')
        ],
        [
            InlineKeyboardButton(text='Cancel', callback_data='cancel')
        ]
    ], resize_keyboard=True
)
