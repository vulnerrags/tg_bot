# коммент
from constant import *

logging.basicConfig(level=logging.INFO)


def is_secure(message: types.Message):
    user_id = str(message.from_user.id)
    return user_id in ALLOWED_USERS_ID


@dp.message_handler(state='*', commands='cancel')
@dp.message_handler(Text(equals='cancel', ignore_case=True), state='*')
async def cmd_cancel(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        return

    logging.info('Отменяем шаг %r', current_state)
    await state.finish()
    await message.reply('Ввод сброшен.', reply_markup=menu_ok)
    await Form.start_help_state.set()


@dp.message_handler(commands='start')
async def cmd_start(message: types.Message):
    if not is_secure(message):
        return
    msg = text('Привет!',
               'Это бот по сборке и выгрузке materials.',
               bold('Выбери одну из следующих команд:'),
               sep='\n')
    await Form.start_help_state.set()
    await message.answer(text=msg, reply_markup=menu_start, parse_mode=ParseMode.MARKDOWN)


@dp.message_handler(commands=['help'])
@dp.callback_query_handler(text=["help", "ok"],
                           state=Form.start_help_state)
async def cmd_help(message: types.Message, state: FSMContext):
    if not is_secure(message):
        return
    msg = text(bold('Выбери одно из действий:\n'),
               '1) Create material (create and download materials on Vertica)',
               '2) Transfer material (transfer materials from Vertica to Clickhouse)',
               '3) Checking (Olap-checking)',
               '4) Logs Materials (file with making logs)',
               '5) Logs Transfer (file with transfering logs)',
               '6) Logs Checking (file with checking logs)',
               '7) Check materials (check creating status)',
               '8) Check transfer (check transfer status)',
               '9) Clear logs (clear file with logs)',
               '10) Kill process (killing present process)\n',
               '- Logistics\n',
               '- Cancel',
               sep='\n')
    await bot.send_message(message.from_user.id, msg, parse_mode=ParseMode.MARKDOWN, reply_markup=menu_help)
    await state.finish()


# ----------------------начало блока выгрузки факта---------------------------------

@dp.callback_query_handler(text="download")
async def cmd_download(call: CallbackQuery):
    if async_download not in (False, True) and async_download.poll() is None:
        await Form.start_help_state.set()
        return await call.message.answer('Подожди, пожалуйста, я еще не закончил предыдущую сборку факта.',
                                         reply_markup=menu_ok)
    if async_transfer not in (False, True) and async_transfer.poll() is None:
        await Form.start_help_state.set()
        return await call.message.answer('Я пока что занят переливкой собранного факта на Clickhouse :)',
                                         reply_markup=menu_ok)
    await call.message.answer('Ты перешел в режим выгрузки факта!',
                              parse_mode=ParseMode.MARKDOWN, reply_markup=menu_download)
    await Form.start_state.set()


# проверка 1
@dp.callback_query_handler(text="cancel", state=Form.start_state)
async def cmd_cancel_button_1(call: CallbackQuery):
    global check_dates
    global check_period
    await call.message.answer('Отменено.',
                              parse_mode=ParseMode.MARKDOWN, reply_markup=menu_ok)
    check_dates = True
    check_period = True
    await Form.start_help_state.set()


@dp.message_handler(state=Form.start_state)
@dp.callback_query_handler(text='ok', state=Form.start_state)
async def cmd_date_start_input(message: types.Message):
    await bot.send_message(message.from_user.id, 'Введи дату начала выгрузки в формате YYYY-MM-DD:')
    await Form.date_start_state.set()


# проверка на отмену 2
@dp.callback_query_handler(text="cancel", state=Form.date_start_state)
async def cmd_cancel_button_2(call: CallbackQuery):
    global check_dates
    global check_period
    await call.message.answer('Отменено.',
                              parse_mode=ParseMode.MARKDOWN, reply_markup=menu_ok)
    check_dates = True
    check_period = True
    await Form.start_help_state.set()


@dp.callback_query_handler(state=Form.date_start_state)
@dp.message_handler(state=Form.date_start_state)
async def cmd_date_start_check(message: types.Message, state: FSMContext):
    global check_dates

    if check_dates and not re.search(r'\d\d\d\d-\d\d-\d\d', message.text):
        await Form.start_state.set()
        return await message.reply('Неверный формат.\nПравильный формат: YYYY-MM-DD.',
                                   reply_markup=menu_date)
    elif check_dates and (message.text < '2018-01-01' or message.text > datetime.datetime.now().strftime("%Y-%m-%d")):
        await Form.start_state.set()
        return await message.reply('Неверный формат.\nДата начала выгрузки не должна быть меньше 2018-01-01.',
                                   reply_markup=menu_date)
    if check_dates:
        async with state.proxy() as dates:
            dates['date_start'] = message.text

    await Form.date_end_state.set()
    answer_text = text('Введи дату конца выгрузки в формате YYYY-MM-DD:\n',
                       bold('Внимание: последний месяц не будет входить в выгрузку'),
                       sep='\n')
    await bot.send_message(message.from_user.id, answer_text, parse_mode=ParseMode.MARKDOWN)


# проверка 3
@dp.callback_query_handler(text="cancel", state=Form.date_end_state)
async def cmd_cancel_button_3(call: CallbackQuery):
    global check_dates
    global check_period
    await call.message.answer('Отменено.',
                              parse_mode=ParseMode.MARKDOWN, reply_markup=menu_ok)
    check_dates = True
    check_period = True
    await Form.start_help_state.set()


@dp.callback_query_handler(state=Form.date_end_state)
@dp.message_handler(state=Form.date_end_state)
async def cmd_date_end_check(message: types.Message, state: FSMContext):
    global check_period
    global check_dates
    global check_transfer
    global async_download
    global id_user_message

    if check_period and not re.search(r'\d\d\d\d-\d\d-\d\d', message.text):
        await Form.date_start_state.set()
        check_dates = False
        return await message.reply('Неверный формат.\nПравильный формат: YYYY-MM-DD.',
                                   reply_markup=menu_date)
    elif check_period and (message.text < '2018-01-01' or message.text > datetime.datetime.now().strftime("%Y-%m-%d")):
        await Form.date_start_state.set()
        check_dates = False
        return await message.reply(f'Неверный формат.\nДата конца выгрузки не должна быть больше сегодняшнего дня.\n'
                                   f'Сегодняшняя дата: {datetime.datetime.now().strftime("%Y-%m-%d")}',
                                   reply_markup=menu_date)
    if check_period:
        async with state.proxy() as dates:
            dates['date_end'] = message.text

    await bot.send_message(message.from_user.id, 'Я начал собирать и выгружать факт. Наберись терпения :)\n'
                                                 'Я сообщу, когда факт будет готов!',
                           reply_markup=menu_ok)
    id_user_message = message.from_user.id
    async_download = subprocess.Popen(
        ['/home/masmirnov/tg_bot/bot_env/bin/python3.7',
         'download.py', dates.get('date_start'), dates.get('date_end'), str(id_user_message)])
    await Form.start_help_state.set()
    check_transfer = True
# ----------------------конец блока выгрузки факта---------------------------------


# ----------------------начало блока переливки факта---------------------------------

@dp.callback_query_handler(text='transfer')
async def cmd_transfer(message: types.Message):
    global check_transfer
    global async_transfer
    global id_user_message
    if async_transfer not in (False, True) and async_transfer.poll() is None:
        await Form.start_help_state.set()
        return await bot.send_message(message.from_user.id, 'Подожди, пожалуйста, я еще не закончил переливать'
                                                            ' предыдущий факт на Clickhouse.', reply_markup=menu_ok)
    id_user_message = message.from_user.id
    if async_download not in (False, True) and async_download.poll() is None:
        await Form.start_help_state.set()
        return await bot.send_message(message.from_user.id, 'Пожалуйста, подожди, я занят сборкой и выгрузкой факта!',
                                      reply_markup=menu_ok)
    if not check_transfer:
        msg = text('Внимание!\n',
                   'Ты собираешься перелить старые данные.',
                   'Уверен?',
                   sep='\n')
        await bot.send_message(message.from_user.id, msg, reply_markup=menu_download,
                               parse_mode=ParseMode.MARKDOWN)
        return await Form.transfer_state_start.set()

    await bot.send_message(message.from_user.id, 'Ты перешел в режим переливки данных на Clickhouse!',
                           reply_markup=menu_download)
    await Form.transfer_state_start.set()


@dp.callback_query_handler(text='cancel', state=Form.transfer_state_start)
async def cmd_cancel_button_5(call: CallbackQuery):
    await call.message.answer('Отменено.',
                              parse_mode=ParseMode.MARKDOWN, reply_markup=menu_ok)
    await Form.start_help_state.set()


@dp.callback_query_handler(text='ok', state=Form.transfer_state_start)
async def cmd_ok_1(message: types.Message):
    global check_transfer
    global async_transfer
    global id_user_message
    await bot.send_message(message.from_user.id, 'Выбери режим ввода названия таблицы на Clickhouse:\n\n'
                                                 '- Автоматически (название таблицы сгенерируется автоматически и'
                                                 ' начнется переливка факта)\n'
                                                 '- Вручную (тебе нужно будет самостоятельно ввести название таблицы)',
                           reply_markup=menu_auto)
    await Form.transfer_state.set()


@dp.callback_query_handler(text='cancel', state=Form.transfer_state)
async def cmd_cancel_button_5(call: CallbackQuery):
    await call.message.answer('Отменено.',
                              parse_mode=ParseMode.MARKDOWN, reply_markup=menu_ok)
    await Form.start_help_state.set()


@dp.callback_query_handler(text='auto', state=Form.transfer_state)
async def cmd_auto(message: types.Message):
    global check_transfer
    global async_transfer
    global id_user_message

    int_time_now = int(datetime.datetime.now().strftime("%H%M"))
    int_date_now = int(datetime.datetime.now().strftime("%Y%m%d"))
    table_name = f'vertica_fact.Calc3_MP_{int_date_now}_{int_time_now}'

    await bot.send_message(message.from_user.id, f'Хорошо! Полное название таблицы:\n{table_name}, находится на 1м железном шарде. \n\n'
                                                 f'Я начал переливать данные с Vertica на Clickhouse в эту таблицу!',
                           reply_markup=menu_ok)
    id_user_message = message.from_user.id
    async_transfer = subprocess.Popen(
        ['/home/masmirnov/tg_bot/bot_env/bin/python3.7',
         'transfer.py', str(id_user_message), 'auto', int_time_now, int_date_now])
    await Form.start_help_state.set()
    check_transfer = False


@dp.callback_query_handler(text='manual', state=Form.transfer_state)
async def cmd_manual(message: types.Message):
    await bot.send_message(message.from_user.id, 'Хорошо! Пожалуйста, введи название таблицы '
                                                 '(обязательно латинскими буквами), '
                                                 'после этого я начну переливать факт!\n\n'
                                                 'Пример:\ndb.check_table')
    await Form.transfer_manual_state.set()


@dp.callback_query_handler(text='cancel', state=Form.transfer_manual_state)
async def cmd_cancel_button_5(call: CallbackQuery):
    await call.message.answer('Отменено.',
                              parse_mode=ParseMode.MARKDOWN, reply_markup=menu_ok)
    await Form.start_help_state.set()


@dp.message_handler(state=Form.transfer_manual_state)
async def cmd_manual_txt(message: types.Message):
    global check_transfer
    global async_transfer
    global id_user_message
    await bot.send_message(message.from_user.id, 'Супер! Я начал переливать факт на Clickhouse!\n'
                                                 'Сообщу, когда закончу :)', reply_markup=menu_ok)
    id_user_message = message.from_user.id
    async_transfer = subprocess.Popen(
        ['/home/masmirnov/tg_bot/bot_env/bin/python3.7',
         'transfer.py', str(id_user_message), str(message.text)])
    await Form.start_help_state.set()
    check_transfer = False
# ----------------------конец блока переливки факта---------------------------------


# ----------------------начало проверки переливок ----------------------------------
@dp.callback_query_handler(text=["check_fact"])
async def cmd_async_check_fact(message: types.Message):
    global async_download
    await Form.start_help_state.set()
    if not async_download:
        return await bot.send_message(message.from_user.id, 'Я еще ничего не собирал и не выгружал :)',
                                      reply_markup=menu_ok)
    if async_download.poll() is None:
        return await bot.send_message(message.from_user.id, 'Подожди, пожалуйста, пока собираю и выгружаю факт :)',
                                      reply_markup=menu_ok)
    return await bot.send_message(message.from_user.id, 'Уже все сделал!', reply_markup=menu_ok)


@dp.callback_query_handler(text=["check_transfer"])
async def cmd_async_check_transfer(message: types.Message):
    global async_transfer
    await Form.start_help_state.set()
    if not async_transfer:
        return await bot.send_message(message.from_user.id, 'Я еще ничего не переливал :)',
                                      reply_markup=menu_ok)
    if async_transfer.poll() is None:
        return await bot.send_message(message.from_user.id, 'Подожди, пожалуйста, пока переливаю факт на Clickhouse :)',
                                      reply_markup=menu_ok)
    return await bot.send_message(message.from_user.id, 'Уже все сделал!', reply_markup=menu_ok)
# ----------------------конец проверки переливок ---------------------------------------

# ---------------------- начало очистки файлов  ----------------------------------------
@dp.callback_query_handler(text="clear_logs")
async def cmd_clear_logs(message: types.Message):
    await Form.clear_logs.set()
    msg = text('Ты перешел в режим очистка файлов.\n',
               'Выбери, какой файл ты хочешь почистить.',
               sep='\n')
    await bot.send_message(message.from_user.id, msg, parse_mode=ParseMode.MARKDOWN,
                           reply_markup=menu_clear)


@dp.callback_query_handler(text='cancel', state=Form.clear_logs)
async def cmd_cancel_button_10(call: CallbackQuery):
    await call.message.answer('Отменено.',
                              parse_mode=ParseMode.MARKDOWN, reply_markup=menu_ok)
    await Form.start_help_state.set()


@dp.callback_query_handler(text="clear_log_vertica", state=Form.clear_logs)
async def cmd_clear_log_vertica(message: types.Message):
    await Form.start_help_state.set()
    file_way = os.path.join(".", f"fact_log_vertica_{str(message.from_user.id)}.txt")
    with open(file_way, 'w+') as file:
        time = f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
        file.write(f'{time}: Файл был очищен.\n')
        file.write('------------------------------------------------------------------------------------------------\n')
    await bot.send_message(message.from_user.id, 'Txt-файл с логами выполнения запросов на Vertica успешно почищен.',
                           parse_mode=ParseMode.MARKDOWN, reply_markup=menu_ok)


@dp.callback_query_handler(text="clear_log_check", state=Form.clear_logs)
async def cmd_clear_log_check(message: types.Message):
    await Form.start_help_state.set()
    file_way = os.path.join(".", f"fact_log_check_{str(message.from_user.id)}.txt")
    with open(file_way, 'w+') as file:
        time = f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
        file.write(f'{time}: Файл был очищен.\n')
        file.write('------------------------------------------------------------------------------------------------\n')
    await bot.send_message(message.from_user.id, 'Txt-файл с логами выполнения сверок с OLAP-кубами успешно почищен.',
                           parse_mode=ParseMode.MARKDOWN, reply_markup=menu_ok)


@dp.callback_query_handler(text="clear_log_transfer", state=Form.clear_logs)
async def cmd_clear_log_transfer(message: types.Message):
    await Form.start_help_state.set()
    file_way = os.path.join(".", f"fact_log_transfer_{str(message.from_user.id)}.txt")
    with open(file_way, 'w+') as file:
        time = f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
        file.write(f'{time}: Файл был очищен.\n')
        file.write('------------------------------------------------------------------------------------------------\n')
    await bot.send_message(message.from_user.id, 'Txt-файл с логами переливки факта с Vertica на Clickhouse успешно почищен.',
                           parse_mode=ParseMode.MARKDOWN, reply_markup=menu_ok)
# ---------------------- конец очистки файлов -----------------------------------------

# ----------------------начало xslx остановки процесса ---------------------------------
@dp.callback_query_handler(text='answer')
async def cmd_answer(message: types.Message):
    if async_check not in (False, True) and async_check.poll() is None:
        await Form.start_help_state.set()
        return await bot.send_message(message.from_user.id, 'Подожди, пожалуйста, я еще не закончил предыдущщие сверки.',
                                         reply_markup=menu_ok)
    await bot.send_message(message.from_user.id, 'Введи месяц, который хочешь сверить, в формате YYYYMM.\n\nПример: 202104',
                              parse_mode=ParseMode.MARKDOWN)
    return await Form.answer_state_month.set()


@dp.callback_query_handler(text='cancel', state=Form.answer_state_month)
async def cmd_cancel_button_5(call: CallbackQuery):
    await call.message.answer('Отменено.',
                              parse_mode=ParseMode.MARKDOWN, reply_markup=menu_ok)
    await Form.start_help_state.set()


@dp.callback_query_handler(state=Form.answer_state_month)
@dp.message_handler(state=Form.answer_state_month)
async def cmd_answer_month(message: types.Message):
    global async_check
    if not re.search(r'\d\d\d\d\d\d', message.text):
        await Form.start_help_state.set()
        return await message.reply('Неверный формат.\nПравильный формат: YYYYMM',
                                   reply_markup=menu_date)
    elif int(message.text) < 201801:
        await Form.start_help_state.set()
        return await message.reply('Неверный формат.\nМесяц не должен быть меньше 201801 (1 января 2018 года).',
                                   reply_markup=menu_date)
    elif int(message.text) > int(datetime.datetime.now().strftime("%Y%m")):
        await Form.start_help_state.set()
        return await message.reply(f'Неверный формат.\nМесяц не может быть больше сегодняшнего.\n'
                                   f'Сегодняшний месяц: {datetime.datetime.now().strftime("%Y%m")}',
                                   reply_markup=menu_date)
    await bot.send_message(message.from_user.id, 'Я начал делать сверки. Пожалуйста, наберись терпения :)\n\n'
                                                 'Я сообщу, когда сверки будут готовы и пришлю excel-файл с ними!',
                           reply_markup=menu_ok)
    async_check = subprocess.Popen(['/home/masmirnov/tg_bot/bot_env/bin/python3.7', 'check.py',\
                                    message.text, str(message.from_user.id)])
    await Form.start_help_state.set()
# ----------------------конец xslx остановки процесса ---------------------------------

# ---------------------- начало выгрузка логов ----------------------------------------
@dp.callback_query_handler(text='log_vertica')
async def cmd_log1(message: types.Message):
    await Form.start_help_state.set()
    await bot.send_chat_action(message.from_user.id, ChatActions.UPLOAD_DOCUMENT)
    if os.path.isfile(f'fact_log_vertica_{str(message.from_user.id)}.txt'):
        return await bot.send_document(message.from_user.id, document=open(f'fact_log_vertica_{str(message.from_user.id)}.txt', 'rb'),
                            caption='fact_log_vertica', reply_markup=menu_ok)
    else:
        with open(f'fact_log_vertica_{str(message.from_user.id)}.txt', 'w+') as file:
            time = f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
            file.write(f'{time}: Файл был очищен.\n')
            file.write('------------------------------------------------------------------------------------------------\n')
        return await bot.send_document(message.from_user.id, document=open(f'fact_log_vertica_{str(message.from_user.id)}.txt', 'rb'),
                            caption='fact_log_vertica', reply_markup=menu_ok)



@dp.callback_query_handler(text='log_check')
async def cmd_log2(message: types.Message):
    await Form.start_help_state.set()
    await bot.send_chat_action(message.from_user.id, ChatActions.UPLOAD_DOCUMENT)
    if os.path.isfile(f'fact_log_check_{str(message.from_user.id)}.txt'):
        return await bot.send_document(message.from_user.id, document=open(f'fact_log_check_{str(message.from_user.id)}.txt', 'rb'),
                            caption='fact_log_check', reply_markup=menu_ok)
    else:
        with open(f'fact_log_check_{str(message.from_user.id)}.txt', 'w+') as file:
            time = f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
            file.write(f'{time}: Файл был очищен.\n')
            file.write('------------------------------------------------------------------------------------------------\n')
        return await bot.send_document(message.from_user.id, document=open(f'fact_log_check_{str(message.from_user.id)}.txt', 'rb'),
                            caption='fact_log_check', reply_markup=menu_ok)


@dp.callback_query_handler(text='log_transfer')
async def cmd_log3(message: types.Message):
    await Form.start_help_state.set()
    await bot.send_chat_action(message.from_user.id, ChatActions.UPLOAD_DOCUMENT)
    if os.path.isfile(f'fact_log_transfer_{str(message.from_user.id)}.txt'):
        return await bot.send_document(message.from_user.id, document=open(f'fact_log_transfer_{str(message.from_user.id)}.txt', 'rb'),
                            caption='fact_log_transfer', reply_markup=menu_ok)
    else:
        with open(f'fact_log_transfer_{str(message.from_user.id)}.txt', 'w+') as file:
            time = f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
            file.write(f'{time}: Файл был очищен.\n')
            file.write('------------------------------------------------------------------------------------------------\n')
        return await bot.send_document(message.from_user.id, document=open(f'fact_log_transfer_{str(message.from_user.id)}.txt', 'rb'),
                            caption='fact_log_transfer', reply_markup=menu_ok)
# ---------------------- конец выгрузка логов ----------------------------------------

# ----------------------начала блока остановки процесса -------------------------------
@dp.callback_query_handler(text="kill")
async def cmd_kill(message: types.Message):
    await Form.kill_state.set()
    msg = text('Ты перешел в режим экстренной отмены процесса!\n',
               'Уверен, что хочешь отменить процесс?', '- Ок', '- Отмена',
               sep='\n')
    await bot.send_message(message.from_user.id, msg, parse_mode=ParseMode.MARKDOWN,
                           reply_markup=menu_download, )


@dp.callback_query_handler(text='cancel', state=Form.kill_state)
async def cmd_cancel_button_7(call: CallbackQuery):
    await call.message.answer('Отменено.',
                              parse_mode=ParseMode.MARKDOWN, reply_markup=menu_ok)
    await Form.start_help_state.set()


@dp.callback_query_handler(text="ok", state=Form.kill_state)
async def cmd_kill(message: types.Message):
    global async_download
    global async_transfer
    global async_check
    if async_download not in (False, True) and async_download.poll() is None:
        async_download.terminate()
        await Form.start_help_state.set()
        async_download = False
        return await bot.send_message(message.from_user.id, 'Процесс сборки и выгрузки факта отменен.',
                                      reply_markup=menu_ok)
    if async_transfer not in (False, True) and async_transfer.poll() is None:
        async_transfer.terminate()
        await Form.start_help_state.set()
        async_transfer = False
        return await bot.send_message(message.from_user.id, 'Процесс переливки на Clickhouse отменен.',
                                      reply_markup=menu_ok)
    if async_check not in (False, True) and async_check.poll() is None:
        async_check.terminate()
        await Form.start_help_state.set()
        async_check = False
        return await bot.send_message(message.from_user.id, 'Процесс проверок отменен.',
                                      reply_markup=menu_ok)
    await bot.send_message(message.from_user.id, 'Ни один процесс пока не запущен!', reply_markup=menu_ok)
    return await Form.start_help_state.set()
# ----------------------конец блока остановки процесса ---------------------------------


# ---------------------------------------------------------------
@dp.callback_query_handler(text=["logistics", "ok_logist", "cancel_logist"])
async def cmd_download_logist(call: CallbackQuery):
    msg = text('',
                      'Пожалуйста, выбери одной из действий:\n\n',
                      '1) ',
                      '2) ',
                      '3) ',
                      '4) ',
                      '5) ',
                      '6) ',
                      '- Отмена', sep='\n')
    await call.message.answer(msg, reply_markup=menu_logist_main)
    await Form.storage1_state_logistics.set()


@dp.callback_query_handler(text='cancel', state=Form.storage1_state_logistics)
async def cmd_cancel(call: CallbackQuery):
    await call.message.answer('Отменено.', reply_markup=menu_ok)
    await Form.start_help_state.set()

# ---------------------------------- выгрузка DP_team STORAGE table начало --------------------
@dp.callback_query_handler(text="download_storage_logist", state=Form.storage1_state_logistics)
async def cmd_download_logist(call: CallbackQuery):
    if async_download_storage not in (False, True) and async_download_storage.poll() is None:
        await Form.start_help_state.set()
        return await call.message.answer('Подожди, пожалуйста, я еще не закончил предыдущую сборку таблицы DP_team.Calc3_Logistics_Storage.',
                                         reply_markup=menu_ok)
    await call.message.answer('Ты перешел в режим сборки таблицы DP_team.Calc3_Logistics_Storage!', reply_markup=menu_download)
    await Form.storage2_state_logistics.set()


# проверка 1
@dp.callback_query_handler(text="cancel", state=Form.storage2_state_logistics)
async def cmd_cancel_button_1_logist(call: CallbackQuery):
    await call.message.answer('Отменено.', reply_markup=menu_ok)
    await Form.start_help_state.set()


@dp.message_handler(state=Form.storage2_state_logistics)
@dp.callback_query_handler(text='ok', state=Form.storage2_state_logistics)
async def cmd_date_start_input(message: types.Message):
    await bot.send_message(message.from_user.id, 'Введи месяц, до которого ты хочешь собрать остатки.\n\nФормат: YYYYMM.\n'
                                                 'Внимание: месяц, который ты введешь, не будет включен в выгрузку!')
    await Form.storage3_state_logistics.set()


# проверка на отмену 2
@dp.callback_query_handler(text="cancel", state=Form.storage3_state_logistics)
async def cmd_cancel_button_2_logist(call: CallbackQuery):

    await call.message.answer('Отменено.',
                              parse_mode=ParseMode.MARKDOWN, reply_markup=menu_ok)
    await Form.start_help_state.set()


@dp.callback_query_handler(state=Form.storage3_state_logistics)
@dp.message_handler(state=Form.storage3_state_logistics)
async def cmd_date_start_check(message: types.Message):
    global async_download_storage
    if not re.search(r'\d\d\d\d\d\d', message.text):
        await Form.start_help_state.set()
        return await message.reply('Неверный формат.\nПравильный формат: YYYYMM',
                                   reply_markup=menu_date)
    elif int(message.text) < 201801:
        await Form.start_help_state.set()
        return await message.reply('Неверный формат.\nМесяц не должен быть меньше 201801 (1 января 2018 года).',
                                   reply_markup=menu_date)
    elif int(message.text) > int(datetime.datetime.now().strftime("%Y%m")):
        await Form.start_help_state.set()
        return await message.reply(f'Неверный формат.\nМесяц не может быть больше сегодняшнего.\n'
                                   f'Сегодняшний месяц: {datetime.datetime.now().strftime("%Y%m")}',
                                   reply_markup=menu_date)
    await bot.send_message(message.from_user.id, f'Я начал собирать таблицу db.vertica_smth. Пожалуйста, наберись терпения :)\n\n'
                                                 f'Я сообщу, когда таблица будет собрана.\nТаблица будет собрана в промежутке [2019-01-01; '
                                                 f'{message.text[:4]}-{message.text[4:6]}-01).',
                           reply_markup=menu_ok)
    async_download_storage = subprocess.Popen(['/home/masmirnov/tg_bot/bot_env/bin/python3.7', 'storage.py',\
                                    message.text, str(message.from_user.id)])
    await Form.start_help_state.set()


@dp.callback_query_handler(text='download_storage_lost_logist')
async def cmd_log2(message: types.Message):
    await Form.start_help_state.set()
    await bot.send_chat_action(message.from_user.id, ChatActions.UPLOAD_DOCUMENT)
    if os.path.isfile(f'fact_log_storage_{str(message.from_user.id)}.txt'):
        return await bot.send_document(message.from_user.id, document=open(f'fact_log_storage_{str(message.from_user.id)}.txt', 'rb'),
                            caption='fact_log_storage', reply_markup=menu_ok)
    else:
        with open(f'fact_log_storage_{str(message.from_user.id)}.txt', 'w+') as file:
            time = f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
            file.write(f'{time}: Файл был очищен.\n')
            file.write('------------------------------------------------------------------------------------------------\n')
        return await bot.send_document(message.from_user.id, document=open(f'fact_log_storage_{str(message.from_user.id)}.txt', 'rb'),
                            caption='fact_log_storage', reply_markup=menu_ok)


@dp.callback_query_handler(text="kill_process_logist")
async def cmd_kill(message: types.Message):
    await Form.kill_state_logist.set()
    msg = text('Ты перешел в режим экстренной отмены процесса!\n',
               'Уверен, что хочешь отменить процесс?', '- Ок', '- Отмена',
               sep='\n')
    await bot.send_message(message.from_user.id, msg, parse_mode=ParseMode.MARKDOWN,
                           reply_markup=menu_download, )


@dp.callback_query_handler(text='cancel', state=Form.kill_state_logist)
async def cmd_cancel_button_7(call: CallbackQuery):
    await call.message.answer('Отменено.',
                              parse_mode=ParseMode.MARKDOWN, reply_markup=menu_ok)
    await Form.start_help_state.set()


@dp.callback_query_handler(text="ok", state=Form.kill_state_logist)
async def cmd_kill(message: types.Message):
    global async_download_storage
    if async_download_storage not in (False, True) and async_download_storage.poll() is None:
        async_download_storage.terminate()
        await Form.start_help_state.set()
        async_download_storage = False
        return await bot.send_message(message.from_user.id, 'Процесс сборки и выгрузки витрины остатков.',
                                      reply_markup=menu_ok)
    await bot.send_message(message.from_user.id, 'Ни один процесс логистики пока не запущен!', reply_markup=menu_ok)
    return await Form.start_help_state.set()


# ---------------------------------------------------------------
@dp.message_handler(content_types=ContentType.ANY)
async def cmd_unknown_message(msg: types.Message):
    if not is_secure(msg):
        return
    await msg.reply('Я не знаю, что с этим делать =(\n'
                    'У меня есть команды:\n'
                    '- /start\n'
                    '- /help')
# ---------------------------------------------------------------------------------------
if __name__ == '__main__':

    with open('passwords.json') as jsonFile:
        passwords = json.load(jsonFile)
        jsonFile.close()
    executor.start_polling(dp, skip_updates=True)
