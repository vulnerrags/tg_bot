from constant import vertica_table, category1_list
from config import *
from utils import *
from constant import recipients_for_log

logging.basicConfig(level=logging.INFO)


def transfer_to_ch(chat_id):
    """
    Функция переливки факта с вертики на кликхаус

    Возвращаемое значение (NoneType) : None
    """
    if sys.argv[2] == 'auto':
        int_time_now = int(sys.argv[3])
        int_date_now = int(sys.argv[4])

        # Название таблицы на кликхаусе
        table_name = f'db.check_table_{int_date_now}_{int_time_now}'

        # Запрос создания таблицы на кликхаусе
        SQL_create_NT = f'''CREATE TABLE {table_name}
        (
            `Month`									Int64
            , `Category 1`							Int64
            , `SOME_METRIC`                         Int64
        )
        ENGINE = MergeTree()
        PARTITION BY (`Month`)
        ORDER BY (
                    `Category 1`
        )
        SETTINGS index_granularity = 8192;'''

        clickhouse_client.execute(SQL_create_NT)
    else:
        table_name = sys.argv[2]

    clickhouse_table = f'{table_name}'

    # Собираем данные о столбцах таблицы на вертике
    sql = f'select column_name, data_type from columns where table_name = \'{vertica_table.split(".")[1]}\''
    table_structure = get_df_from_vertica(sql, 'fact_log_transfer.txt', chat_id)
    try: # проверка на то, что вернулся pd.DataFrame
        table_structure.columns = ['name', 'type']
    except Exception:
        exc = traceback.format_exc()
        log_entry(
            f'Не получилось получить данные о столбцах таблицы на вертике.\nПолный текст ошибки:\n {exc}',
            'fact_log_transfer.txt', chat_id)
        send_files_to_email(filepath=f'fact_log_transfer_{str(chat_id)}.txt'
                            , txt=f'Не получилось получить данные о столбцах таблицы на вертике!\n'
                                  f'Полный текст ошибки в файле.'
                            , recipients=recipients_for_log)
        return False
    vertica_headers = table_structure.name.tolist()

    # Собираем данные о месяцах в таблице на вертике
    sql = f"""
        select distinct Month as dates 
        from {vertica_table}
        order by Month asc
    """
    df_dates = get_df_from_vertica(sql, 'fact_log_transfer.txt', chat_id)
    try: # проверка на то, что вернулся pd.DataFrame
        dates = df_dates.iloc[:, 0].tolist()
    except Exception:
        exc = traceback.format_exc()
        log_entry(
            f'Не получилось получить данные о месяцах в таблице на вертике.\nПолный текст ошибки:\n {exc}',
            'fact_log_transfer.txt', chat_id)
        send_files_to_email(filepath=f'fact_log_transfer_{str(chat_id)}.txt'
                            , txt=f'Не получилось получить данные о месяцах в таблице на вертике!\n'
                                  f'Полный текст ошибки в файле.'
                            , recipients=recipients_for_log)
        return False

    for i in tqdm.tqdm(range(0, len(dates))):
        if i >= len(dates):
            print(f'End of the dates.')
            break

        print(f'{datetime.datetime.now().time()} - Loading part {i} for date {dates[i]}')
        log_entry(f'Loading part {i} for date {dates[i]}', 'fact_log_transfer.txt', chat_id)

        for j in tqdm.tqdm(range(0, len(category1_list))):

            print(f'{datetime.datetime.now().time()} - Loading part {j} for categories. Category 1 ID: {category1_list[j]}')
            log_entry(f'Loading part {j} for categories. Category 1 ID: {category1_list[j]}', 'fact_log_transfer.txt', chat_id)

            sql = f"""
                select *
                from {vertica_table}
                where Month = '{dates[i]}'
                and "Category 1" = '{category1_list[j]}'
            """
            try_number = 1
            while try_number < 6:
                try:
                    vertica_conn = vertica_python.connect(**vertica_conn_info)
                    break
                except Exception:
                    exc = traceback.format_exc()
                    log_entry(f'На шаге переливки Category 1 ID: {category1_list[j]} не смог подключиться к Vertica.\n'
                              f'Засыпаю на 4 минуты и попробую заново.\n'
                              f'Полный текст ошибки:{exc}', 'fact_log_transfer.txt', chat_id)
                    try_number += 1
                    time.sleep(240)
            else:
                if try_number == 6:
                    log_entry(f'На шаге переливки Category 1 ID: {category1_list[j]} не смог подключиться к Vertica.\n'
                              f'После 5 попыток прекратил цикл.\n'
                              f'Полный текст ошибки:{exc}', 'fact_log_transfer.txt', chat_id)
                    send_files_to_email(filepath=f'fact_log_transfer_{str(chat_id)}.txt'
                                        , txt=f'Ошибка при переливке факта с Vertica на Clickhouse! Полный текст ошибки в файле.\n'
                                        , recipients=recipients_for_log)
                    return False

            try_number = 1
            while try_number < 6:
                try:
                    vertica_cursor = vertica_conn.cursor()
                    vertica_cursor.execute(sql)
                    break
                except Exception:
                    exc = traceback.format_exc()
                    log_entry(f'На шаге переливки Category 1 ID: {category1_list[j]} не смог выполнить запрос на Vertica.\n'
                              f'Засыпаю на 4 минуты и попробую заново.\n'
                              f'Полный текст ошибки:{exc}', 'fact_log_transfer.txt', chat_id)
                    try_number += 1
                    time.sleep(240)
            else:
                if try_number == 6:
                    log_entry(f'На шаге переливки Category 1 ID: {category1_list[j]} не смог выполнить запрос на Vertica.\n'
                              f'После 5 попыток прекратил цикл.\n'
                              f'Полный текст ошибки:{exc}', 'fact_log_transfer.txt', chat_id)
                    send_files_to_email(filepath=f'fact_log_transfer_{str(chat_id)}.txt'
                                        , txt=f'Ошибка при переливке факта с Vertica на Clickhouse! Полный текст ошибки в файле.\n'
                                        , recipients=recipients_for_log)
                    vertica_conn.close()
                    return False

            # Переливка таблицы с вертики на кликхаус
            vertica_data = vertica_cursor.fetchmany(100000)
            while vertica_data:
                for k in range(0, len(vertica_data)):
                    vertica_data[k] = tuple(vertica_data[k])
                vertica_df = pd.DataFrame(vertica_data)

                vertica_df.columns = vertica_headers
                try:
                    clickhouse_client.execute(f"""
                        insert into {clickhouse_table} values """
                                                            , vertica_df.to_dict('records'), types_check=False,
                                                            settings={'max_partitions_per_insert_block': 10000}
                                                            )
                except Exception:
                    exc = traceback.format_exc()
                    log_entry(
                        f'Вставка в таблицу {clickhouse_table} не удалась. Проверьте логи.\n'
                        f'Ошибка: {exc}', 'fact_log_transfer.txt', chat_id)
                    send_files_to_email(filepath=f'fact_log_transfer_{str(chat_id)}.txt'
                                        , txt=f'Ошибка при инсерте в таблицу {clickhouse_table} на Clickhoyse! Полный текст ошибки в файле.'
                                        , recipients=recipients_for_log)
                    return False

                vertica_data = vertica_cursor.fetchmany(100000)
            vertica_conn.commit()
            vertica_conn.close()

            print(f'{datetime.datetime.now().time()} - Loaded part {j} for categories. Category 1 ID: {category1_list[j]}')
            log_entry(f'Loaded part {j} for categories. Category 1 ID: {category1_list[j]}', 'fact_log_transfer.txt', chat_id)

        print(f'{datetime.datetime.now().time()} - Loaded part {i} for date {dates[i]}')
        log_entry(f'Loaded part {i} for date {dates[i]}', 'fact_log_transfer.txt', chat_id)

    print(f'Load done')
    log_entry(f'Load done', 'fact_log_transfer.txt', chat_id)
    return True


async def send_message(txt):
    await bot.send_message(chat_id=sys.argv[1], text=txt)


async def run_transfer():
    if transfer_to_ch(sys.argv[1]):
        return await send_message('Я завершил переливку факта на Clickhouse!')
    return await send_message('У меня не получилось завершить переливку факта на Clickhouse!\n'
                              'Тебе на почту должно было прийти сообщение с полной ошибкой.')


if __name__ == '__main__':
    executor.start(dp, run_transfer())
