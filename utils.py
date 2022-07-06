from account import *
from libraries import *
from constant import recipients_for_log


def get_passwords() -> dict:
    """
    Функция для открытия json файла с паролями
    """
    with open('secret.txt') as f:
        string = ''.join(i for i in f.readlines())
        passwords = json.loads(string)
    return passwords


def send_files_to_email(filepath: str, txt: str, recipients: list) -> None:
    """
    Функция отправки файлов на почту

    Параметры:
    txt (str) - текст в письму
    recipients (list of str's) - список получателей (их электронные адреса)

    Возвращаемое значение (NoneType) : None
    """

    server = 'zzz.o100.ru'
    user = passwords['user_smtp']
    password = passwords['password_smtp']

    recipients = recipients
    sender = 'feelingalive_da@mail.ru'
    subject = 'Ошибка в работе бота FactLoad'
    text = txt
    html = '<html><head></head><body><p>' + text + '</p></body></html>'

    # Ниже указать путь к файлу
    filepath = fr'{filepath}'
    basename = os.path.basename(filepath)
    filesize = os.path.getsize(filepath)

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = 'Download fact tg-bot <' + sender + '>'
    msg['To'] = ', '.join(recipients)
    msg['Reply-To'] = sender
    msg['Return-Path'] = sender
    msg['X-Mailer'] = 'Python/' + (python_version())

    part_text = MIMEText(text, 'plain')
    part_html = MIMEText(html, 'html')
    part_file = MIMEBase('application', 'octet-stream; name="{}"'.format(basename))
    part_file.set_payload(open(filepath, "rb").read())
    part_file.add_header('Content-Description', basename)
    part_file.add_header('Content-Disposition', 'attachment; filename="{}"; size={}'.format(basename, filesize))
    encoders.encode_base64(part_file)

    msg.attach(part_text)
    msg.attach(part_html)
    msg.attach(part_file)

    mail = smtplib.SMTP(server)
    mail.login(user, password)
    mail.sendmail(sender, recipients, msg.as_string())
    mail.quit()


def log_entry(txt: str, file_name: str, chat_id) -> None:
    """
    Функция для записи логов в случае ошибки при выполнении скрипта

    Параметры:
    text (str) - текст, который запишется в log-файл

    Возвращаемое значение (NoneType): None
    """
    file_name = file_name.replace('.txt', '')
    file_name = file_name + '_' + str(chat_id) + '.txt'
    with open(file_name, 'a+') as file:
        time = f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
        file.write(f'{time}: {txt}\n')
        file.write('------------------------------------------------------------------------------------------------\n')


def vertica_execute(sql: str, chat_id, file='fact_log_vertica') -> bool:
    """
    Функция для выполнения sql-скрипта на Вертике

    Если запрос не выполнился за 5 попыток (каждая с промежутком в 5 минут),
    то выполнение всего скрипта прекращается с помощью sys.exit(), а также происходит отбивка на почту.
    Параметры:
    sql (str) - sql-скрипт в текстовом формате

    Возвращаемое значение: bool
    """
    try_number = 1

    while try_number < 6:
        try:
            connection_check_flag = False
            vertica_connection = vertica_python.connect(**vertica_conn_info)
            connection_check_flag = True

            cursor = vertica_connection.cursor()
            cursor.execute(sql)

            vertica_connection.commit()
            vertica_connection.close()

            return True

        except Exception:
            exc = traceback.format_exc()
            log_entry(f'Vertica: Попытка {try_number} не удалась. Начинаю попытку {try_number + 1} через 6 минут\n'
                      f'Ошибка: {exc}', file + '.txt', chat_id)
            try_number += 1
            time.sleep(420)
    else:
        if try_number == 6:
            log_entry(f'Попытался 5 раз, не удалось, завершил цикл. Ошибка: {exc}', f'{file}.txt', chat_id)
            if connection_check_flag:
                vertica_connection.close()
            return False


def clickhouse_execute(sql: str, chat_id, file='fact_log_vertica'):
    """
    Функция для выполнения sql-скрипта на Кликхаусе

    Если запрос не выполнился за 5 попыток (каждая с промежутком в 5 минут),
    то выполнение всего скрипта прекращается с помощью sys.exit(), а также происходит отбивка на почту.
    Здесь нет переменной check_clickhouse по аналогии с функцией для вертики, т.к. тут не нужно закрывать коннект
    к клику и чекать, прошло ли подключение.

    Параметры:
    sql (str) - sql-скрипт в текстовом формате
    """
    try_number = 1

    while try_number < 6:
        try:
            clickhouse_result = clickhouse_client.execute(sql)
            TableData = pd.DataFrame.from_records(clickhouse_result)
            return (TableData)
        except Exception:
            exc = traceback.format_exc()
            log_entry(f'Clickhouse: Попытка {try_number} не удалась. Начинаю попытку {try_number + 1} через 6 минут\n'
                      f'Ошибка: {exc}', file + '.txt', chat_id)
            try_number += 1
            time.sleep(420)
    else:
        if try_number == 6:
            log_entry(f'Попытался 5 раз, не удалось, завершил цикл. Ошибка: {exc}', file + '.txt', chat_id)
            return False


def get_count_of_periods(date_start: str, date_end: str) -> int:
    """
    Функция для получения количества периодов.

    Параметры:
    date_start (str) - дата начала string
    date_end(str) - дата конца string

    Возвращаемое значение: int
    """

    date_start_dt = datetime.datetime.strptime(date_start, "%Y-%m-%d")
    date_end_dt = datetime.datetime.strptime(date_end, "%Y-%m-%d")
    return abs((date_start_dt.year - date_end_dt.year) * 12 + date_start_dt.month - date_end_dt.month)


def as_text(value) -> str:
    """
    Вспомогательная функция для преобразования любого значения в строку

    Параметры:
    value (*) - значение

    Возвращаемое значение (str) - значение value, преобразованное в строку
    """
    if value is None:
        return ""
    return str(value)


def get_df_from_vertica(sql: str, file: str, chat_id) -> pd.DataFrame:
    """
    Функция для выполнения DataFrame с Вертики

    Если запрос не выполнился за 5 попыток (каждая с промежутком в 5 минут),
    то возвращается False. Если выполнился, то возвращается pd.DataFrame.
    Параметры:
    sql (str) - sql-скрипт в текстовом формате

    Возвращаемое значение: pd.DataFrame
    """
    try_number = 1

    while try_number < 6:
        try:
            connection_check_flag = False
            vertica_connection = vertica_python.connect(**vertica_conn_info)
            connection_check_flag = True

            df = pd.read_sql(sql, vertica_connection)

            vertica_connection.commit()
            vertica_connection.close()

            return df

        except Exception:
            exc = traceback.format_exc()
            log_entry(f'Ошибка при получении dataframe на Vertica, попытка {try_number}.\n'
                      f'Через 6 минут начинаю новую попытку.\n'
                      f'{exc}', file, chat_id)
            time.sleep(420)
            try_number += 1
    else:
        if try_number == 6:
            log_entry(f'Попытался 5 раз получить dataframe на Vertica, не удалось, завершил цикл попыток. Ошибка: {exc}',
                      chat_id)
            if connection_check_flag:
                vertica_connection.close()
            return False


def get_dates(date_start: str, date_end : str, first_month_of_fact_date : str):
    """
    Вспомогательная функция для получения дат (начало и конец AccountingDate, а так же конец FactDate)

    Параметры:
    date_start (str) - дата начала периода
    date_end (str) - дата конца периода
    first_month_of_fact_date (str) - первый месяц, когда начинается FactDate

    Возвращаемое значение (tuple(str, str, str)) : кортеж дат
    """
    date1 = datetime.datetime.strptime(date_start, "%Y-%m-%d").date()

    if (datetime.datetime.strptime(date_end, "%Y-%m-%d").date() - datetime.datetime.strptime(first_month_of_fact_date, "%Y-%m-%d").date()).days > 0:
        date2 = datetime.datetime.strptime(first_month_of_fact_date, "%Y-%m-%d").date()
    else:
        date2 = datetime.datetime.strptime(date_end, "%Y-%m-%d").date()

    date3 = datetime.datetime.strptime(date_end, "%Y-%m-%d").date()

    return date1, date2, date3


def get_periods_of_accounting_date(date1: str, date2: str, count_of_months_in_subperiod: int) -> list:
    """
    Вспомогательная функция дла получения периодов с AccountingDate

    Параметры:
    date1 (str) - дата начала периода
    date2 (str) - дата конца периода
    count_of_months_in_subperiod (int) - количество месяцев в каждом периоде

    Возвращаемое значение (list) : список дат (периоды)
    """
    periods_of_AccountingDate = [(date1 + relativedelta(months=i)).strftime("%Y-%m-%d")
                                 for i in range(0,
                                                (date2.year - date1.year) * 12 + (date2.month - date1.month),
                                                count_of_months_in_subperiod)]
    periods_of_AccountingDate.append(date2.strftime("%Y-%m-%d"))
    return periods_of_AccountingDate


def get_periods_of_fact_date(date2: str, date3: str, count_of_months_in_subperiod: int) -> list:
    """
    Вспомогательная функция дла получения периодов с FactDate

    Параметры:
    date2 (str) - дата начала периода
    date3 (str) - дата конца периода
    count_of_months_in_subperiod (int) - количество месяцев в каждом периоде

    Возвращаемое значение (list) : список дат (периоды)
    """
    periods_of_FactDate = [(date2 + relativedelta(months=i)).strftime("%Y-%m-%d")
                           for i in range(0,
                                          (date3.year - date2.year) * 12 + (date3.month - date2.month),
                                          count_of_months_in_subperiod)]
    periods_of_FactDate.append(date3.strftime("%Y-%m-%d"))
    return periods_of_FactDate


def fill_dates(dates: pd.DataFrame, periods: list) -> pd.DataFrame:
    """
    Функция для заполнения датафрэйма периодов

    Параметры:
    dates (pd.DataFrame) - датафрэйм для заполнения
    periods (list) - список дат (периоды)

    Возвращаемое значение (pd.DataFrame) : заполненный датафрэйм с периодами
    """
    for i in range(0, len(periods) - 1):
        dates.loc[len(dates)] = [periods[i],
                                 periods[i + 1],
                                 datetime.datetime.strptime(periods[i], "%Y-%m-%d").date().strftime("%Y%m"),
                                 datetime.datetime.strptime(periods[i + 1], "%Y-%m-%d").date().strftime("%Y%m")]
    return dates


def get_periods(date_start: str, date_end: str, count_of_months_in_subperiod=1) -> list:
    """
    Вспомогательная функция дла получения периодов с AccountingDate

    Параметры:
    date1 (str) - дата начала периода
    date2 (str) - дата конца периода
    count_of_months_in_subperiod (int) - количество месяцев в каждом периоде

    Возвращаемое значение (list) : список дат (периоды)
    """

    date_start_dt = datetime.datetime.strptime(date_start, "%Y-%m-%d").date()
    date_end_dt = datetime.datetime.strptime(date_end, "%Y-%m-%d").date()

    periods_acc = [(date_start_dt + relativedelta(months=i)).strftime("%Y-%m-%d")
                   for i in range(0,
                                  (date_end_dt.year - date_start_dt.year) * 12 + (
                                              date_end_dt.month - date_start_dt.month),
                                  count_of_months_in_subperiod)]
    periods_acc.append(date_end_dt.strftime("%Y-%m-%d"))

    dates = pd.DataFrame(
        columns=['FILTER_START_DATE', 'FILTER_END_DATE', 'FILTER_START_INT_DATE', 'FILTER_END_INT_DATE'])
    periods = fill_dates(dates, periods_acc)

    return periods


def get_mdx_query(date_start: str, date_end: str, metric: str) -> list:
    """
    Функция для получения правильного MDX запроса для вычисления GMV к кубу

    Параметры:
    date_start (str) - дата начала периода
    date_end (str) - дата конца периода
    metric (str) - получаемая метрика

    Возвращаемое значение (list): список 2х составляющих (текстовые переменные: запрос к кубу и название самой метрики)
    """
    if metric == 'GMV':
        MDX_QUERY0 = """

        """
    elif metric == 'Items':
        MDX_QUERY0 = """
        
        """

    MDX_QUERY = MDX_QUERY0.replace('PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME', 'MEMBER_NAME')

    month = {'01': 'Январь', '02': 'Февраль', '03': 'Март',
             '04': 'Апрель', '05': 'Май', '06': 'Июнь',
             '07': 'Июль', '08': 'Август', '09': 'Сентябрь',
             '10': 'Октябрь', '11': 'Ноябрь', '12': 'Декабрь'}

    dt_type1 = 'Date_Accounting'

    date_start_dt = datetime.datetime.strptime(date_start, "%Y-%m-%d").date()
    date_end_dt = datetime.datetime.strptime(date_end, "%Y-%m-%d").date()

    dates = [(date_start_dt + relativedelta(months=i)).strftime("%Y-%m-%d")
             for i in
             range(0, (date_end_dt.year - date_start_dt.year) * 12 + (date_end_dt.month - date_start_dt.month))]

    pattern = '[FILTER_TYPE_1].[Y-M-D].[MonthName].&[FILTER_MONTH FILTER_YEAR]'
    string_to_insert = ''
    for i, date in enumerate(dates):
        year = date[:4]
        mon = date[5:7]
        string_to_insert += pattern.replace('FILTER_MONTH', month[mon]).replace('FILTER_YEAR', year)
        if i != len(dates) - 1:
            string_to_insert += ','

    MDX_QUERY = MDX_QUERY.replace(pattern, string_to_insert)
    MDX_QUERY = MDX_QUERY.replace('FILTER_TYPE_1', dt_type1)

    return [MDX_QUERY, metric]


def get_df_from_cube(MDX_QUERY: str, metric: str) -> pd.DataFrame:
    """
    Функция для получения данных из куба

    Параметры:
    MDX_QUERY (str) - DF какого параметра мы хотим получить (gmv, items и др.)
    metric (str) - метрика (gmv, items)

    Возвращаемое значение (pd.DataFrame) : таблица с выбранной метрикой по категориям из скоростного куба
    """

    # строка подключения; в поле Location пишем имя сервера
    connection_string = """
    """

    # Подключение к серверу
    conn = AdomdConnection(connection_string)
    conn.Open()
    cmd = conn.CreateCommand()
    cmd.CommandText = MDX_QUERY
    adp = AdomdDataAdapter(cmd)
    dataset_from_cube = DataSet()
    adp.Fill(dataset_from_cube)
    conn.Close()

    # Получение информации по таблице со скоростного куба
    rows_count = len(dataset_from_cube.Tables[0].Rows)
    cols_count = len(dataset_from_cube.Tables[0].Columns)

    columns = []
    for j in range(cols_count):
        columns.append(
            dataset_from_cube.Tables[0].Columns[j].ColumnName.split('.')[1].replace('[', '').replace(']', ''))

    # Заполнение датафрэйма
    speed_cube_table = pd.DataFrame(columns=columns)

    temp_list = [0 for elem in range(0, cols_count)]
    for i in range(1, rows_count):
        row = len(speed_cube_table)
        for j in range(0, cols_count):
            temp_list[j] = dataset_from_cube.Tables[0].Rows[i][j]
        speed_cube_table.loc[row] = temp_list

    for j in range(0, cols_count):
        temp_list[j] = dataset_from_cube.Tables[0].Rows[0][j]
    speed_cube_table.loc[rows_count] = temp_list

    for j in range(1, cols_count):
        speed_cube_table.iloc[:, j] = speed_cube_table.iloc[:, j].astype(str).str.replace(',', '.').replace('',
                                                                                                            0).astype(
            float)
    if metric == '1':
        speed_cube_table['1_olap'] = speed_cube_table[columns[1]] + speed_cube_table[columns[2]] - \
                                          speed_cube_table[columns[3]]
        speed_cube_table.drop(columns=columns[1:], inplace=True)
        return speed_cube_table
    elif metric == '2':
        speed_cube_table.drop(columns=["1"], inplace=True)
        speed_cube_table.rename(columns={"2": "2_olap"}, inplace=True)
        return speed_cube_table


def get_list_of_dates(month):
    date_start = datetime.datetime(2019, 1, 1).date()
    # prev_month = (datetime.datetime.now() - relativedelta(days=datetime.datetime.now().day)).date()

    # до какого именно месяца собирать остатки. если до последнего - то закмомментить строку ниже
    # prev_month = datetime.datetime(2021, 10, 31).date()

    prev_month = datetime.datetime.strptime(datetime.datetime.strptime(month, '%Y%m').strftime('%Y-%m-%d'), '%Y-%m-%d').date()

    # получаем список дат для разбивки
    list_of_dates = [date_start]
    date_inc = date_start + relativedelta(months=4)  # указать промежуток в relativedelta
    while date_inc < prev_month:
        list_of_dates.append(date_inc)
        date_inc += relativedelta(months=4)  # указать промежуток в relativedelta
    list_of_dates.append(prev_month + timedelta(days=1))

    # преобразование datetime-дат из списка в формат "%Y-%m-%d"-дат
    list_of_dates = [date.strftime("%Y-%m-%d") for date in list_of_dates]

    # добавление в даты еще одних кавычек для вставления в SQL-запрос
    for index, date in enumerate(list_of_dates):
        list_of_dates[index] = ''.join(["'", date, "'"])

    return list_of_dates