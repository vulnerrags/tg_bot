# Проект telegram-бота для сборки некоторой витрины, переливки этой витрины из одной СУБД в другую, сверки собранных метрик.

- main.py - работа непосредственно с ботом (декораторы, стэйджы и т.д.)
- transfer.py - функция переливки факта с Vertica на Clickhouse
- keyboard.py - все клавиатуры для бота (InlineKeyboard)
- config.py - конфигурация бота (токен, диспатчер, юзеры, прокси)
- account.py - учетки на Vertica и Clickhouse
- libraries.py - все используемые библиотеки
- constant.py - файл с константными значениями
- utils.py - все функции, используемые в transfer.py и в download.py