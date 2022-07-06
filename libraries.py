from clickhouse_driver.client import Client
import vertica_python
import datetime
from dateutil.relativedelta import *
from datetime import timedelta
import pandas as pd
import numpy as np
import traceback
# import pyodbc
import openpyxl
import copy
import os
import os.path
import pickle
import smtplib
import sys
import tqdm
import json

from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from platform import python_version

# import clr

# Ниже вставить путь к файлу Microsoft.AnalysisServices.AdomdClient.dll на вашем компьютере;
# если он отсутствует - нужно скачать

PATH_TO_DLL = "C:\Program Files (x86)\Microsoft SQL Server Management Studio 18\Common7\IDE\Microsoft.AnalysisServices.AdomdClient.dll"

# clr.AddReference(PATH_TO_DLL)
# clr.AddReference("System.Data")

# from Microsoft.AnalysisServices.AdomdClient import AdomdConnection , AdomdDataAdapter
# from System.Data import DataSet
# from System import String
# from System.Collections import *
# from System import Security

import logging
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters import Text, state
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import ParseMode, CallbackQuery, ContentType
from aiogram.utils import executor
from aiogram.utils.markdown import text, bold, italic, code, pre
from aiogram.types import ParseMode, InputMediaPhoto, InputMediaVideo, ChatActions
import re
import subprocess
import asyncio
import time
# import multiprocessing
# import threading
import aiohttp

from keyboard import *
