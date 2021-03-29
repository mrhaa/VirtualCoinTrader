#_*_ coding: utf-8 _*_

import os
import platform

import telegram


base_dir = (os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))
if platform.system() == 'Windows':
    key_dir =  '%s\\'%(base_dir)
else:
    key_dir = '%s/'%(base_dir)


class Telegram():

    def __init__(self):
        print("Generate Telegram.")

        self.token = None
        self.chat_id = None

        self.bot = None

    def __del__(self):
        print("Destroy Telegram.")

    def get_token(self):

        f = open("%stelegram.txt"%(key_dir))
        lines = f.readlines()
        self.token = lines[0].strip()
        self.chat_id = lines[1].strip()
        f.close()

        return self.token

    def get_bot(self):

        self.get_token()
        self.bot = telegram.Bot(token=self.token)

    def send_message(self, msg='Hello'):

        self.bot.sendMessage(chat_id=self.chat_id, text=msg)


