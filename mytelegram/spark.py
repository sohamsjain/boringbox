import json
import logging
from functools import wraps
from os.path import join, dirname
from urllib.parse import quote

from telegram import ChatAction
from telegram.error import TimedOut
from telegram.ext import CommandHandler, Filters, MessageHandler, Updater

spark_json_path = join(dirname(__file__), 'spark.json')

logging.basicConfig(filename='spark.log', level=logging.FATAL)


def send_typing_action(func):
    """Sends typing action while processing func command."""

    @wraps(func)
    def command_func(cls, update, context, *args, **kwargs):
        context.bot.send_chat_action(chat_id=update.effective_message.chat_id, action=ChatAction.TYPING)
        return func(cls, update, context, *args, **kwargs)

    return command_func


def dict_to_str(dictionary, pro='', epi='', kv_sep=': ', elem_sep='\n', keys=()):
    message = []
    if pro:
        message = [pro, "\n"]
    if keys:
        message.extend([str(k) + kv_sep + str(v) for k, v in dictionary.items() if k in keys])
    else:
        message.extend([str(k) + kv_sep + str(v) for k, v in dictionary.items()])
    if epi:
        message.extend(["\n", epi])
    return elem_sep.join(message)


class Spark:
    def __init__(self, jsonpath=None):
        self.init_file = jsonpath or spark_json_path

        self.token: str = self.read_key_from_settings("token")
        if self.token is None:
            self.token = input("Please Enter Api Token: ")
            self.write_key_to_settings("token", self.token)

        self.clients: dict = self.read_key_from_settings("clients")
        if self.clients is None:
            self.clients = {}
            self.write_key_to_settings('clients', self.clients)

        self.updater = Updater(self.token, use_context=True)
        self.dispatcher = self.updater.dispatcher
        self.bot = self.updater.bot
        self.manager = None
        # on different commands - answer in Telegram
        self.dispatcher.add_handler(CommandHandler("start", self.start))
        self.dispatcher.add_handler(CommandHandler("help", self.help))
        self.dispatcher.add_handler(CommandHandler("roll", self.dice))

        # on noncommand i.e message - echo the message on Telegram
        self.dispatcher.add_handler(MessageHandler(Filters.text, self.echo))

        # log all errors
        self.dispatcher.add_error_handler(self.error)

        # Start the Bot
        self.updater.start_polling()

        # Run the bot until you press Ctrl-C or the process receives SIGINT,
        # SIGTERM or SIGABRT. This should be used most of the time, since
        # start_polling() is non-blocking and will stop the bot gracefully.
        # self.updater.idle()

    def set_manager(self, manager):
        self.manager = manager

    def write_key_to_settings(self, key, value):
        try:
            file = open(self.init_file, 'r')
        except IOError:
            data = {}
            with open(self.init_file, 'w') as output_file:
                json.dump(data, output_file)
        file = open(self.init_file, 'r')
        try:
            data = json.load(file)
        except:
            data = {}
        data[key] = value
        with open(self.init_file, 'w') as output_file:
            json.dump(data, output_file)

    def read_key_from_settings(self, key):
        try:
            file = open(self.init_file, 'r')
        except IOError:
            file = open(self.init_file, 'w')
        file = open(self.init_file, 'r')
        try:
            data = json.load(file)
            return data[key]
        except:
            pass
        return None

    def save(self):
        self.write_key_to_settings('clients', self.clients)

    def stop(self):
        self.updater.stop()

    def send_all_clients(self, message):
        for name, chat_id in self.clients.items():
            self.send_message(chat_id, message)

    def send_message(self, chat_id, message):
        try:
            self.bot.send_message(chat_id, message)
        except TimedOut:
            pass

    @send_typing_action
    def start(self, update, context):
        """Send a message when the command /start is issued."""

        name = str(update.message.chat.first_name)
        chat_id = update.message.chat_id
        self.clients.update({name: chat_id})
        self.save()
        text = "Hey {}, I'm Spark.\n" \
               "I'll be delivering all notifications right here!".format(name)
        update.message.reply_text(text)

    @send_typing_action
    def help(self, update, context):
        """Send a message when the command /help is issued."""
        update.message.reply_text('Help!')

    @send_typing_action
    def echo(self, update, context):
        try:
            """Echo the user message."""
            msg = str(update.message.text)
            text = "Unfortunately, I can't help you with that. Allow me to redirect you to Soham.\nHere you go!\n\n" \
                   "https://api.whatsapp.com/send?phone=919619479104&text={}".format(quote(msg))
            update.message.reply_text(text)
        except Exception as e:
            print(e)

    def error(self, update, context):
        """Log Errors caused by Updates."""
        print(f'Update {update} caused error {context.error}')

    @send_typing_action
    def dice(self, update, context):
        chat_id = update.message.chat_id
        self.bot.send_dice(chat_id)


if __name__ == '__main__':
    spark = Spark(spark_json_path)
