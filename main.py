from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from config_bot import TOKEN
from secondary_defs import *

bot = Bot(token=TOKEN)
dp = Dispatcher(bot)



class My_bot():        
    @dp.message_handler(commands='help')
    async def cmd_help(message: types.Message):
        await message.reply(text = ('Мне доступны две команды: /show_me и /show_all\n'
            'После /show_me необходимо передать интересующий тебя временной интервал. '
            'Принимает часы(Н), дни(D) и месяцы(М). Например /show_me 12h выведет тебе '
            'статистику за последние 12 часов. Если хочешь узнать за весь записанный период, '
            'пропиши /show_me all.\n'
            'Команда /show_all применяется ко всем пользователям, а не лично к тебе. Ведет себя как /show_me + какой-то аргумент\n'
            'Команда /ping покажет тебе, жив ли бот'))
    
    @dp.message_handler(commands='ping')
    async def cmd_show_me(message: types.Message):
        await message.reply(text = 'Я живой')
        
    @dp.message_handler(commands='show_me')
    async def cmd_show_me(message: types.Message):
        try:
            if message.text =='/show_me':
                return await message.reply(text = 'Упс. Значение пустое. Передай что-нибудь после /show_me, например /show_me 12h')

            type_data, time, chat_id, user_id, user_name = message_preprocessing(message)
            df = init_df(type_data, time, user_id)

            if df.shape[0] == 0:
                del df
                return await message.reply(text = 'За указанный период ты ничего не написал')
            

            df, number_message, mean_count_message = df_preprocessing(df)

            create_boxplot_word_count_me(df, user_name)
            create_graph_hourly_distribution_me(df, user_name)
            create_graph_days_distribution_me(df, user_name)

            del df

            await message.reply(text = 'Ты успел написать {} сообщений.\n'
                'В среднем ты писал {} слов в сообщении' .format(number_message, mean_count_message))
            await bot.send_photo(chat_id = chat_id, photo = open('boxplot.png', 'rb'))        
            await bot.send_photo(chat_id = chat_id, photo = open('dist_hours.png', 'rb'))        
            await bot.send_photo(chat_id = chat_id, photo = open('dist_days.png', 'rb'))

            del_graph(['boxplot.png', 'dist_hours.png','dist_days.png'])
        except ValueError or UnboundLocalError:
            await message.reply(text = 'Что-то пошло не так. Проверь введенные данные')
        
    @dp.message_handler(commands='show_all')
    async def cmd_show_all(message: types.Message):
        try:
            if message.text =='/show_all':
                return await message.reply(text = 'Упс. Значение пустое. Передай что-нибудь после /show_all, например /show_all 12h')
            type_data, time, chat_id, user_id, user_name = message_preprocessing(message)
            del user_name
            user_id = None

            df = init_df(type_data, time, user_id)
            if df.shape[0] == 0:
                del df
                return await message.reply(text = 'За указанный период вы ничего не написали')
            df, number_message, mean_count_message = df_preprocessing(df)
            create_sns_boxplot_word_count(df)
            graph_count_message(df)
            create_pie(df)

            del df
            await message.reply(text = 'Вы успели написать {} сообщений.\n'
                'В среднем вы писали {} слов в сообщении' .format(number_message, mean_count_message))
            await bot.send_photo(chat_id = chat_id, photo = open('snsboxplot.png', 'rb'))        
            await bot.send_photo(chat_id = chat_id, photo = open('count_message.png', 'rb'))
            await bot.send_photo(chat_id = chat_id, photo = open('pie.png', 'rb'))   
            del_graph(['snsboxplot.png','count_message.png','pie.png'])
        except ValueError or UnboundLocalError:
            await message.reply(text = 'Что-то пошло не так. Проверь введенные данные')

    @dp.message_handler(content_types=['photo'])
    async def scan_message(message: types.Message):
        """
        Check to BAYAN
        """
        user_id, date, text, user_name, message_id, chat_id = init_photo_data(message)

        if type(text)!= type(None):
            word_count = get_len(text)
        else:
            word_count = 0        
        send_data_regular_message(user_id, user_name, date, word_count, text, message_id)
        
        document_id = message.photo[0].file_id
        file_info = await bot.get_file(document_id)
        row = None
        is_in_BD = send_data_photo_message(user_id, message_id, date, file_info.file_unique_id, row)

        if type(is_in_BD[0][0]) != type(None):
            is_in_BD = int(is_in_BD[0][0])
            await bot.send_message(chat_id=chat_id, reply_to_message_id=is_in_BD, text = (
                'БАЯН БАЯН БАЯН!!!111. ВОТ ОРИГИНАЛ'))

    @dp.message_handler()
    async def get_message(message: types.Message):
        """
        Initializations data from message and send to def send_data().
        """
        user_id, date, text, user_name, message_id = init_message_data(message)
        word_count = get_len(text)
        
        send_data_regular_message(user_id, user_name, date, word_count, text, message_id)
 
async def on_startup(_):
    await bot.send_message(chat_id = '-1001246197532', text = 'Бот запущен')

if __name__ == "__main__":       
    My_bot = My_bot()    
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
