import sqlite3
import random
import time
from config import ACCOUNTS, RECIPIENTS
from modules_settings import *
from settings import TYPE_WALLET, RANDOM_WALLET, IS_SLEEP, SLEEP_FROM, SLEEP_TO
from loguru import logger


file_path = "completed_tasks.db"

# Создайте подключение к базе данных
conn = sqlite3.connect('completed_tasks.db')
cursor = conn.cursor()

# Создайте таблицу для хранения выполненных задач, включая столбцы ethereum_address и task_description
cursor.execute('''CREATE TABLE IF NOT EXISTS completed_tasks (
                    task_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_description TEXT,
                    ethereum_address TEXT
                 )''')

tasks = [
         #"Starknet ID",
         #"Enable collateral ZkLend",
         #"Disable collateral ZkLend",
        # "Mint StarkVerse NFT",
        # "Mint NFT on Pyramid",
         #"Dmail send mail",
        # "JediSwap",
        # "MySwap",
        # "10kSwap",
        # "SithSwap",
        # "Avnu",
        # "Protoss",
        # #"Fibrous",
        # "Deposit ZkLend",
         "Unframed",
         "Flex",
         "StarkStars NFT"
    ]




def execute_random_task(ethereum_address, cursor, conn, try_again):
    unfinished_tasks = [task for task in tasks if task not in get_completed_tasks(ethereum_address, cursor)]
    logger.info(f'Выполненные задания: {get_completed_tasks(ethereum_address, cursor)}')
    logger.info(f'Осталось сделать: {unfinished_tasks}')
    id = 1
    key = ethereum_address
    if unfinished_tasks:
        task_to_execute = random.choice(unfinished_tasks)
        logger.info(f"Приватный ключ Starknet {ethereum_address}: Выполнение задачи: {task_to_execute}")
        if task_to_execute == 'Starknet ID':
            module = mint_starknet_id
            res = run_module(module, id, key)
        if task_to_execute == 'Enable collateral ZkLend':
            module = enable_collateral_zklend
            res = run_module(module, id, key)
        if task_to_execute == 'Disable collateral ZkLend':
            module = disable_collateral_zklend
            res = run_module(module, id, key)
        if task_to_execute == 'Mint StarkVerse NFT':
            module = mint_starkverse
            res = run_module(module, id, key)
        if task_to_execute == 'Mint NFT on Pyramid':
            module = create_collection_pyramid
            res = run_module(module, id, key)
        if task_to_execute == 'Dmail send mail':
            module = send_mail_dmail
            res = run_module(module, id, key)
        if task_to_execute == 'JediSwap':
            module = swap_jediswap
            res = run_module(module, id, key)
        if task_to_execute == 'MySwap':
            module = swap_myswap
            res = run_module(module, id, key)
        if task_to_execute == '10kSwap':
            module = swap_starkswap
            res = run_module(module, id, key)
        if task_to_execute == 'SithSwap':
            module = swap_sithswap
            res = run_module(module, id, key)
        if task_to_execute == 'Avnu':
            module = swap_avnu
            res = run_module(module, id, key)
        if task_to_execute == 'Protoss':
            module = swap_protoss
            res = run_module(module, id, key)
        if task_to_execute == 'Fibrous':
            module = swap_fibrous
            res = run_module(module, id, key)
        if task_to_execute == 'Deposit ZkLend':
            module = deposit_zklend
            res = run_module(module, id, key)
        if task_to_execute == 'Unframed':
            module = cancel_order_unframed
            res = run_module(module, id, key)  
        if task_to_execute == 'Flex':
            module = cancel_order_flex
            res = run_module(module, id, key)
        if task_to_execute == 'StarkStars NFT':
            module = mint_starkstars
            res = run_module(module, id, key)

        if 'code=None' in str(res):
            mark_task_as_completed(task_to_execute, ethereum_address, cursor, conn)
        else:
            if try_again < 3: # кол-во попыток повторного запуска при ошибке
                try_again += 1
                logger.warning(f'Не получилось выполнить {task_to_execute}. Пробую сделать другое задание')
                execute_random_task(ethereum_address, cursor, conn, try_again)
            else:
                logger.warning('Не получилось выполнить. Попробую при следующем запуске')
        tm = random.randint(110, 300)
        logger.info(f'Спим {tm}')
        time.sleep(tm)  # задержка
    else:
        logger.success(f"Кошелек Starknet {ethereum_address}: Все задачи выполнены!")

# Получение списка выполненных задач из базы данных для конкретного кошелька Ethereum
def get_completed_tasks(ethereum_address, cursor):
    cursor.execute("SELECT task_description FROM completed_tasks WHERE ethereum_address = ?", (ethereum_address,))
    completed_tasks = [row[0] for row in cursor.fetchall()]
    return completed_tasks

# Пометить задачу как выполненную в базе данных для конкретного кошелька Ethereum
def mark_task_as_completed(task_description, ethereum_address, cursor, conn):
        cursor.execute("INSERT INTO completed_tasks (task_description, ethereum_address) VALUES (?, ?)", (task_description, ethereum_address))
        conn.commit()


# Функция для выполнения одной задачи для конкретного кошелька Ethereum
def execute_one_task_for_ethereum_address(ethereum_address, cursor, conn, try_again):
    execute_random_task(ethereum_address, cursor, conn, try_again)
    tm = random.randint(5, 10)
    logger.info(f'Спим {tm}')
    time.sleep(tm)   # задержка




def execute_tasks_in_thread_pool(ethereum_addresses):
    num = 0
    print('Всего кошельков', len(ethereum_addresses))
    for ethereum_address in ethereum_addresses:
       try:
            num += 1
            print(num,'/',len(ethereum_addresses))
            try_again = 0
            conn = sqlite3.connect('completed_tasks.db')
            cursor = conn.cursor()
            execute_one_task_for_ethereum_address(ethereum_address, cursor, conn, try_again)
            conn.close()
       except Exception as err:
            logger.error(err)


def answer():
    print('Что будем делать?')
    print('1 - Продолжить текущие задания.')
    print('2 - Удалить текущую базу данных и начать все задания с начала.')
    answer_usr = input('Введите свой ответ 1 или 2 и нажмите ENTER: ')
    if answer_usr == '1':
        print('Продолжаем работу')
    elif answer_usr == '2':
        print('База данных удалена. Начинаю работу')
        cursor.execute("DELETE FROM completed_tasks")
        conn.commit()
    else:
        print('Такого ответа нет. Попробуйте еще раз.')
        answer()




def run_module(module, account_id, key):
        res = asyncio.run(module(account_id, key, TYPE_WALLET))
        return res

def main():

    answer()

    wallets = ACCOUNTS

    if RANDOM_WALLET:
        random.shuffle(wallets)

    execute_tasks_in_thread_pool(wallets)


if __name__ == '__main__':

    main()

