# inosatiot_influxdb_mirror

Зеркалирование данных Influxdb.

# Установка
1. Скачать проект с github
   
        $ mkdir ~/inosatiot && cd ~/inosatiot && sudo apt install git
        $ cd ~/inosatiot
        $ git clone https://github.com/Konstantin-Dudersky/inosatiot_influxdb_mirror.git
        $ cd inosatiot_influxdb_mirror

2. Создать файл с настройками config_inosatiot_resources_sim.yaml. Шаблон находится в setup/inosatiot_cfg.json_template.
       
        $ cp setup/config_example.yaml ../config_inosatiot_influxdb_mirror.yaml

   Прописать в файле настройки.


3. Установка библиотек python в файле setup/setup.sh
   
        $ chmod +x setup/setup.sh && setup/setup.sh

   Что делает скрипт:
   - Обновляет пакеты в системе
   - Устанавливает пакеты python
   - Создает виртуальное окружение venv, скачивает необходимые пакеты
   - Создает сервис systemd, устанавливает автозапуск
    
    
4. После установки можно запустить на выполнение через systemd
   
        $ sudo systemctl start config_inosatiot_resources_server.service  // запустить
        $ sudo systemctl stop config_inosatiot_resources_server.service  // остановить
        $ sudo systemctl restart config_inosatiot_resources_server.service  // перезапустить
        $ sudo systemctl status config_inosatiot_resources_server.service  // просмотреть статус

# Обновить проект
- Синхронизировать проект с github (локальные изменения теряются)
   
        $ sudo systemctl stop inosatiot_resources_server.service
        $ cd ~/inosatiot/inosatiot_influxdb_mirror/
        $ git fetch origin && git reset --hard origin/master && git clean -f -d
        $ chmod +x setup/setup.sh && setup/setup.sh

Файл настроек находится в вышестоящей папке, поэтому настройки остаются