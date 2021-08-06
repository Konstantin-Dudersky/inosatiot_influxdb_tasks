# запустить скрипт перед публикацией на github

echo
echo "-----> Create requirements.txt:"
venv/bin/pip3 freeze >setup/requirements.txt
