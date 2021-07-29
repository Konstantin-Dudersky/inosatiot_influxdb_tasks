# запустить скрипт перед публикацией на github

echo
echo "-----> Create requirements.txt:"
pip freeze >setup/requirements.txt
