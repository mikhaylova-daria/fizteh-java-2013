## Shell (Оболочка)
Консольное приложение, частично эмулирующее оболочку
[shell](http://en.wikipedia.org/wiki/Unix_shell).

```
java Shell [COMMAND1 [; COMMAND2 ...]]
```

Если Shell запускается без параметров, то запускается интерактивный режим,
в котором пользователь может ввести команду (или команды) прямо в консоли.

Список команд:
* ```cd <absolute path|relative path>``` &mdash; change directory, смена
текущей директории. Поддерживаются ```.```, ```..```, относительные и абсолютные
пути
* ```mkdir <dirname>``` &mdash; создание директории в текущей директории
* ```pwd``` &mdash; print working directory, печатает абсолютный путь к текущей
директории
* ```rm <file|dir>``` &mdash; удаляет указанную в параметрах папку (рекурсивно)
или файл
* ```cp <source> <destination>``` &mdash; копирует указанную в параметра
папку/файл в указанное место
* ```mv <source> <destination>``` &mdash; переносит указанный файл/папку в
новое место (файл на прежнем месте удаляется). В частности переименовывает
файл/папку, если ```source``` и ```destination``` находятся в одной папке
* ```dir``` &mdash; печатает содержимое текущей директории
* ```exit``` &mdash; выход из приложения

За один раз можно написать несколько команд. Разделителем команд является
```;``` (точка с запятой).

### Интерактивный режим
В интерактивном режиме должно отображаться "приглашение" &mdash; ```$ ``` (знак
доллара и пробел), после которого производится ввод команд. Также в интерактивном
режиме допускается вывод текущей директории перед "приглашением".

### Пакетный режим
Если запустить консольное приложение с параметрами, то параметры должны
интерпретироваться как команды (все команды должны склеиваться через пробел).
Приложение должно выполнить последовательно все команды и завершиться.

В случае ошибки команды, приложение должно выводить ошибку в stderr.

В случае ошибки в любой из команд, приложение должно написать сообщение об
ошибке и завершиться с ненулевым кодом.

### Вывод команд
Вывод команд имеет строгий формат. Никакого дополнительного вывода в stdout быть
не должно (в финальном коде вся debug-информация должна отсутствовать).

```(bash)
$ cd /noexistingdir
cd: '/noexistingdir': No such file or directory
$ cd /home/user
$ pwd
/home/user
$ dir
Folder
file.txt
$ mkdir MyFolder
$ dir
Folder
MyFolder
file.txt
$ cp file.txt MyFolder
$ cd MyFolder
$ dir
file.txt
$ mv file.txt file2.txt
$ dir
file2.txt
$ rm file.txt
rm: cannot remove 'file.txt': No such file or directory
$ rm file2.txt
$ dir
$ cd ..
$ dir
Folder
MyFolder
file.txt
$ mv Folder MyFolder
$ dir
MyFolder
file.txt
$ exit
```
