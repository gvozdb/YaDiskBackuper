#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
YaDiskBackuper

Author: Pavel Gvozdb
Version: 1.0.0-pl
"""

import os
import sys
import shutil
import subprocess
import yaml
import time

from datetime import datetime, date, timedelta
from os.path import join as joinpath

from YaDiskClient import YaDisk, YaDiskException


######### >> Параметры
current_path = os.path.dirname( sys.argv[0] ) # директория размещения скрипта (надо для cron)
if not current_path:
	current_path = '.'

config_f = open( current_path +"/config.yaml" )
config = yaml.load( config_f )

mysql_u = config['mysql']['user']
mysql_p = config['mysql']['pass']
yd_u = config['ydisk']['user']
yd_p = config['ydisk']['pass']
date_today = date.today()
date_today_str = str( date_today )
path_backup = config['path_backup']
path_webdav = config['path_webdav']
path_backup_today = path_backup + date_today_str +"/"
path_webdav_today = path_webdav + date_today_str +"/"
backup_sys = config['backup']['sys']
backup_db = config['backup']['db']
backup_files = config['backup']['files']
remove_old_logs = config['remove_old_logs']
store_old = config['store_old']
sleep_time = 2 # кол-во секунд, на которое время от времени засыпать...

config_f.close()
######### <<


disk = YaDisk( yd_u, yd_p ) # Подключаемся к ЯДиску


######### >> Создаем директорию для сегодняшнего бэкапа
if not os.path.exists( path_backup_today ):
	try:
		os.makedirs( path_backup_today )
	except BaseException:
		print( 'Dir '+ path_backup_today +' already exists' )

os.chdir( path_backup_today ) # переходим в директорию с бекапами
######### <<

######### >> Создаём директорию для бекапа на ЯДиске
try_ = True
try_i = 1

while try_ != False and try_i <= 5:
	try:
		disk.ls( path_webdav_today )
		try_ = False
	except YaDiskException as e:
		if e.code == 404:
			dir_webdav = '/'
			for dir in path_webdav_today.split('/'):
				if dir:
					dir_webdav += dir +"/"
					try:
						dir_webdav_ls = disk.ls( dir_webdav )
					except YaDiskException as e:
						try:
							disk.mkdir( dir_webdav )
							try_ = False
						except YaDiskException as e:
							if e.code == 500:
								try_ = True
	try_i += 1
######### <<

######### >> Сохраняем БД и заливаем на ЯДиск
if backup_db:
	dbs = str( subprocess.Popen( "mysql -u"+ mysql_u +" -p"+ mysql_p +" -e'show databases;' | grep -v information_schema | grep -v Database", stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True, universal_newlines=True ).communicate()[0] )

	for db in dbs.split('\n'):
		if db and db != 'mysql' and db != 'performance_schema' and db != 'pma':
			db_file = date_today_str +"-www-"+ db +".sql.bz2"

			try_ = True
			try_i = 1

			while try_ != False and try_i <= 5:
				subprocess.Popen( "mysqldump --skip-lock-tables -u"+ mysql_u +" -p"+ mysql_p +" "+ db +" | bzip2 -c > "+ db_file, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True, universal_newlines=True ).communicate()

				time.sleep(sleep_time)

				if try_i % 2 == 0:
					disk.upload( os.path.abspath( db_file ), path_webdav_today + db_file ) # заливаем на ЯДиск
				else:
					subprocess.Popen( "curl --user "+ yd_u +":"+ yd_p +" -T "+ os.path.abspath(db_file) +" https://webdav.yandex.ru"+ path_webdav_today, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True, universal_newlines=True ).communicate()

				os.remove( os.path.abspath(db_file) ) # удаляем файл с сервера

				try:
					for today_dump_file in disk.ls( path_webdav_today ):
						if not today_dump_file.get('isDir') and today_dump_file.get('displayname') == db_file:
							try_ = False
				except YaDiskException as e:
					if e.code == 404 or e.code == 500:
						continue

				try_i += 1
######### <<

######### >> Сохраняем системные директории и заливаем на ЯДиск
if backup_sys:
	sys_files = {
		"etc": {
			"dir"	: "/etc/",
			"file"	: date_today_str +"-sys-etc.tar.bz2"
		},
		"log": {
			"dir"	: "/var/log/",
			"file"	: date_today_str +"-sys-var-log.tar.bz2"
		},
		"root": {
			"dir"	: "/root/",
			"file"	: date_today_str +"-sys-root.tar.bz2"
		}
	}

	for sys in sys_files:
		try_ = True
		try_i = 1

		while try_ != False and try_i <= 5:
			subprocess.Popen( "tar -cjf "+ sys_files[sys]['file'] +" "+ sys_files[sys]['dir'], stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True, universal_newlines=True ).communicate()

			time.sleep(sleep_time)

			if try_i % 2 == 0:
				disk.upload( os.path.abspath( sys_files[sys]['file'] ), path_webdav_today + sys_files[sys]['file'] ) # заливаем на ЯДиск
			else:
				subprocess.Popen( "curl --user "+ yd_u +":"+ yd_p +" -T "+ os.path.abspath( sys_files[sys]['file'] ) +" https://webdav.yandex.ru"+ path_webdav_today, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True, universal_newlines=True ).communicate()

			os.remove( os.path.abspath( sys_files[sys]['file'] ) ) # удаляем файл с сервера

			try:
				for today_dump_file in disk.ls( path_webdav_today ):
					if not today_dump_file.get('isDir') and today_dump_file.get('displayname') == sys_files[sys]['file']:
						try_ = False
			except YaDiskException as e:
				if e.code == 404 or e.code == 500:
					continue

			try_i += 1
######### <<

######### >> Сохраняем файлы сайтов и заливаем на ЯДиск
if backup_files:
	sites_dir = "/var/www/"
	for site in os.listdir( sites_dir ):
		if not os.path.isfile( joinpath( sites_dir, site ) ) and site != 'pma' and site != 'html':
			site_file = date_today_str +"-www-"+ site +".tar.bz2"

			try_ = True
			try_i = 1

			while try_ != False and try_i <= 5:
				subprocess.Popen( "tar -cjf "+ site_file +" "+ joinpath( sites_dir, site ) +" --exclude=core/cache/*", stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True, universal_newlines=True ).communicate()

				time.sleep(sleep_time)

				if try_i % 2 == 0:
					disk.upload( os.path.abspath( site_file ), path_webdav_today + site_file ) # заливаем на ЯДиск
				else:
					subprocess.Popen( "curl --user "+ yd_u +":"+ yd_p +" -T "+ os.path.abspath( site_file ) +" https://webdav.yandex.ru"+ path_webdav_today, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True, universal_newlines=True ).communicate()

				os.remove( os.path.abspath( site_file ) ) # удаляем файл с сервера

				try:
					for today_dump_file in disk.ls( path_webdav_today ):
						if not today_dump_file.get('isDir') and today_dump_file.get('displayname') == site_file:
							try_ = False
				except YaDiskException as e:
					if e.code == 404 or e.code == 500:
						continue

				try_i += 1
######### <<

######### >> Чистим старые логи, удаляем папку созданную для сегодняшних бекапов
if remove_old_logs:
	subprocess.Popen( "find /var/log -type f \( -name \"*.gz\" -o -name \"*.1*\" \) -exec rm '{}' \;", stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True, universal_newlines=True ).communicate()

	shutil.rmtree( path_backup_today ) # удаляем папку созданную для сегодняшних бекапов
######### <<

######### >> Удаляем старые бекапы с ЯДиска
if store_old:
	for dumps_dir in disk.ls( path_webdav ):
		if dumps_dir.get('isDir') and dumps_dir.get('path') != path_webdav:
			dir_dump = dumps_dir.get('path').split('/')[-1] if dumps_dir.get('path').split('/')[-1] != '' else dumps_dir.get('path').split('/')[-2]

			dir_dump_date = dir_dump.split('-')

			date_backup = datetime( int(dir_dump_date[0]), int(dir_dump_date[1]), int(dir_dump_date[2]) ).date()
			date_today = date.today()
			date_store_old = date_today - timedelta( days=store_old )

			if date_backup <= date_store_old:
				disk.rm( path_webdav + str(date_backup) ) # Удаляем старые папки с ЯДиска
######### <<