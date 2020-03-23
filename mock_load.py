import sys
from custom import faker_options_container
import _thread
import logging
import getpass
import string
import random
import uuid

# globals
host="10.227.160.17"
port=3306
password=""
username="root"
name="oomtestdb"


id_val = 0

def fake2db_logger():
    '''creates a logger obj'''
    # Pull the local ip and username for meaningful logging
    username = getpass.getuser()
    
    # Set the logger
    FORMAT = '%(asctime)-15s %(user)-8s %(message)s'
    logging.basicConfig(format=FORMAT)
    extra_information = {'user': username}
    logger = logging.getLogger('fake2db_logger')
    # --------------------
    return logger, extra_information


def str_generator():
    '''generates uppercase 8 chars
    '''
    return ''.join(random.choice(string.ascii_uppercase) for i in range(8))

def lower_str_generator():
    '''generates lowercase 8 chars
    '''
    return ''.join(random.choice(string.ascii_lowercase) for i in range(8))


def rnd_id_generator():
    '''generates a UUID such as :
    UUID('dd1098bd-70ac-40ea-80ef-d963f09f95a7')
    than gets rid of dashes
    '''
    id_val +=1
    # return str(uuid.uuid4()).replace('-', '')
    return str(id_val)
try:
    import mysql.connector
except ImportError:
    logger.error(
            'MySql Connector/Python not found on sys, '
            'you need to get it : http://dev.mysql.com/downloads/connector/python/',
            extra=extra_information)

logger, extra_information = fake2db_logger()

d = extra_information

try:
    from faker import Factory
except ImportError:
    logger.error('faker package not found onto python packages, please run : \
    pip install -r requirements.txt  \
    on the root of the project', extra=d)
    sys.exit(1)

def database_caller_creator(host, port, password, username, name=None):
    '''creates a mysql db
    returns the related connection object
    which will be later used to spawn the cursor
    '''
    cursor = None
    conn = None

    try:
        if name:
            db = name
        else:
            db = 'mysql_' + str_generator()

        conn = mysql.connector.connect(user=username, host=host, port=port, password=password)

        cursor = conn.cursor()
        cursor.execute('CREATE DATABASE IF NOT EXISTS ' + db + ' DEFAULT CHARACTER SET ''utf8''')
        cursor.execute('USE ' + db)
        # logger.warning('Database created and opened succesfully: %s' % db, extra=extra_information)

    except mysql.connector.Error as err:
        logger.error(err.msg, extra=extra_information)
        sys.exit(1)

    return cursor, conn

def custom_db_creator(batch_rows, cursor, conn, num_threads):
    '''creates and fills the table with simple regis. information
    '''

    custom_d = faker_options_container()
    sqlst = "CREATE TABLE `custom` (`id` BIGINT NOT NULL AUTO_INCREMENT,"
    custom_payload = "INSERT INTO custom ("
    
    custom = ["name", "country", "date"]
    # form the sql query that will set the db up
    for c in custom:
        if custom_d.get(c):
            sqlst += " `" + c + "` " + custom_d[c] + ","
            custom_payload += " " + c + ","
            logger.warning("fake2db found valid custom key provided: %s" % c, extra=extra_information)
        else:
            logger.error("fake2db does not support the custom key you provided.", extra=extra_information)
            sys.exit(1)
            
    sqlst += " PRIMARY KEY (`id`));"
    try:
        # create table
        cursor.execute(sqlst)
        conn.commit()
        print(sqlst)
    except mysql.connector.Error as err:
        logger.error(err.msg, extra=extra_information)



    custom_payload = custom_payload[:-1]
    custom_payload += ") VALUES ("
    for i in range(0, len(custom)):
        custom_payload += "%s, "
    custom_payload = custom_payload[:-2] + ")"

    def load_data():
        print("thread in")
        l_cursor,l_conn = database_caller_creator(host, port, password, username, name)
        faker = Factory.create()
        # load data func
        while True:
            multi_lines = []
            try:
                for i in range(0, batch_rows):
                    multi_lines.append([])
                    for c in custom:
                        multi_lines[i].append(getattr(faker, c)())
                
                l_cursor.executemany(custom_payload, multi_lines)
                l_conn.commit()
                # logger.warning('custom Commits are successful after write job!', extra=extra_information)
            except Exception as e:
                logger.error(e, extra=extra_information)
            pass
        print("thread out")

    try:
        for i in range(num_threads):
            _thread.start_new_thread(load_data, ())
            pass
    except:
        logger.error("unable start thread")

    while True:
       pass 

cursor,conn = database_caller_creator(host, port, password, username, name)

custom_db_creator(1000, cursor, conn, 10)
