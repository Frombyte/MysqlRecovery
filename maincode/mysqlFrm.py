import io

import mysql.connector
import uuid
import os
from pathlib import Path
import struct


class MysqlFrm:
    def __init__(self, username, password, port, srcdir):
        self.username = username
        self.password = password
        self.port = port
        self.srcdir = srcdir
    def convert_to_sql(self):
        dbname = str(uuid.uuid4()).replace('-', '')
        all_sql = io.StringIO()  # 使用StringIO来累积所有生成的SQL语句

        try:
            conn = mysql.connector.connect(user=self.username, password=self.password, host='localhost', port=self.port)
            cursor = conn.cursor()
            cursor.execute("SET NAMES utf8;")
            cursor.execute(f"CREATE DATABASE `{dbname}`")
            cursor.execute("SELECT @@datadir")
            datadir, = cursor.fetchone()
            datadir = os.path.join(datadir, dbname) + "\\"
            if not os.path.exists(datadir):
                os.makedirs(datadir)
            conn.database = dbname

            for frm in Path(self.srcdir).glob('*.frm'):
                print(f"convert : {frm}")
                tablename = frm.stem
                oldtype = None
                try:
                    with open(frm, 'rb') as file:
                        b = file.read()
                        if b[0] != 0xfe or b[1] != 0x01:
                            print("not a table frm")
                            continue

                        if b[3] == 10:  # _DB_TYPE_MRG_MYISAM
                            with open(datadir + tablename + ".mrg", 'w') as f:
                                f.write('')
                        elif b[3] == 11:  # _DB_TYPE_BLACKHOLE_DB
                            pass
                        elif b[3] == 16:  # _DB_TYPE_ARCHIVE_DB
                            with open(datadir + tablename + ".arz", 'w') as f:
                                f.write('')
                        elif b[3] == 17:  # _DB_TYPE_CSV_DB
                            with open(datadir + tablename + ".csv", 'w') as f:
                                f.write('')
                        else:  # modify frm set type=memory
                            offset = struct.unpack_from('<H', b, 6)[0]  # io_size
                            offset += struct.unpack_from('<H', b, 0x0e)[0]  # tmp_key_length
                            offset += struct.unpack_from('<H', b, 0x10)[0]  # rec_length
                            offset += 2  # 00 00
                            len = struct.unpack_from('<H', b, offset)[0]  # type string length, in word
                            offset += 2
                            oldtype = b[offset:offset + len].decode('latin1')
                            b = bytearray(b)
                            b[3] = 6  # _DB_TYPE_HEAP
                            b[offset:offset + 6] = b'MEMORY'  # MEMORY

                        with open(datadir + frm.name, 'wb') as fw:
                            fw.write(b)

                        cursor.execute("FLUSH TABLES;")
                        cursor.execute(f"SHOW CREATE TABLE `{tablename}`")
                        sql = cursor.fetchone()[1]
                        if oldtype:
                            sql = sql.replace(" ENGINE =MEMORY", f" ENGINE ={oldtype}")
                        all_sql.write(sql + ";\n")

                        cursor.execute(f"DROP TABLE `{tablename}`")
                except Exception as ex:
                    print(f"error!! : {ex}")

            cursor.execute(f"DROP DATABASE `{dbname}`")
            conn.close()
        except Exception as ex:
            print(ex)

        all_sql.seek(0)
        return all_sql
