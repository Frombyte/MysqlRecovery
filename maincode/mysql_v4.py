import re
import os
import sqlite3
import struct
import time
import binascii
import datetime

json_type_0b = {
    0xe03f:0.5,
    61503:1.0,
    63551: 1.5,
    64:2.0,
    0x0440:2.5,
    2112:3.0,
    0x0c40:3.5,
    4160:4.0,
    4672:4.5,
    5184:5.0,
    0x1640:5.5,
    6208:6.0,
    6720:6.5,
    7232:7.0,
    0x1e40:7.5,
    8256:8.0,
    8768:9.0,
    9280:10.0,
    9792:11.0,
    10304:12.0,
    10816:13.0,
    11328:14.0,
    11840:15.0,
    12352:16.0,
    12608:17.0,
    12864:18.0,
    13120:19.0,
    13376:20.0,
    0x3540:21.0,
    0x3640: 22.0,
    0x3740: 23.0,
    0x3840: 24.0,
    0x3940: 25.0,
    0x3a40: 26.0,
    0x3b40: 27.0,
    0x3c40: 28.0,
    0x3d40: 29.0,
    0x3e40: 30.0,
    0x4940:50.0,
    0x805140:70.0,
    0x5440:80.0,
    0x805840:98,
    0xC05840:99.0,
    22848:100.0

}
class MysqlV4:
    def __init__(self):
        pass

    def get_lob(self,file_no, page_no):
        next_page = page_no
        col = bytearray()
        length = 0
        while next_page != 0xffffffff:
            page_file.seek(16384 * next_page)
            lob_page = page_file.read(16384)
            file_id = struct.unpack('>L', lob_page[34:38])[0]
            size = struct.unpack('>L', lob_page[38:42])[0]
            next_page = struct.unpack('>L', lob_page[42:46])[0]
            if file_id == file_no:
                col[length:length + size] = lob_page[46:46 + size]
                length += size
            else:
                break
        return col



    def get_null_map(self,null_data):
        null_map = []
        for i in range(len(null_data)):
            a = bin(struct.unpack('B', null_data[len(null_data) - i - 1:len(null_data) - i])[0])[2:].zfill(8)
            for k in range(8):
                b = int(a[8 - k - 1:8 - k])
                null_map.append(b)
        return null_map



    def get_char(self,col_data, type_com, lenth_bin, comments):
        col = ''
        try:
            col_tmp = col_data.decode(encoding='utf-8').rstrip().replace('\\', '\\\\').replace("'", "\\'")
            col_tmp = col_tmp.replace('\r', '\\r').replace('\n', '\\n')
            col = f"'{col_tmp}'"
        except:
            try:
                col_tmp = col_data.decode(encoding='gbk').encode(encoding='utf-8').decode(
                    encoding='utf-8').rstrip().replace('\\', '\\\\').replace("'", "\\'")
                col_tmp = col_tmp.replace('\r', '\\r').replace('\n', '\\n')
                col = f"'{col_tmp}'"
            except:
                col_tmp = col_data.decode(encoding='big5').encode(encoding='utf-8').decode(
                    encoding='utf-8').rstrip().replace('\\', '\\\\').replace("'", "\\'")
                col_tmp = col_tmp.replace('\r', '\\r').replace('\n', '\\n')
                col = f"'{col_tmp}'"
        return col

    def get_varchar(self,col_data, type_com, lenth_bin, comments):
        col = ''
        try:
            col_tmp = col_data.decode(encoding='utf-8').rstrip().replace('\\', '\\\\').replace("'", "\\'")
            col_tmp = col_tmp.replace('\r', '\\r').replace('\n', '\\n')
            col = f"'{col_tmp}'"
        except:
            try:
                col_tmp = col_data.decode(encoding='gbk').encode(encoding='utf-8').decode(
                    encoding='utf-8').rstrip().replace('\\', '\\\\').replace("'", "\\'")
                col_tmp = col_tmp.replace('\r', '\\r').replace('\n', '\\n')
                col = f"'{col_tmp}'"
            except:
                col_tmp = col_data.decode(encoding='big5').encode(encoding='utf-8').decode(
                    encoding='utf-8').rstrip().replace('\\', '\\\\').replace("'", "\\'")
                col_tmp = col_tmp.replace('\r', '\\r').replace('\n', '\\n')
                col = f"'{col_tmp}'"
        return col

    def get_tinytext(self,col_data, type_com, lenth_bin, comments):
        col = ''
        lob_idx = struct.unpack('B', lenth_bin)[0] >> 4
        if lob_idx != 0x0c:
            try:
                col_tmp = col_data.decode(encoding='utf-8').rstrip().replace('\\', '\\\\').replace("'", "\\'")
                col_tmp = col_tmp.replace('\r', '\\r').replace('\n', '\\n')
                col = f"'{col_tmp}'"
            except:
                col_tmp = col_data.decode(encoding='GBK').encode(encoding='utf-8').decode(
                    encoding='utf-8').rstrip().replace('\\', '\\\\').replace("'", "\\'")
                col_tmp = col_tmp.replace('\r', '\\r').replace('\n', '\\n')
                col = f"'{col_tmp}'"
        else:
            col_pre = col_data[:-20]
            file_id = struct.unpack('>L', col_data[-20:-16])[0]
            page_no = struct.unpack('>L', col_data[-16:-12])[0]
            if page_no != 0:
                col = self.get_lob(self,file_id, page_no)
                col = col_pre + col
                try:
                    col_tmp = col.decode(encoding='utf-8').rstrip().replace('\\', '\\\\').replace("'", "\\'")
                    col_tmp = col_tmp.replace('\r', '\\r').replace('\n', '\\n')
                    col = f"'{col_tmp}'"
                except:
                    col_tmp = col.decode(encoding='GBK').encode(encoding='utf-8').decode(encoding='utf-8').rstrip().replace(
                        '\\', '\\\\').replace("'", "\\'")
                    col_tmp = col_tmp.replace('\r', '\\r').replace('\n', '\\n')
                    col = f"'{col_tmp}'"
            else:
                col = "''"
        return col

    def get_mediumtext(self,col_data, type_com, lenth_bin, comments):
        col = ''
        lob_idx = struct.unpack('B', lenth_bin)[0] >> 4
        if lob_idx != 0x0c:
            try:
                col_tmp = col_data.decode(encoding='utf-8').rstrip().replace('\\', '\\\\').replace("'", "\\'")
                col_tmp = col_tmp.replace('\r', '\\r').replace('\n', '\\n')
                col = f"'{col_tmp}'"
            except:
                col_tmp = col_data.decode(encoding='GBK').encode(encoding='utf-8').decode(
                    encoding='utf-8').rstrip().replace('\\', '\\\\').replace("'", "\\'")
                col_tmp = col_tmp.replace('\r', '\\r').replace('\n', '\\n')
                col = f"'{col_tmp}'"
        else:
            col_pre = col_data[:-20]
            file_id = struct.unpack('>L', col_data[-20:-16])[0]
            page_no = struct.unpack('>L', col_data[-16:-12])[0]
            if page_no != 0:
                col = self.get_lob(file_id, page_no)
                col = col_pre + col
                try:
                    col_tmp = col.decode(encoding='utf-8').rstrip().replace('\\', '\\\\').replace("'", "\\'")
                    col_tmp = col_tmp.replace('\r', '\\r').replace('\n', '\\n')
                    col = f"'{col_tmp}'"
                except:
                    col_tmp = col.decode(encoding='GBK').encode(encoding='utf-8').decode(encoding='utf-8').rstrip().replace(
                        '\\', '\\\\').replace("'", "\\'")
                    col_tmp = col_tmp.replace('\r', '\\r').replace('\n', '\\n')
                    col = f"'{col_tmp}'"
            else:
                col = "''"
        return col

    def get_int(self,col_data, type_com, lenth_bin, comments):
        if unsigned == 0:
            sign = struct.unpack('B', col_data[:1])[0] >> 7 if unsigned == 0 else 1
            if sign == 0:
                col = (~struct.unpack('>l', col_data)[0]) & 0x7FFFFFFF
                col = ~col
                col = f"'{col}'"
            else:
                col = struct.unpack('>L', col_data)[0] & 0x7FFFFFFF
                col = f"'{col}'"
        else:
            col = struct.unpack('>L', col_data)[0]
            col = f"'{col}'"
        return col

    def get_bigint(self,col_data, type_com, lenth_bin, comments):
        if unsigned == 0:
            sign = struct.unpack('B', col_data[:1])[0] >> 7 if unsigned == 0 else 1
            if sign == 0:
                col = (~struct.unpack('>q', col_data)[0]) & 0x7FFFFFFFFFFFFFFF
                col = ~col
                col = f"'{col}'"
            else:
                col = struct.unpack('>Q', col_data)[0] & 0x7FFFFFFFFFFFFFFF
                col = f"'{col}'"
        else:
            col = struct.unpack('>Q', col_data)[0]
            col = f"'{col}'"
        return col

    def get_tinyint(self,col_data, type_com, lenth_bin, comments):
        if unsigned == 0:
            sign = struct.unpack('B', col_data[:1])[0] >> 7 if unsigned == 0 else 1
            if sign == 0:
                col = (~struct.unpack('b', col_data)[0]) & 0x7F
                col = ~col
                col = f"'{col}'"
            else:
                col = struct.unpack('B', col_data)[0] & 0x7F
                col = f"'{col}'"
        else:
            col = struct.unpack('B', col_data)[0]
            col = f"'{col}'"
        return col

    def get_smallint(self,col_data, type_com, lenth_bin, comments):
        if unsigned == 0:
            sign = struct.unpack('B', col_data[:1])[0] >> 7 if unsigned == 0 else 1
            if sign == 0:
                col = (~struct.unpack('>h', col_data)[0]) & 0x7FFF
                col = ~col
                col = f"'{col}'"
            else:
                col = struct.unpack('>H', col_data)[0] & 0x7FFF
                col = f"'{col}'"
        else:
            col = struct.unpack('>H', col_data)[0]
            col = f"'{col}'"
        return col

    def get_mediumint(self,col_data, type_com, lenth_bin, comments):
        if unsigned == 0:
            sign = struct.unpack('B', col_data[:1])[0] >> 7
            if sign == 0:
                col = (~int.from_bytes(col_data, byteorder='big', signed=True)) & 0x7FFFFF
                col = ~col
                col = f"'{col}'"
            else:
                col = int.from_bytes(col_data, byteorder='big', signed=False) & 0x7FFFFF
                col = f"'{col}'"
        else:
            col = int.from_bytes(col_data, byteorder='big', signed=False)
            col = f"'{col}'"
        return col

    def get_datetime(self,col_data, type_com, lenth_bin, comments):
        col = ''
        # time lenth = 5
        col_data_tmp = ((struct.unpack('>L', col_data[0:4])[0] << 8) + (struct.unpack('B', col_data[4:5]))[
            0]) & 0x7FFFFFFFFF
        year = str((col_data_tmp >> 22) // 13).zfill(4)
        month = str((col_data_tmp >> 22) % 13).zfill(2)
        day = str((col_data_tmp & 0x3FFFFF) >> 17).zfill(2)
        hour = str((col_data_tmp & 0x1FFFF) >> 12).zfill(2)
        min = str((col_data_tmp & 0xFFF) >> 6).zfill(2)
        sec = str(col_data_tmp & 0x3F).zfill(2)
        col = year + '-' + month + '-' + day + ' ' + hour + ':' + min + ':' + sec
        # if (col > '1970-01-01 00:00:00' and col < time_limit_str): #or col == '0000-00-00 00:00:00':
        if (col < time_limit_str):  # or col == '0000-00-00 00:00:00':
            if len(col_data) > 5:
                dec_len = len(col_data) - 5
                dec = int.from_bytes(col_data[5:5 + dec_len], 'big')
                col = rf"'{col}.{dec}'"
            # time lenth = 8
            # col_data = str(struct.unpack('>Q',col_data)[0] & 0x7FFFFFFFFFFFFFFF)
            # year = col_data[:4]
            # month = col_data[4:6].zfill(2)
            # day = col_data[6:8].zfill(2)
            # hour = col_data[8:10].zfill(2)
            # min = col_data[10:12].zfill(2)
            # sec = col_data[12:14].zfill(2)
            # col = year + '-' + month + '-' + day + ' ' + hour + ':' + min + ':' + sec
            # col = f"'{col}'"
            # return col
            else:
                col = f"'{col}'"
            return col
        else:
            return 'error'

    def get_date(self,col_data, type_com, lenth_bin, comments):
        col_data = (struct.unpack('>H', col_data[0:2])[0] << 8) + (struct.unpack('B', col_data[2:3]))[0] & 0xFFFFF
        year = str(col_data >> 9).zfill((4))
        month = str((col_data & 0x1FF) >> 5).zfill(2)
        day = str(col_data & 0x1F).zfill(2)
        col = year + '-' + month + '-' + day
        col = f"'{col}'"
        return col

    def get_time(self,col_data, type_com, lenth_bin, comments):
        col_data = (struct.unpack('>H', col_data[0:2])[0] << 8) + (struct.unpack('B', col_data[2:3]))[0] & 0x1FFFF
        hour = str(col_data >> 12).zfill(2)
        minute = str((col_data & 0xFFF) >> 6).zfill(2)
        sec = str(col_data & 0x3F).zfill(2)
        col = "'" + hour + ':' + minute + ':' + sec + "'"
        return col

    def get_timestamp(self,col_data, type_com, lenth_bin, comments):
        try:
            int_time = struct.unpack('>L', col_data)[0]
            timeArray = time.localtime(int_time)
            col = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
            col = f"'{col}'"
        except:
            dec = 0
            int_time = struct.unpack('>L', col_data[:4])[0]
            timeArray = time.localtime(int_time)
            col = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
            for i in range(len(col_data) - 4):
                dec_tmp = col_data[4 + i:5 + i]
                dec_tmp = struct.unpack('B', dec_tmp)[0]
                dec += dec_tmp << (3 - i - 1) * 8
            col = "'" + col + '.' + (str(dec).zfill((len(col_data) - 4) * 2)) + "'"
        return col

    def get_decimal(self,col_data, type_com, lenth_bin, comments):
        int_data = 0
        dec_data = 0
        sign = struct.unpack('B', col_data[:1])[0] >> 7
        int_len = comments["int_len"]
        dec_len = comments["dec_len"]
        dec_hex_len = comments["dec_hex_len"]
        if int_len <= 4:
            for i in range(int_len):
                value = struct.unpack('B', col_data[i:i + 1])[0]
                sign = struct.unpack('B', col_data[:1])[0] >> 7
                if sign == 0:
                    value = ~struct.unpack('b', col_data[i:i + 1])[0]
                a = value if i != 0 else (value & 0x7F)
                int_data += a << (8 * (int_len - i - 1))
        else:
            int_data = []
            temp = 0
            int_len_re = int_len % 4
            for k in range(int_len // 4 if int_len_re == 0 else int_len // 4 + 1):
                if k == 0:
                    for i in range(int_len_re if int_len_re != 0 else 4):
                        value = struct.unpack('B', col_data[i:i + 1])[0]
                        if sign == 0:
                            value = ~struct.unpack('b', col_data[i:i + 1])[0]
                        a = value if i != 0 else (value & 0x7F)
                        temp += a << (8 * ((int_len_re if int_len_re != 0 else 4) - i - 1))
                        int_data.append(str(temp))
                else:
                    off1 = int_len_re if int_len_re != 0 else 4
                    a = struct.unpack('>L', col_data[off1 + (k - 1) * 4:off1 + k * 4])[0]
                    if sign == 0:
                        a = ~struct.unpack('>l', col_data[off1 + (k - 1) * 4:off1 + k * 4])[0]
                    int_data.append(str(a))
            int_data = int(''.join(int_data))
        int_data = abs(int_data)
        if dec_len <= 4 and dec_hex_len > 0:
            for i in range(dec_len):
                # a = struct.unpack('B', col_data[int_len + i:int_len + i + 1])[0]
                a = int.from_bytes(col_data[int_len:int_len+dec_len],'big',signed=False)
                if sign == 0:
                    a = ~int.from_bytes(col_data[int_len:int_len+dec_len],'big',signed=True)
                dec_data = str(a).rjust(2,'0')
        else:
            dec_data = []
            temp = 0
            dec_len_re = dec_len % 4
            for k in range(dec_len // 4 if dec_len_re == 0 else dec_len // 4 + 1):
                if k == 0:
                    if dec_len_re != 0:
                        value = int.from_bytes(col_data[0-dec_len_re:],'big')
                        if sign == 0:
                            value = ~int.from_bytes(col_data[0 - dec_len_re:], 'big',signed=True)
                        value = str(value).rjust(2,'0')
                    else:
                        value = int.from_bytes(col_data[-4:],'big')
                        if sign == 0:
                            value = ~int.from_bytes(col_data[-4:], 'big',signed=True)
                        value = str(value).rjust(2, '0')
                    dec_data.append(value)
                else:
                    off1 = dec_len_re if dec_len_re != 0 else 4
                    a = struct.unpack('>L', col_data[-off1 - k * 4:-off1 - (k - 1) * 4])[0]
                    if sign == 0:
                        a = ~struct.unpack('>l', col_data[-off1 - k * 4:-off1 - (k - 1) * 4])[0]
                    a = str(a).rjust(2, '0')
                    dec_data.append(a)
            dec_data = ''.join(dec_data)
        if dec_len > 0:
            col = "'" + str(int_data) + '.' + dec_data + "'" if sign != 0 else "'" + '-' + str(
                int_data) + '.' + dec_data + "'"
        else:
            col = "'" + str(int_data) + "'" if sign != 0 else "'" + '-' + str(int_data) + "'"
        return col

    def get_double(self,col_data, type_com, lenth_bin, comments):
        try:
            a = type_com[type_com.index('(') + 1:type_com.index(')')]
            b = a.split(',')
            dec_len = int(b[1])
            col = f"'{round(struct.unpack('d', col_data)[0], dec_len)}'"
        except:
            col = "%.6f" % struct.unpack('d', col_data)[0]
        return col

    def get_float(self,col_data, type_com, lenth_bin, comments):
        try:
            a = type_com[type_com.index('(') + 1:type_com.index(')')]
            b = a.split(',')
            dec_len = int(b[1])
            col = f"'{round(struct.unpack('d', col_data)[0], dec_len)}'"
        except:
            col = "'%.2f'" % struct.unpack('f', col_data)[0]
        return col

    def get_text(self,col_data, type_com, lenth_bin, comments):
        col = ''
        lob_idx = struct.unpack('B', lenth_bin)[0] >> 4
        if lob_idx != 0x0c:
            try:
                col_tmp = col_data.decode(encoding='utf-8').rstrip().replace('\\', '\\\\').replace("'", "\\'")
                col_tmp = col_tmp.replace('\r', '\\r').replace('\n', '\\n')
                col = f"'{col_tmp}'"
            except:
                col_tmp = col_data.decode(encoding='GBK').encode(encoding='utf-8').decode(
                    encoding='utf-8').rstrip().replace('\\', '\\\\').replace("'", "\\'")
                col_tmp = col_tmp.replace('\r', '\\r').replace('\n', '\\n')
                col = f"'{col_tmp}'"
        else:
            col_pre = col_data[:-20]
            file_id = struct.unpack('>L', col_data[-20:-16])[0]
            page_no = struct.unpack('>L', col_data[-16:-12])[0]
            if page_no != 0:
                col = self.get_lob(file_id, page_no)
                col = col_pre + col
                try:
                    col_tmp = col.decode(encoding='utf-8').rstrip().replace('\\', '\\\\').replace("'", "\\'")
                    col_tmp = col_tmp.replace('\r', '\\r').replace('\n', '\\n')
                    col = f"'{col_tmp}'"
                except:
                    col_tmp = col.decode(encoding='GBK').encode(encoding='utf-8').decode(encoding='utf-8').rstrip().replace(
                        '\\', '\\\\').replace("'", "\\'")
                    col_tmp = col_tmp.replace('\r', '\\r').replace('\n', '\\n')
                    col = f"'{col_tmp}'"
            else:
                col = "''"
        return col

    def get_longtext(self,col_data, type_com, lenth_bin, comments):
        col = ''
        lob_idx = struct.unpack('B', lenth_bin)[0] >> 4
        if lob_idx != 0x0c:
            try:
                col_tmp = col_data.decode(encoding='utf-8').rstrip().replace('\\', '\\\\').replace("'", "\\'")
                col_tmp = col_tmp.replace('\r', '\\r').replace('\n', '\\n')
                col = f"'{col_tmp}'"
            except:
                col_tmp = col_data.decode(encoding='GBK').encode(encoding='utf-8').decode(
                    encoding='utf-8').rstrip().replace('\\', '\\\\').replace("'", "\\'")
                col_tmp = col_tmp.replace('\r', '\\r').replace('\n', '\\n')
                col = f"'{col_tmp}'"
        else:
            col_pre = col_data[:-20]
            file_id = struct.unpack('>L', col_data[-20:-16])[0]
            page_no = struct.unpack('>L', col_data[-16:-12])[0]
            if page_no != 0:
                col = self.get_lob(file_id, page_no)
                col = col_pre + col
                try:
                    col_tmp = col.decode(encoding='utf-8').rstrip().replace('\\', '\\\\').replace("'", "\\'")
                    col_tmp = col_tmp.replace('\r', '\\r').replace('\n', '\\n')
                    col = f"'{col_tmp}'"
                except:
                    col_tmp = col.decode(encoding='GBK').encode(encoding='utf-8').decode(encoding='utf-8').rstrip().replace(
                        '\\', '\\\\').replace("'", "\\'")
                    col_tmp = col_tmp.replace('\r', '\\r').replace('\n', '\\n')
                    col = f"'{col_tmp}'"
            else:
                col = "''"
        return col

    def get_blob(self,col_data, type_com, lenth_bin, comments):
        col = ''
        blob_idx = struct.unpack('B', lenth_bin)[0] >> 4
        if blob_idx == 0x0c:
            col_pre = col_data[:-20]
            file_id = struct.unpack('>L', col_data[-20:-16])[0]
            page_no = struct.unpack('>L', col_data[-16:-12])[0]
            if page_no != 0:
                col_tail = self.get_lob(file_id, page_no)
                col_data = col_pre + col_tail
                col = '0x' + binascii.b2a_hex(col_data).decode(encoding='utf-8').upper()
            else:
                col = '0x' + binascii.b2a_hex(col_pre).decode(encoding='utf-8').upper()
        elif len(col_data) == 0:
            col = "''"
        else:
            col = '0x' + binascii.b2a_hex(col_data).decode(encoding='utf-8').upper()
        return col


    def get_longblob(self,col_data, type_com, lenth_bin, comments):
        col = ''
        blob_idx = struct.unpack('B', lenth_bin)[0] >> 4
        if blob_idx == 0x0c:
            col_pre = col_data[:-20]
            file_id = struct.unpack('>L', col_data[-20:-16])[0]
            page_no = struct.unpack('>L', col_data[-16:-12])[0]
            if page_no != 0:
                col_tail = self.get_lob(file_id, page_no)
                col_data = col_pre + col_tail
                col = '0x' + binascii.b2a_hex(col_data).decode(encoding='utf-8').upper()
            else:
                col = '0x' + binascii.b2a_hex(col_pre).decode(encoding='utf-8').upper()
        elif len(col_data) == 0:
            col = "''"
        else:
            col = '0x' + binascii.b2a_hex(col_data).decode(encoding='utf-8').upper()
        return col

    def get_enum(self,col_data, type_com, lenth_bin, comments):
        enum_key = int.from_bytes(col_data, 'big')
        col = f"'{comments[enum_key]}'"
        return col

    def get_bit(self,col_data, type_com, lenth_bin, comments):
        col = bytes.hex(col_data)
        col = int(col)
        return str(col)

    def get_binary(self,col_data, type_com, lenth_bin, comments):
        col = '0x' + binascii.b2a_hex(col_data).decode(encoding='utf-8').upper()
        return col

    def get_varbinary(self,col_data, type_com, lenth_bin, comments):
        col = '0x' + binascii.b2a_hex(col_data).decode(encoding='utf-8').upper()
        return col


    def get_json(self,col_data, type_com, lenth_bin, comments):
        lob_idx = struct.unpack('B', lenth_bin)[0] >> 4
        if lob_idx != 0x0c:
            flag = int.from_bytes(col_data[:1],'little')
            if flag in [0,2]:
                col_data = col_data[1:]
                col = self.get_json_value(flag,col_data)
                if len(col) != 0:
                    col = str(col).replace("'",'"').replace(r'\n',r'\\n').replace(r'\t',r'\\t')
                    col = f"'{col}'"
                    col = rf'REPLACE({col}' + ",'" + r'\"' + "'" + ",'" + '"' + "')"
                else:
                    col = r"'{}'"
            else:
                col = False
        else:
            col_pre = col_data[:-20]
            file_id = struct.unpack('>L', col_data[-20:-16])[0]
            page_no = struct.unpack('>L', col_data[-16:-12])[0]
            if page_no != 0:
                col_tail = self.get_lob(file_id, page_no)
                col_data = col_pre + col_tail
                flag = int.from_bytes(col_data[:1], 'little')
                if flag in [0, 2]:
                    col_data = col_data[1:]
                    col = self.get_json_value(flag, col_data)
                    if len(col) != 0:
                        col = str(col).replace("'", '"').replace(r'\n', r'\\n').replace(r'\t', r'\\t')
                        col = f"'{col}'"
                        col = rf'REPLACE({col}' + ",'" + r'\"' + "'" + ",'" + '"' + "')"
                    else:
                        col = r"'{}'"
                else:
                    col = False
            else:
                col = r"'{}'"
        return col

    def get_json_value(self,type,col_data):
        key_num = int.from_bytes(col_data[:2],'little')
        value_type = ''
        value_info = bytearray()
        if type == 0:
            json_dict = {}
        elif type == 2:
            json_dict = []
        for i in range(key_num):
            if type == 0:
                key_info = col_data[4+i*4:4+(i+1)*4]
                key_off = int.from_bytes(key_info[:2],'little')
                key_len = int.from_bytes(key_info[2:4],'little')
                key = col_data[key_off:key_off+key_len].decode('utf8')
                value_info = col_data[4+key_num*4+i*3:4+key_num*4+(i+1)*3]
                value_type = int.from_bytes(value_info[:1],'little')
            elif type == 2:
                value_info = col_data[4+i*3:4+(i+1)*3]
                value_type = int.from_bytes(value_info[:1],'little')
            elif type == 0x0c:
                json_dict = col_data[2:].decode('utf8').replace('"',r'\"')
                continue
            value = ''
            if value_type in (4,5):
                value = int.from_bytes(value_info[1:3],'little')
            else:
                value_off = int.from_bytes(value_info[1:3],'little')
                if value_type == 7:
                    value = int.from_bytes(col_data[value_off:value_off+4],'little')
                elif value_type == 0x0a:
                    value = int.from_bytes(col_data[value_off:value_off + 8], 'little')
                elif value_type == 0x0c:
                    value_len = int.from_bytes(col_data[value_off:value_off + 1], 'little')
                    if value_len >> 7 == 0:
                        value = col_data[value_off + 1:value_off + 1 + value_len].decode('utf8').replace('"',r'\"')
                    else:
                        value = col_data[value_off + 2:value_off + 2 + value_len].decode('utf8').replace('"',r'\"')
                elif value_type in (0,2):
                    value = self.get_json_value(value_type,col_data[value_off:])
                elif value_type == 0x0b:
                    value = int.from_bytes(col_data[value_off:value_off + 8], 'big')
                    if value in json_type_0b.keys():
                        value = json_type_0b[value]
                    else:
                        value = ''
                else:
                    # print(f'{table_name}   {page_no} {f_record_off}  json err')
                    continue
            if type == 0:
                json_dict[key] = value
            elif type == 2:
                json_dict.append(value)
        return json_dict



    def get_lenth(self,col_type):
        lenth = 0
        comments = ''
        simple_type = col_type[:col_type.index('(')].lower() if col_type.count('(') > 0 else col_type.lower()
        width = col_type[col_type.index('(')+1:col_type.index(')')] if col_type.count('(') > 0 else '0'
        if simple_type == 'int':
            lenth = 4
        elif simple_type == 'tinyint':
            lenth = 1
        elif simple_type == 'smallint':
            lenth = 2
        elif simple_type == 'mediumint':
            lenth = 3
        elif simple_type == 'bigint':
            lenth = 8
        elif simple_type == 'float':
            lenth = 4
        elif simple_type == 'double':
            lenth = 8
        elif simple_type == 'decimal':
            comments = {}
            a = ''.join(i for i in col_type if i.isdigit() or i == ',')
            b = a.split(',')
            # int_len = int(b[0])                # for mysql5.0
            int_len = int(b[0]) - int(b[1])    # for higher than mysql5.0
            dec_len = int(b[1])
            if int_len == 7:
                int_len = 8
            cell_num = int_len // 9
            cell_last = int_len % 9
            max_cell_last = 0 if cell_last == 0 else int(''.join(str(9) for i in range(cell_last)))
            cell_last_hex_len = 0 if max_cell_last == 0 else len(hex(max_cell_last)[2:])
            cell_last_len = cell_last_hex_len // 2 if cell_last_hex_len % 2 == 0 else cell_last_hex_len // 2 + 1
            int_len = cell_last_len + cell_num * 4
            cell_num = dec_len // 9
            cell_last = dec_len % 9
            comments["dec_hex_len"] = dec_len
            max_cell_last = 0 if cell_last == 0 else int(''.join(str(9) for i in range(cell_last)))
            cell_last_hex_len = 0 if max_cell_last == 0 else len(hex(max_cell_last)[2:])
            cell_last_len = cell_last_hex_len // 2 if cell_last_hex_len % 2 == 0 else cell_last_hex_len // 2 + 1
            dec_len = cell_last_len + cell_num * 4
            lenth = int_len + dec_len
            comments["int_len"] = int_len
            comments["dec_len"] = dec_len
        elif simple_type == 'date':
            lenth = 3
        elif simple_type == 'time':
            lenth = 3
        elif simple_type == 'year':
            lenth = 1
        elif simple_type == 'datetime':
            int_time_len = 5                                          # 5.7及以上版本
            width = int(width)
            sec_decimal_len = width//2 if width%2==0 else width//2+1
            lenth = int_time_len + sec_decimal_len
            # lenth = 8                                                   # 5.6及以下版本
        elif simple_type == 'timestamp':
            lenth = 4
            if col_type.count('(') > 0:
                dec_len_temp = int(col_type[col_type.index('(')+1:col_type.index(')')])
                dec_len = dec_len_temp // 2 if dec_len_temp % 2 == 0 else (dec_len_temp // 2) + 1
                lenth = lenth + dec_len
        elif simple_type in ['char','varchar','text','tinytext','mediumtext','longtext','blob','longblob','varbinary']:
            comments = width
            lenth = 0
        elif simple_type == 'enum':
            comments = {}
            lenth = 1
            comments_list = list(width.replace("'","").split(','))
            comments_list.insert(0,'error')
            for i in range (len(comments_list)):
                comments[i] = comments_list[i]
        elif simple_type == 'bit':
            lenth = 1
        elif simple_type == 'binary':
            lenth = int(width)
        elif simple_type == 'json':
            lenth = 0
        return lenth,comments




    def get_info_dic(self,struct_str):
        info_dict = {}
        struct_str.seek(0)
        lines = struct_str.readlines()
        i = 0
        while True:
            if i < len(lines):
                if len(lines[i]) > 12:
                    if lines[i][:12] == 'CREATE TABLE':
                        primary_key_list = []
                        null_able_col = 0
                        table_info = {}
                        line_list = lines[i].split(' ')
                        a = line_list[2]
                        table_name = line_list[2].replace('`','').replace('(','').rstrip()
                        col_name = 0
                        primary_key = 0
                        null_able = 0
                        col_type = 0
                        col_id = 1
                        col_sum = 0
                        try:
                            while lines[i + 1][:12] != 'CREATE TABLE':
                                if lines[i+1][2:3] == '`':
                                    col_info = {}
                                    next_line_list = lines[i + 1].replace(', ', ',').split(' ')
                                    col_name = next_line_list[2].replace('`', '')
                                    col_type = next_line_list[3].rstrip('\n').rstrip(',')
                                    if col_type[:3] == 'dec':
                                        col_type = next_line_list[3] + next_line_list[4]
                                    lenth, comments = self.get_lenth(col_type)
                                    if 'NOT NULL' in lines[i + 1]:
                                        null_able = 0
                                    else:
                                        null_able = 1
                                        null_able_col += 1
                                    if 'unsigned' in lines[i + 1] or 'UNSIGNED' in lines[i + 1]:
                                        unsigned = 1
                                    else:
                                        unsigned = 0
                                    col_info["col_id"] = col_id
                                    col_info["col_type"] = f"get_{col_type[:col_type.index('(')].lower() if col_type.count('(') > 0 else col_type.lower()}"
                                    col_info["null_able"] = null_able
                                    col_info["lenth"] = lenth
                                    col_info["unsigned"] = unsigned
                                    col_info["comments"] = comments
                                    table_info[col_name] = col_info
                                    i += 1
                                    col_sum += 1
                                    # print(table_name, col_id, col_name, col_type)
                                elif lines[i + 1][2:13] == 'PRIMARY KEY':
                                    temp_list = lines[i + 1].split('`')
                                    for k in range(len(temp_list)//2):
                                        primary_key = temp_list[k*2+1]
                                        primary_key_list.append(primary_key)
                                        table_info["primary_key"] = primary_key_list
                                    i += 1
                                elif lines[i + 1][2:12] == 'UNIQUE KEY':
                                    if len(primary_key_list) == 0:
                                        temp_list = lines[i + 1].split('`')[2:]
                                        for k in range(len(temp_list)//2):
                                            primary_key = temp_list[k*2+1]
                                            primary_key_list.append(primary_key)
                                            table_info["primary_key"] = primary_key_list
                                    i += 1
                                else:
                                    i += 1
                                    break
                                col_id += 1
                            table_info["null_able_col"] = null_able_col
                            info_dict[table_name] = table_info
                        except:
                            continue
                    else:
                        i += 1
                else:
                    i += 1
            else:
                break
        return info_dict


    def process(self,struct_str, row_dir, page_file_dir, is_del,table_name_list,db_path):
        time_start = datetime.datetime.now()
        time_limit = time_start + datetime.timedelta(days=36500)
        global time_limit_str
        time_limit_str = datetime.datetime.strftime(time_limit,'%Y-%m-%d %H:%M:%S')
        # if is_del:
        #     tail_mark = '_del.sql'
        # else:
        #     tail_mark = '_data.sql'
        info_dic = self.get_info_dic(struct_str)
        if row_dir:
            list_all_name = table_name_list
        else:
            list_all_name = list(info_dic.keys())
        for table_name in list_all_name:
            print(table_name)
            show_table_num = 0
            break_flag = False
            if row_dir:
                if table_name in table_name_list:
                    table_info = info_dic[table_name]
            else:
                table_info = info_dic[table_name]
            try:
                primary_key = table_info['primary_key']
            except:
                primary_key = []
                pass
            page_file_path = os.path.join(page_file_dir, f"{table_name}.ibd")
            if os.path.exists(page_file_path):
                if row_dir:
                    row_file = open(rf'{row_dir}\{table_name}_data.sql', 'w', encoding='utf-8')
                else:
                    row_file = None
                global page_file
                page_file = open(page_file_path, 'rb')
                page_file_size = os.path.getsize(page_file_path)
                null_able_col = table_info['null_able_col']
                null_map_size = null_able_col // 8 if null_able_col % 8 == 0 else null_able_col // 8 + 1
                for i in range(3,page_file_size // 16384):
                    if break_flag:
                        break
                    page_file.seek(i * 16384)
                    page = page_file.read(16384)
                    page_no = struct.unpack('>L', page[4:8])[0]
                    page_type = struct.unpack('>H', page[24:26])[0]
                    idx_id = struct.unpack('>L', page[70:74])[0]
                    # if indx_id == '':
                    #     idx_id = ''
                    page_level = struct.unpack('>H', page[0x40:0x42])[0]
                    if page_type != 0 and page_level == 0 :
                        f_record_off1 = struct.unpack('>H', page[16374:16376])[0]
                        f_record_off_del = struct.unpack('>H', page[44:46])[0]
                        if is_del:
                            f_r_o = [f_record_off1,f_record_off_del]
                        else:
                            f_r_o = [f_record_off1]
                        n = 0
                        for f_record_off in (f_r_o):
                            if break_flag:
                                break
                            if f_record_off != 0:
                                try:
                                    next_record_off = struct.unpack('>h', page[f_record_off - 2:f_record_off])[0]
                                except:
                                    continue
                                if n == 1:
                                    next_record_off = f_record_off
                                    f_record_off = 0
                                while next_record_off != 0:
                                    try:
                                        row_dic = {}
                                        n = 0
                                        x_no = 0
                                        null_able_no = 0
                                        f_record_off = f_record_off + next_record_off
                                        if f_record_off != 112:
                                            try:
                                                record_top = struct.unpack('B', page[f_record_off - 5:f_record_off - 4])[0] >> 6
                                            except:
                                                break
                                            if record_top == 0:
                                                record_top_size = 5
                                            else:
                                                record_top_size = 6
                                            null_data = page[f_record_off - record_top_size - null_map_size:f_record_off - record_top_size]
                                            null_map = self.get_null_map(null_data)
                                            if len(primary_key) == 0:
                                                n = 6
                                            else:
                                                for primary in primary_key:
                                                    col_name = primary
                                                    col_info = table_info[col_name]
                                                    col_id = col_info['col_id']
                                                    type = col_info['col_type']
                                                    lenth = col_info['lenth']
                                                    global unsigned
                                                    unsigned = col_info['unsigned']
                                                    comments = col_info['comments']
                                                    if lenth != 0:
                                                        col_data = page[f_record_off + n:f_record_off + n + lenth]
                                                        col =getattr(self, type)(col_data, type, 1, comments)
                                                    else:
                                                        lenth_bin = page[
                                                                    f_record_off - record_top_size - null_map_size - x_no - 1:f_record_off - record_top_size - null_map_size - x_no]
                                                        lenth = struct.unpack('B', lenth_bin)[0]
                                                        col_data = page[f_record_off + n:f_record_off + n + lenth]
                                                        col = getattr(self, type)(col_data, type, lenth_bin, comments)
                                                        x_no += 1
                                                    n += lenth
                                                    row_dic[col_id] = col
                                                    # print(col)
                                            for col_name in list(table_info.keys()):
                                                if col_name not in primary_key and col_name != 'primary_key' and col_name != 'null_able_col':
                                                    col_info = table_info[col_name]
                                                    col_id = col_info['col_id']
                                                    type = col_info['col_type']
                                                    lenth = col_info['lenth']
                                                    is_null_able = col_info['null_able']
                                                    unsigned = col_info['unsigned']
                                                    comments = col_info['comments']
                                                    if is_null_able == 1:
                                                        is_null = null_map[null_able_no]
                                                        null_able_no += 1
                                                        if is_null == 0:
                                                            if lenth == 0:
                                                                lenth_bin = page[
                                                                            f_record_off - record_top_size - null_map_size - x_no - 1:f_record_off - record_top_size - null_map_size - x_no]
                                                                lenth_temp = struct.unpack('B', lenth_bin)[0]
                                                                if lenth_temp >> 7 != 1:
                                                                    lenth = lenth_temp
                                                                    x_no += 1
                                                                else:
                                                                    lenth_bin1 = page[
                                                                                 f_record_off - record_top_size - null_map_size - x_no - 2:f_record_off - record_top_size - null_map_size - x_no - 1]
                                                                    lenth_temp1 = struct.unpack('B', lenth_bin1)[0]
                                                                    lenth = ((lenth_temp & 0x7f) << 8) + lenth_temp1
                                                                    if type in ['get_blob', 'get_text', 'get_mediumtext','get_longtext', 'get_longblob']:
                                                                        lenth = ((lenth_temp & 0x3f) << 8) + lenth_temp1
                                                                    x_no += 2
                                                                if lenth == 0:
                                                                    col = "''"
                                                                else:
                                                                    col_data = page[f_record_off + n + 13:f_record_off + n + lenth + 13]
                                                                    col = getattr(self, type)(col_data, type, lenth_bin, comments)
                                                                n += lenth
                                                            else:
                                                                col_data = page[f_record_off + n + 13:f_record_off + n + lenth + 13]
                                                                col = getattr(self, type)(col_data, type, 1, comments)
                                                                n += lenth
                                                        else:
                                                            col = 'NULL'
                                                    else:
                                                        if lenth == 0:
                                                            lenth_bin = page[
                                                                        f_record_off - record_top_size - null_map_size - x_no - 1:f_record_off - record_top_size - null_map_size - x_no]
                                                            lenth_temp = struct.unpack('B', lenth_bin)[0]
                                                            if lenth_temp >> 7 != 1:
                                                                lenth = lenth_temp
                                                                x_no += 1
                                                            else:
                                                                lenth_bin1 = page[
                                                                             f_record_off - record_top_size - null_map_size - x_no - 2:f_record_off - record_top_size - null_map_size - x_no - 1]
                                                                lenth_temp1 = struct.unpack('B', lenth_bin1)[0]
                                                                lenth = ((lenth_temp & 0x7f) << 8) + lenth_temp1
                                                                if type in ['get_blob', 'get_text', 'get_mediumtext','get_longtext', 'get_longblob', 'get_json']:
                                                                    lenth = ((lenth_temp & 0x3f) << 8) + lenth_temp1
                                                                x_no += 2
                                                            col_data = page[f_record_off + n + 13:f_record_off + n + lenth + 13]
                                                            col = getattr(self, type)(col_data, type, lenth_bin, comments)
                                                            n += lenth
                                                        else:
                                                            col_data = page[f_record_off + n + 13:f_record_off + n + lenth + 13]
                                                            col = getattr(self, type)(col_data, type, 1, comments)
                                                            n += lenth
                                                    if col != 'error':
                                                        row_dic[col_id] = col
                                                    else:
                                                        # row_dic[col_id] = 'NULL'
                                                        row_dic = {}
                                                        break
                                            if row_dic != {}:
                                                row_new = []
                                                for l in range(len(row_dic)):
                                                    row_new.append(row_dic[l + 1])
                                                str_value = ','.join(row_new)
                                                str_value = 'insert into {a} values({b});\n'.format(a=table_name, b=str_value)
                                                s = re.compile('[\\x00-\\x08\\x0b-\\x0c\\x0e-\\x1f]').search(str_value)
                                                if s is None:
                                                    if row_file:
                                                        row_file.write(str_value)
                                                    else:
                                                        conn = sqlite3.connect(db_path)
                                                        cursor = conn.cursor()
                                                        # 执行SQL插入语句
                                                        try:
                                                            cursor.execute(str_value)
                                                            conn.commit()  # 提交事务
                                                            # print("sqlite数据插入成功")

                                                        except sqlite3.Error as e:
                                                            print("sqlite发生错误:", e)
                                                        finally:
                                                            # 关闭游标和连接
                                                            cursor.close()
                                                            conn.close()

                                                        show_table_num += 1
                                                        # print(show_table_num)
                                                        if show_table_num >= 100:
                                                            break_flag = True
                                                            break
                                        next_record_off = struct.unpack('>h', page[f_record_off - 2:f_record_off])[0]
                                    except:
                                        a =  page[f_record_off - 2:f_record_off]
                                        next_record_off = struct.unpack('>h', page[f_record_off - 2:f_record_off])[0]
                            n += 1
                    else:
                        continue
        end_time = datetime.datetime.now()
        print(end_time-time_start)
