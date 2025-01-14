import json
import os
import struct
import zlib
import io


class IbdToSql:
    def __init__(self):
        pass
    def get_sdi_info(self,dir):
        info = {}
        struct_file = io.StringIO()
        name_list = os.listdir(dir)
        for id in name_list:
            sql_list = []
            path = os.path.join(dir, id)
            if os.path.isfile(path):
                info['path'] = path
                try:
                    file = open(path, 'rb')
                    file.seek(3 * 16384)
                    sdi_page = file.read(16384)
                    f_record_off = struct.unpack('>H', sdi_page[16374:16376])[0]
                    next_record_off = struct.unpack('>h', sdi_page[f_record_off - 2:f_record_off])[0]
                    f_record_off = f_record_off + next_record_off
                    while f_record_off != 112:
                        sdi_info = sdi_page[f_record_off + 29:]
                        sdi_len = struct.unpack('>L', sdi_info[:4])[0]
                        sdi = zlib.decompress(sdi_info[4:sdi_len + 4]).decode('utf-8')
                        next_record_off = struct.unpack('>h', sdi_page[f_record_off - 2:f_record_off])[0]
                        f_record_off = f_record_off + next_record_off
                        sdi = json.loads(sdi)
                        if sdi['dd_object_type'] == 'Table':
                            table_name = sdi['dd_object']['name']
                            new_path = os.path.join(dir, table_name+ '.ibd')
                            info['name'] = table_name

                            if 'columns' in sdi['dd_object'].keys():
                                pri_key = ''
                                for column_dict in sdi['dd_object']['columns']:
                                    col_name = column_dict['name']
                                    if col_name not in ['DB_TRX_ID', 'DB_ROLL_PTR']:
                                        is_nullable = 'NOT NULL' if not column_dict['is_nullable'] else 'NULL'
                                        is_zerofill = column_dict['is_zerofill']
                                        is_unsigned = '' if not column_dict['is_unsigned'] else 'unsigned'
                                        char_length = column_dict['char_length']
                                        comment = '' if column_dict['comment'] == '' else "COMMENT '%s'" % (
                                            column_dict['comment'].replace("'", "`"))
                                        column_type = column_dict['column_type_utf8']
                                        if column_type[:3] == 'bit':
                                            default = '' if column_dict['default_value_utf8'] == '' else 'DEFAULT %s' % \
                                                                                                         column_dict[
                                                                                                             'default_value_utf8']
                                        else:
                                            default = '' if column_dict['default_value_utf8'] == '' else 'DEFAULT "%s"' % \
                                                                                                         column_dict[
                                                                                                             'default_value_utf8']
                                        column_key = column_dict['column_key']  # column_key=2时，表示为主键
                                        if column_key == 2:
                                            pri_key = '  PRIMARY KEY (`%s`) USING BTREE' % col_name

                                        a = ("  `%s` %s %s %s %s,\n" % (
                                        col_name, column_type, is_nullable, default, comment)).replace("''", "")
                                        sql_list.append(a)
                                # 首先，合并sql_list成一个字符串，并确保CURRENT_TIMESTAMP格式正确
                                sql_str = ''.join(sql_list)
                                sql_str = sql_str.replace("'CURRENT_TIMESTAMP'",
                                                          'CURRENT_TIMESTAMP')  # 替换单引号包围的CURRENT_TIMESTAMP
                                sql_str = sql_str.replace('"CURRENT_TIMESTAMP"',
                                                          'CURRENT_TIMESTAMP')  # 替换双引号包围的CURRENT_TIMESTAMP
                                # 构造完整的CREATE TABLE语句
                                a = f'DROP TABLE IF EXISTS `{table_name}`;\nCREATE TABLE `{table_name}`  (\n{sql_str}{pri_key}\n);\n'
                                struct_file.write(a)
                        else:
                            table_info = sdi['dd_object']['name']
                            data_base_name = table_info[:table_info.index('/')]
                            info['data_base_name'] = data_base_name
                    file.close()
                    os.renames(path, new_path)

                except Exception as a:
                    print('get_sdi_info:',a)
        return struct_file