class Singleton:
    _instance = None
    d_co = None
    my_conn = None
    # large_fields_tablename  = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Singleton, cls).__new__(cls)
            # 可以在这里初始化其他必要的变量
        return cls._instance