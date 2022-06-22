# chain-pymysql: Easy to use pymysql.

# @link https://github.com/Tiacx/chain-pymysql
# @copyright Copyright (c) 2022 Tiac
# @license MIT
# @author Tiac
# @since 1.0

class RuntimeError(RuntimeError):

    def __init__(self, *args, **kwargs):
        code, message = args[0]
        self.code = code
        self.message = message

        super().__init__(*args, **kwargs)

    def get_code(self):
        return self.code

    def get_message(self):
        return self.message
