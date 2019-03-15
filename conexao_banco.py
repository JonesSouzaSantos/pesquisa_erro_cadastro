import cx_Oracle


def pararametros_conexao(host, usuario, senha, database, sgbd, cursor='n', fetchall='n', query=None):
    oracle_connection = cx_Oracle.connect(usuario,
                                          senha,
                                          '{host}/{database}'.format(host=host, database=database))
    if cursor == 'n':
        return oracle_connection
    elif cursor != 'n' and fetchall != 'n':
        cursor = oracle_connection.cursor()
        cursor.execute(query)
        return cursor.fetchall()
    else:
        cursor = oracle_connection.cursor()
        cursor.execute(query)
