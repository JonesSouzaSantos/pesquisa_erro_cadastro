from conexao_banco import pararametros_conexao
import pandas as pd

host = ''
usuario = ''
senha = ''
database = ''
sgbd = ''


nome_tabelas = pd.read_sql('''SELECT table_name
            FROM ALL_TABLES
            where owner = 'DBAMV'
            and num_rows > 0
            order by num_rows''', con=oracle_connection)


def extrair_colunas_defeituosas(nome_tabela, todas_colunas, colunas_date):
    colunas_to_char = []
    df = ''
    for a in colunas_date:
        todas_colunas.remove(a)
        limpar = str(a).replace('[', '').replace(']', '').replace("'", '')
        colunas_to_char.append("""to_char({coluna}, 'dd/mm/yyyy') {coluna}""".format(coluna=limpar))
    contador = 0
    for b, c in zip(colunas_date, colunas_to_char):
        contador += 1
        tratamento_01 = str(todas_colunas).replace('[', '').replace(']', '').replace("'", '')
        tratamento_02 = str(c).replace('[', '').replace(']', '').replace('"', '')
        tratamento_03 = str(b).replace('[', '').replace(']', '').replace('"', '')

        consulta = """
                    select {to_char_datas}, {demais}
                    from dbamv.{tabela}
                    where
                        cast(substr(to_char({datas}, 'dd/mm/yyyy'), 7, 4) as int) <= 1900
                        or cast(substr(to_char({datas}, 'dd/mm/yyyy'), 7, 4) as int) > 2019""".format(to_char_datas=tratamento_02,
                                                                                datas=tratamento_03,
                                                                                demais=tratamento_01,
                                                                                tabela=nome_tabela)
        # df = consulta
        df = pd.read_sql(consulta, con=oracle_connection)

        arquivo_saida = '{tabela}_{nr}.csv'.format(tabela=nome_tabela, nr=contador)
        if len(df) > 1:
            try:
                pd.to_datetime(df[str(tratamento_03)], format='%d/%m/%Y')
            except:
                print(nome_tabela, tratamento_03)
                df.to_csv(arquivo_saida, sep='|')
        else:
            # print(nome_tabela, tratamento_03)
            pass


def analisar_coluna(nome_tabela):
    nome_tabela = str(nome_tabela).replace('[', '').replace(']', '').replace("'", '')
    query = pd.read_sql('select * from dbamv.{tabela} where rownum <=1'.format(tabela=nome_tabela), con=oracle_connection)
    todas_colunas = query.columns.values.tolist()
    colunas_date = query.select_dtypes(include=['datetime64[ns]']).columns.values.tolist()
    if len(colunas_date) < 1:
        pass
    else:
        extrair_colunas_defeituosas(nome_tabela=nome_tabela, todas_colunas=todas_colunas, colunas_date=colunas_date)


for a in nome_tabelas.values:
    analisar_coluna(a)
