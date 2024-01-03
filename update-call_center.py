import csv
import mysql.connector
import os
from dotenv import load_dotenv
from mysql.connector import Error

load_dotenv()

db_host = os.getenv("DB_HOST")
db_database = os.getenv("DB_DATABASE")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

try:
    # Estabelece a conexão
    con = mysql.connector.connect(host='{db_host}', database='{db_database}', user='{db_user}', password='{db_password}')

    # Cria o cursor
    cursor = con.cursor()

    # Caminho para o arquivo CSV no seu PC
    arquivo_csv = 'NaoTabuladoSacSemear30122023.csv'

    # Abre o arquivo CSV e lê os dados
    with open(arquivo_csv, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        
        # Pula o cabeçalho se houver
        next(csv_reader, None)

        # Itera sobre as linhas do arquivo CSV
        for row in csv_reader:
            protocolo, tabulacao, descricao = row
            update_sql = f"UPDATE hist_cliente SET tabulacao = '{tabulacao}', descricao = '{descricao}' WHERE protocolo = '{protocolo}'"
            update_sql2 = f"UPDATE registro_cliente SET tabulacao = '{tabulacao}', descricao = '{descricao}' WHERE protocolo = '{protocolo}'"
            
            
            try:
                cursor.execute(update_sql)
                cursor.execute(update_sql2)
                con.commit()
                print(f"Update para o protocolo {protocolo} executado com sucesso")
            except mysql.connector.Error as update_error:
                # Tratamento de erro específico para MySQL durante o update
                print(f"Erro ao atualizar protocolo {protocolo}: {update_error}")

except mysql.connector.Error as e:
    # Tratamento de erro específico para MySQL
    print("Erro ao acessar tabela MySQL:", e)

finally:
    # Garante o fechamento do cursor e da conexão
    if con.is_connected():
        cursor.close()
        con.close()
        print("Conexao ao MySQL encerrada")
