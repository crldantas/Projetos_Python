import boto3
import pandas as pd
from io import BytesIO
import snowflake.connector as sf
from snowflake.connector.pandas_tools import write_pandas
import unidecode
import os
import access

def processar_arquivo_sfc(s3, bucket_name, prefix, arquivo_contem, tabela_snowflake, procedimento_armazenado):
    # Lista de arquivos .xlsx para processar
    arquivos_a_processar = []

    # Listar objetos no S3
    objects = s3.list_objects(Bucket=bucket_name, Prefix=prefix)

    # Verificar se há arquivos no S3
    if 'Contents' not in objects or not objects['Contents']:
        print(f'Nenhum arquivo encontrado no S3 com prefixo {prefix}')
        return


    # Iterar sobre os objetos encontrados
    for obj in objects.get('Contents', []):
        key = obj["Key"]
        # Verificar se é um arquivo .xlsx e contém o nome específico
        if key.endswith('.xlsx') and arquivo_contem in key:
            arquivos_a_processar.append(key)

    # Verificar se há arquivos a serem processados
    if not arquivos_a_processar:
        print(f'Nenhum arquivo encontrado no S3 com prefixo {prefix} e contendo {arquivo_contem}')
        return        

    # Processar cada arquivo .xlsx
    for key in arquivos_a_processar:
        try:
            # Ler o arquivo .xlsx usando pandas
            response = s3.get_object(Bucket=bucket_name, Key=key)
            excel_data = pd.read_excel(BytesIO(response['Body'].read()), engine='openpyxl')

            # Remover espaços, caracteres especiais, acentos e converter para maiúsculas nos nomes das colunas
            excel_data.columns = [unidecode.unidecode(col).upper().replace(" ", "_").replace(".", "_").replace("-", "_").replace(":", "") for col in excel_data.columns]

            # Adicionar a coluna NOME_ARQUIVO com o nome do arquivo
            excel_data['NOME_ARQUIVO'] = os.path.basename(key)

            # Iterar sobre as colunas e converter para o formato de data desejado
            for col in excel_data.columns:
                if 'DATA' in col or 'PREVISAO' in col:  # Ajustar conforme necessário
                    excel_data[col] = pd.to_datetime(excel_data[col], errors='coerce').dt.strftime('%Y-%m-%d')

            # Configuração do Snowflake
            snowflake_connection = {
                'account': access.ACCOUNT_SF,
                'user': access.USER_SF,
                'password': access.PWD_SF,
                'warehouse': 'WHDEV',
                'database': 'BCARE',
                'schema': 'TRANSIENT',
            }

            # Configuração da conexão Snowflake
            connSF = sf.connect(**snowflake_connection)
            sfq = connSF.cursor()

            # Montar a string para criar a tabela
            columns_definition = ', '.join([f'{col} STRING' for col in excel_data.columns])

            create_table = f"""
            CREATE OR REPLACE TABLE BCARE.TRANSIENT.{tabela_snowflake} (
            {columns_definition}
            );
            """

            sfq.execute(create_table)
            #print(create_table)

            print(f"TABELA STAGE CRIADA: {tabela_snowflake}")         

            #print(excel_data)

            # Carregar dados no Snowflake apenas se houver dados
            if not excel_data.empty:
                success, num_chunks, num_rows, output = write_pandas(
                    conn=connSF,
                    df=excel_data,
                    table_name=tabela_snowflake,
                    database='BCARE',
                    schema='TRANSIENT',
                )

                # Executar procedimento armazenado apenas se houver sucesso no carregamento
                if success:
                    proc = f"CALL BCARE.TRANSIENT.{procedimento_armazenado}();"
                    sfq.execute("USE WAREHOUSE WHDEV;")
                    sfq.execute(proc)

                    print(f'Arquivo {key} processado e excluído do S3')
                    # Excluir o arquivo .xlsx original do S3
                    s3.delete_object(Bucket=bucket_name, Key=key)
                else:
                    print(f'Erro ao carregar dados do arquivo {key} no Snowflake')
            else:
                print(f'Nenhum dado para carregar do arquivo {key}')

        except Exception as err:
            print(f'Erro ao processar o arquivo {key}: {err}')

# Substitua 'seu_bucket' pelo nome do seu bucket S3
#bucket_name = "funcional-datalake-zone-landing"

# Crie uma instância do cliente S3
s3 = boto3.client('s3', aws_access_key_id=access.aws_access_key_id,
                  aws_secret_access_key=access.aws_secret_access_key)

# Consulta para obter parâmetros
query = """
SELECT BUCKET_NAME, PREFIX, ARQUIVO_CONTEM, TABELA_SNOWFLAKE, PROCEDIMENTO_ARMAZENADO  
FROM MONITORAMENTO.PARAMETROS.CONFIG_ARQUIVOS_S3 
WHERE ATIVO = 'YES';
"""

# Configuração do Snowflake para consulta
snowflake_connection_query = {
    'account': access.ACCOUNT_SF,
    'user': access.USER_SF,
    'password': access.PWD_SF,
    'warehouse': 'WHDEV',
    'database': 'MONITORAMENTO',
    'schema': 'PARAMETROS',
}

# Conectar ao Snowflake
connSF_query = sf.connect(**snowflake_connection_query)
sfq_query = connSF_query.cursor()

# Executar a consulta
sfq_query.execute(query)
results = sfq_query.fetchall()

# Iterar sobre os resultados e chamar a função para cada conjunto de parâmetros
for result in results:
    processar_arquivo_sfc(
        s3,
        result[0],  # BUCKET_NAME
        result[1],  # PREFIX
        result[2],  # ARQUIVO_CONTEM
        result[3],  # TABELA_SNOWFLAKE
        result[4],  # PROCEDIMENTO_ARMAZENADO
       
    )
