import argparse
import boto3
import time

from botocore.exceptions import ClientError
from pprint import pprint


parser = argparse.ArgumentParser()
parser.add_argument('--source-key', required=True, help="AWS Access Key for the origin account")
parser.add_argument('--source-secret', required=True, help="AWS Secret Access Key for the origin account")
parser.add_argument('--source-role', required=True, help="Role from the origin account")
parser.add_argument('--dest-key', required=True, help="AWS Access Key for the target account")
parser.add_argument('--dest-secret', required=True, help="AWS Secret Access Key for the target account")
parser.add_argument('--dest-role', required=True, help="Role for the target account")
parser.add_argument('--table-name', required=True, help="DynamoDB table's name")
parser.add_argument('--create-table', type=bool, default=False, help="If the table need to be created")
parser.add_argument('--dest-table', default=None, help="If the target table must have another name, provide it here")
parser.add_argument('--tags', default='', help="If you need to include tags. Key-value comma separated: Ex: environment=dev,project=my-awesome-project")

args = parser.parse_args()

TABELA = args.table_name
CRIAR_TABELA = args.create_table

if args.dest_table:
    DEST_TABELA = args.dest_table
else:
    DEST_TABELA = TABELA


print(f'Tabela origem: {TABELA}')
print(f'Tabela destino: {DEST_TABELA}')

REGIAO = 'sa-east-1'
TAGS = {}

if args.tags:
    
    for tag in args.tags.split(','):
        print(f'processando tag: {tag}')
        try:
            t = tag.split('=')
            TAGS[t[0]] = t[1]
        except Exception:
            print(f'Nao foi possivel obter a tag especificada: {tag}')

    print(f'utilizando as tags informadas: {TAGS}')

origem = {
    'key': args.source_key,
    'secret': args.source_secret,
    'role': args.source_role
}

dest = {
    'key': args.dest_key,
    'secret': args.dest_secret,
    'role': args.dest_role,
}


def dynamo_client(ambiente):
    role = assume_role(ambiente)

    return boto3.client(
        'dynamodb',
        region_name='sa-east-1',
        aws_access_key_id=role['Credentials']['AccessKeyId'],
        aws_secret_access_key=role['Credentials']['SecretAccessKey'],
        aws_session_token=role['Credentials']['SessionToken'])


def assume_role(ambiente):
    sts = boto3.client(
        'sts',
        region_name='sa-east-1',
        aws_access_key_id=ambiente['key'],
        aws_secret_access_key=ambiente['secret'])
        
    return sts.assume_role(
        RoleArn=ambiente['role'],
        RoleSessionName='assumeRole',
        DurationSeconds=3600)


def criar_tabela(descricao_tabela, dynamodb):
    pprint(f'criando tabela {DEST_TABELA}')

    try:
        attrs = []
        chaves = []
        
        for k in descricao_tabela['KeySchema']:
            chaves.append(k['AttributeName'])
        
        for attr in descricao_tabela['AttributeDefinitions']:
            if attr['AttributeName'] in chaves:
                attrs.append(attr)
        
        
        info = {
            'TableName': DEST_TABELA,
            'KeySchema': descricao_tabela['KeySchema'],
            'AttributeDefinitions': attrs,
            'BillingMode': 'PAY_PER_REQUEST'
        }



        if TAGS:
            info['Tags'] = []
            for k, v in TAGS.items():
                info['Tags'].append({
                    'Key': k,
                    'Value': v
                })

        table = dynamodb.create_table(**info)
        time.sleep(15)
    except ClientError as c:
        print(f'a tabela {DEST_TABELA} já existia')
        raise Exception()


def adicionar_itens(itens, dynamodb):
    pprint(f'adicionando {itens}')
    dynamodb.batch_write_item(
        RequestItems = {
            DEST_TABELA: itens
        }
    )


def scan_tabela_origem(dynamodb):
    response = dynamodb.scan(
        TableName=TABELA,
        Select='ALL_ATTRIBUTES')
    data = response['Items']

    print(f'Lendo {len(data)} itens da tabela {TABELA}')

    while 'LastEvaluatedKey' in response:
        response = dynamodb.scan(
            TableName=TABELA,
            Select='ALL_ATTRIBUTES',
            ExclusiveStartKey=response['LastEvaluatedKey'])

        data.extend(response['Items'])
        print(f'Lendo {len(data)} itens da tabela {TABELA}')

    return data


if __name__ == '__main__':
    dynamodb_origem = dynamo_client(origem)
    dynamodb_dest = dynamo_client(dest)

    if CRIAR_TABELA:
        table = dynamodb_origem.describe_table(
            TableName=TABELA)

        criar_tabela(table['Table'], dynamodb_dest)

    data = scan_tabela_origem(dynamodb_origem)
    itens_para_adicionar = []

    for item in data:
        itens_para_adicionar.append({
            'PutRequest': {
                'Item': item
            }
        })

    for i in range(0, len(itens_para_adicionar), 25):
        adicionar_itens(itens_para_adicionar[i:i+25], dynamodb_dest)
