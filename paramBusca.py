import json
from datetime import datetime

# Pegar a UF e a data do usuário
uf = input("Digite a sigla da UF: ")
date = input("Digite a data (formato AAAA-MM-DD): ")

# Verificar a validade da UF
uf_validas = ['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO']
if uf not in uf_validas:
    print("UF inválida. Por favor, digite uma sigla de UF válida.")
    exit()

# Verificar a validade da data
try:
    datetime.strptime(date, '%Y-%m-%d')
except ValueError:
    print("Data inválida. Por favor, digite uma data no formato AAAA-MM-DD.")
    exit()

# Criar o dicionário com as informações
file_json = {
    'uf': uf,
    'date': date,
    'body': {
        'size': 10000,
        'track_total_hits': True,
        'query': {
            'bool': {
                'filter': [
                    {
                        'match': {
                            'estabelecimento_uf': uf
                        },
                    },
                    {
                        'match': {
                            'vacina_dataAplicacao': date
                        }
                    }
                ]
            }
        }
    }
}

# Salvar o JSON em um arquivo
file_path = './requests/requests.json'
with open(file_path, 'w') as file:
    json.dump(file_json, file)

print(f"Arquivo JSON salvo em: {file_path}")
