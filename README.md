# Projeto da Disciplina de Engenharia de Dados - UFRN

## Descrição
O projeto consiste em um fluxo de trabalho desenvolvido utilizando as tecnologias Python e Ariflow. O fluxo monitora uma pasta que receberá o arquivo com parametros de busca e realizará a consulta em uma API. A consulta resultarar em um ou mais arquivos `json` dos quais serão convertidos em um CSVs, e apartir desse serão gerados mais dois (informações de doses e informações do sexo por município).

## Estrutura de Pastas
`dags/`: Contém o código do fluxo\
`requests/`: Pasta monitorada que conterá o arquivo com parametros de busca\
`data/`: Local em que será salvo as tabelas CSV

## Execução
1. Inicialização do container:\
   `Docker compose up`
2. Acesse no navegador `localhost:8080`:\
   `Username: airflow`\
   `Password: airflow`
3. Adicione uma conexão em `Admin -> Connections -> +`\
   `Connection Id: fs_default`\
   `Connection Type: File (path)`\
   `Host: /opt/airflow/requests/requests.json`
4. O fluxo de tasks está pronto para ser ativo.
5. Depois de ativo, basta executar o script `paramBusca.py` e fornecer a sigla do estado e a data alvo da consulta.

## Tecnologias
- ``Python``
- ``Airflow``

## Sobre a API
É uma API de dados abertos que reúne informações da vacinação de covid-19 disponibilizada pelo ministério da saúde, por meio do SI-PNI, e-SUS APS e dos sistemas próprios de estados e municípios. Os dados disponíveis no OpenDatasus englobam o número de doses aplicadas, por UF e municípios, por um determinado período, por gênero, por faixa etária e por tipo de vacina.

Mais informações em: https://opendatasus.saude.gov.br/dataset/covid-19-vacinacao
