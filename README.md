# desafio-cognitivo

Esse repositório contem a resolução do desafio técnico da cognitivo

## Descrição do Desafio

### Requisitos

1. Conversão do formato dos arquivos: Converter o arquivo CSV presente no diretório data/input/users/load.csv, para um formato colunar de alta performance de leitura de sua escolha. Justificar brevemente a escolha do formato;

2. Deduplicação dos dados convertidos: No conjunto de dados convertidos haverão múltiplas entradas para um mesmo registro, variando apenas os valores de alguns dos campos entre elas. Será necessário realizar um processo de deduplicação destes dados, a fim de apenas manter a última entrada de cada registro, usando como referência o id para identificação dos registros duplicados e a data de atualização (update_date) para definição do registro mais recente;

3. Conversão do tipo dos dados deduplicados: No diretório config haverá um arquivo JSON de configuração (types_mapping.json), contendo os nomes dos campos e os respectivos tipos desejados de output. Utilizando esse arquivo como input, realizar um processo de conversão dos tipos dos campos descritos, no conjunto de dados deduplicados;

### Notas gerais

- Todas as operações devem ser realizadas utilizando Spark. O serviço de execução fica a seu critério, podendo utilizar tanto serviços locais como serviços em cloud. Justificar brevemente o serviço escolhido (EMR, Glue, Zeppelin, etc.).

- Cada operação deve ser realizada no dataframe resultante do passo anterior, podendo ser persistido e carregado em diferentes conjuntos de arquivos após cada etapa ou executados em memória e apenas persistido após operação final.

- Você tem liberdade p/ seguir a sequência de execução desejada;

- Solicitamos a transformação de tipos de dados apenas de alguns campos. Os outros ficam a seu critério

- O arquivo ou o conjunto de arquivos finais devem ser compactados e enviados por e-mail.

### Mapeamento de Tipos

```json
{
    "age": "integer",
    "create_date": "timestamp",
    "update_date": "timestamp"
}
```

## Estrutura do Projeto

### ETL

Na pasta app se encontra a ETL que processa os dados segundo as especificações.

Para executar o projeto na pasta base deste repositório execute

> Os arquivos na pasta `data/output/users` antes de executar o projeto.

``` shell
docker build app -t cognitivo
docker run -v $PWD/data:/app/data -it cognitivo
```

### Análise da escolha do formato

Me chamou atenção a decisão consciente do formato de saída. Estou habituado a utilizar `parquet` e aproveitei a oportunidade de explorar as opções que o `pyspark` nos permite usar.

Além do `parquet`, podemos usar também o formato `orc`. Após uma breve pesquisa encontrei sugestões a favor do `orc`:

- [Difference Between ORC and Parquet](http://www.differencebetween.net/technology/difference-between-orc-and-parquet/)
- [Demystify Hadoop Data Formats: Avro, ORC, and Parquet](https://towardsdatascience.com/demystify-hadoop-data-formats-avro-orc-and-parquet-e428709cf3bb)

Mas não fiquei satisfeito apenas com a pesquisa e decidi fazer um benchmark. Utilizando `orc` e `parquet` nas compressões que já disponíveis na imagem docker que são

```json
{
    "parquet": [
        "none",
        "uncompressed",
        "snappy",
        "gzip"
    ],
    "orc": [
        "none",
        "snappy",
        "zlib"
    ]
}
```

Foram criados arquivos de 300K linhas assim podemos observar melhor as variações. Cada linha é lida 30 vezes tomando o cuidado de forçar a leitura executando um `collect()` e limpar o cache após a leitura.

Os dados do experimento estão em `data/experiment/`. Os arquivos `users_<tipo>_<compressão>` são os arquivos sample utilizados no experimento, `result` contém o tempo de cada execução de cada tipo e compressão e `report` contém a tabela que baseou a decisão.

A criação do `report` envolveu uma breve limpeza, pois a primeira execução, por exemplo é mais custosa que as demais, então deveria ser levado em conta, então para limpar essa e demais pontos fora da curva ignoramos todas as execução fora do intervalo de 3 desvios padrões acima ou abaixo.

O resultado do experimento resultou na tabela

type|compression|time_elapsed|standard_deviation
---|---|---|---
orc|none|8.77851463953654|0.8402440211238561
orc|snappy|8.85045755704244|1.0884556388199642
orc|zlib|8.968025716145833|0.48404335095887663
parquet|uncompressed|9.180603849476782|0.8456225759714966
parquet|none|9.393999625896585|1.2307322471812945
parquet|gzip|9.412825647989909|1.0853285779483361
parquet|snappy|10.099961789449056|1.282639834215839

Aqui já é possível perceber uma melhora pequena do `orc` sobre o `parquet`, mas por causa da alta variância considero um empate entre as compressões. Usando então o tamanho do arquivo final como critério de desempate, decidi pelo `zlib`, que é a forma mais eficiente de compressão.

Para executar esse a parte do projeto execute na base do repositório

> Delete os arquivos da pasta `data/experiment` antes de executar o experimento

``` shell
docker build experiment -t exp
docker run -v $PWD/data:/app/data -it exp
```
