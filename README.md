# Finatech.FipeCrawler

Crawler em Python para exportar a Tabela FIPE em CSV usando a API v2 (Parallelum/Deivid Fortuna).

API v2 Docs: https://deividfortuna.github.io/fipe/v2/

Suporta tipos de veículo: `carros`, `motos`, `caminhoes`.

## Requisitos

- Python 3.9+
- Pip (gerenciador de pacotes)

Instale as dependências:

```bash
pip install -r requirements.txt
```

## Configuração (.env)

Crie/edite o arquivo `.env` na raiz do projeto com:

```
TOKEN=SEU_TOKEN_AQUI
# opcional
REFERENCE=308
```

Observações:
- O `TOKEN` é usado como `X-Subscription-Token` nas requisições v2.
- Você pode informar `--token`/`--reference` via CLI; se presentes, eles têm precedência sobre o `.env`.

## Uso

Execute o script `fipe_crawler.py` escolhendo o tipo e o arquivo de saída:

```bash
python fipe_crawler.py --type carros --out fipe_carros.csv
```

Listar referências disponíveis (code,month):

```bash
python fipe_crawler.py --list-references
```

Opções:

- `--type` (obrigatório): `carros` | `motos` | `caminhoes`
- `--out` (obrigatório): caminho do CSV de saída
- `--timeout` (padrão 15): timeout por requisição (s)
- `--retries` (padrão 3): tentativas em falhas temporárias
- `--backoff` (padrão 0.5): fator de backoff exponencial entre tentativas
- `--rate-delay` (padrão 0.0): delay em segundos entre requisições (ex.: 0.1)
- `--max-brands`: limita quantidade de marcas (útil para testes)
- `--max-models`: limita quantidade de modelos por marca (útil para testes)
- `--workers` (padrão 1): número de requisições concorrentes (multithread)
- `--token`: cabeçalho X-Subscription-Token (obrigatório para v2 se aplicável)
- `--reference`: código do mês de referência (ex.: 308)

### Exemplos

- Carros (amostra rápida para teste):

```bash
python fipe_crawler.py --type carros --out fipe_carros_sample.csv --max-brands 2 --max-models 3 --rate-delay 0.1 --workers 1 --token SEU_TOKEN --reference 308
```

- Motos (com mais tolerância a falhas):

```bash
python fipe_crawler.py --type motos --out fipe_motos.csv --timeout 20 --retries 5 --workers 1 --token SEU_TOKEN
```

- Caminhões (com delay para evitar rate limit):

```bash
python fipe_crawler.py --type caminhoes --out fipe_caminhoes.csv --rate-delay 0.15 --workers 1 --token SEU_TOKEN
```

## Opções detalhadas

- `--type`: Tipo de veículo. Aceita `carros`, `motos`, `caminhoes` (mapeados para `cars`, `motorcycles`, `trucks` na API v2).
- `--out`: Caminho do CSV de saída.
- `--timeout`: Timeout por requisição.
- `--retries` e `--backoff`: Retentativas em erros temporários (429/5xx) com backoff exponencial.
- `--rate-delay`: Atraso entre requisições (ajuda a evitar rate limit).
- `--max-brands` / `--max-models`: Limites para testes rápidos.
- `--workers`: Concorrência (padrão 1). Aumente com cautela devido a limites diários.
- `--token`: Token v2 (se omitido, será lido de `TOKEN` no `.env`).
- `--reference`: Código da referência (se omitido, usa o mês atual; pode ser definido em `REFERENCE` no `.env`).
- `--list-references`: Lista todas as referências (imprime `code,month`) e encerra.

## Saída CSV

Colunas geradas:

```
tipo,codigo_marca,marca,codigo_modelo,modelo,codigo_ano,ano_modelo,combustivel,sigla_combustivel,codigo_fipe,mes_referencia,valor
```

Observação: `valor` vem no formato numérico sem o prefixo "R$"; trate a localidade conforme necessário.
Os campos mapeiam a resposta v2 (`brand`, `model`, `modelYear`, `fuel`, `fuelAcronym`, `codeFipe`, `referenceMonth`, `price`).

## Notas

- A API v2 usa `X-Subscription-Token` e possui limites (por ex., 500 req/dia). Informe `--token` (ou `.env`).
- Rodar o dataset completo pode levar tempo. Use `--max-brands` e `--max-models` para validar primeiro.
- Em caso de HTTP 429/5xx, o script tenta automaticamente novamente (`--retries` e `--backoff`).
- Concurrency: os preços por ano são buscados em paralelo. Por padrão `--workers=1` para respeitar limites; aumente com cautela.

## Troubleshooting

- __Unauthorized (401/403)__: verifique `TOKEN` (CLI ou `.env`). Tokens inválidos ou ausentes causam erro.
- __Too Many Requests (429)__: reduza `--workers`, aumente `--rate-delay` (ex.: 0.1–0.5), aumente `--backoff` e/ou `--retries`.
- __Timeouts__: aumente `--timeout` (ex.: 30) e `--retries`.
- __Sem linhas no CSV__: confira se há dados para a `--reference` usada; tente listar com `--list-references` e ajustar.

## Fluxo com curl (informativo)

Você pode explorar a API FIPE (Parallelum) com `curl` para entender o fluxo antes de usar o script. Substitua os placeholders `{...}` pelos códigos obtidos na etapa anterior.

- Listar marcas (ex.: carros):

```bash
curl -s "https://fipe.parallelum.com.br/api/v2/cars/brands?reference=308" -H "X-Subscription-Token: SEU_TOKEN"
```

- Listar modelos de uma marca:

```bash
curl -s "https://fipe.parallelum.com.br/api/v2/cars/brands/{codigoMarca}/models?reference=308" -H "X-Subscription-Token: SEU_TOKEN"
```

- Listar anos de um modelo:

```bash
curl -s "https://fipe.parallelum.com.br/api/v2/cars/brands/{codigoMarca}/models/{codigoModelo}/years?reference=308" -H "X-Subscription-Token: SEU_TOKEN"
```

- Obter preço/detalhes para um ano específico:

```bash
curl -s "https://fipe.parallelum.com.br/api/v2/cars/brands/{codigoMarca}/models/{codigoModelo}/years/{codigoAno}?reference=308" -H "X-Subscription-Token: SEU_TOKEN"
```

Notas:
- Para motos e caminhões, substitua `cars` por `motorcycles` ou `trucks` na URL.
- Em Windows/PowerShell, `curl` pode ser um alias de `Invoke-WebRequest`. Se preferir, use `curl.exe` explicitamente.
- A resposta é JSON; para visualizar melhor, você pode usar utilitários como `jq` (Linux/macOS) ou formatadores online.

## Licença

Uso interno Finatech. Ajuste conforme a política da sua organização.