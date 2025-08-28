# Finatech.FipeCrawler

Crawler em Python para exportar a Tabela FIPE em CSV usando a API pública Parallelum.

API: https://parallelum.com.br/fipe/api/v1

Suporta tipos de veículo: `carros`, `motos`, `caminhoes`.

## Requisitos

- Python 3.9+
- Pip (gerenciador de pacotes)

Instale as dependências:

```bash
pip install -r requirements.txt
```

## Uso

Execute o script `fipe_crawler.py` escolhendo o tipo e o arquivo de saída:

```bash
python fipe_crawler.py --type carros --out fipe_carros.csv
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

### Exemplos

- Carros (amostra rápida para teste):

```bash
python fipe_crawler.py --type carros --out fipe_carros_sample.csv --max-brands 2 --max-models 3 --rate-delay 0.1
```

- Motos (com mais tolerância a falhas):

```bash
python fipe_crawler.py --type motos --out fipe_motos.csv --timeout 20 --retries 5
```

- Caminhões (com delay para evitar rate limit):

```bash
python fipe_crawler.py --type caminhoes --out fipe_caminhoes.csv --rate-delay 0.15
```

## Saída CSV

Colunas geradas:

```
tipo,codigo_marca,marca,codigo_modelo,modelo,codigo_ano,ano_modelo,combustivel,sigla_combustivel,codigo_fipe,mes_referencia,valor
```

Observação: `valor` vem no formato numérico sem o prefixo "R$"; trate a localidade conforme necessário.

## Notas

- A API Parallelum é pública e sem autenticação; ainda assim, respeite limites de taxa usando `--rate-delay`.
- Rodar o dataset completo pode levar tempo. Use `--max-brands` e `--max-models` para validar primeiro.
- Em caso de HTTP 429/5xx, o script tenta automaticamente novamente (`--retries` e `--backoff`).

## Fluxo com curl (informativo)

Você pode explorar a API FIPE (Parallelum) com `curl` para entender o fluxo antes de usar o script. Substitua os placeholders `{...}` pelos códigos obtidos na etapa anterior.

- Listar marcas (ex.: carros):

```bash
curl -s "https://parallelum.com.br/fipe/api/v1/carros/marcas"
```

- Listar modelos de uma marca:

```bash
curl -s "https://parallelum.com.br/fipe/api/v1/carros/marcas/{codigoMarca}/modelos"
```

- Listar anos de um modelo:

```bash
curl -s "https://parallelum.com.br/fipe/api/v1/carros/marcas/{codigoMarca}/modelos/{codigoModelo}/anos"
```

- Obter preço/detalhes para um ano específico:

```bash
curl -s "https://parallelum.com.br/fipe/api/v1/carros/marcas/{codigoMarca}/modelos/{codigoModelo}/anos/{codigoAno}"
```

Notas:
- Para motos e caminhões, substitua `carros` por `motos` ou `caminhoes` na URL.
- Em Windows/PowerShell, `curl` pode ser um alias de `Invoke-WebRequest`. Se preferir, use `curl.exe` explicitamente.
- A resposta é JSON; para visualizar melhor, você pode usar utilitários como `jq` (Linux/macOS) ou formatadores online.

## Licença

Uso interno Finatech. Ajuste conforme a política da sua organização.