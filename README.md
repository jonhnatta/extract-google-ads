# Extract Google Ads

## Sobre
Projeto para extração de dados de campanhas do Google Ads. O script obtém informações das campanhas via API do Google e armazena os resultados em um banco de dados PostgreSQL na AWS.

## Execução
O projeto pode ser executado localmente na sua máquina ou no AWS Glue

### Local
```bash
poetry run python google_local.py
```
## AWS Glue
Upload do script google_glue.py para AWS Glue e configuração dos parâmetros necessários no job.

### Configuração
Crie um arquivo .env baseado no .env_exemple com suas credenciais:

```bash
DEVELOPER_TOKEN="seu_developer_token"
CLIENT_ID="seu_client_id"
CLIENT_SECRET="seu_client_secret"
REFRESH_TOKEN="seu_refresh_token"
LOGIN_CUSTOMER_ID="id_da_sua_mcc"
CUSTOMER_ID="id_da_conta_google"
DB_URL="jdbc:postgresql://seu_host:5432/sua_database"
DB_USER="usuário_banco"
DB_PASSWORD="senha_do_banco"
DB_TABLE="nome_schema.nome_tabela"