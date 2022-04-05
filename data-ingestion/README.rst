=================
Ingestão de dados
=================

Esta etapa carrega os dados locais para o datalake. O caminho final escolhido é ``datalakeatividadefinal138979/assinaturas/data/``.

Observe que os parâmetros da conexão encontram-se no arquivo ``settings.toml``. Em aplicações de produção as credenciais deveriam estar no arquivo ``.secrets.toml``, que não é versionado.

Uso
===

O arquivo ``main.py`` contém a entrada para subir todos os arquivos pertinentes da pasta ``data/``. A função principal que faz o envio está no módulo ``data_ingestion/upload_data.py``.
