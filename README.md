```
GET     /api/v1/rendezvous/                                     - Lista pontos de encontro de integrações

PUT     /api/v1/rendezvous/<integration id>                     - Cria ponto de encontro de integração e o token de autorização para uso do mesmo
GET     /api/v1/rendezvous/<integration id>                     - Recupera ponto de encontro de integração
DELETE  /api/v1/rendezvous/<integration id>                     - Deleta ponto de encontro de integração

PUT     /api/v1/rendezvous/<integration id>/source/data         - Upload dados de origem
GET     /api/v1/rendezvous/<integration id>/source/data         - Recupera dados de origem
DELETE  /api/v1/rendezvous/<integration id>/source/data         - Deleta dados de origem

PUT     /api/v1/rendezvous/<integration id>/target/data         - Upload dados de destino
GET     /api/v1/rendezvous/<integration id>/target/data         - Recupera dados de destino
DELETE  /api/v1/rendezvous/<integration id>/target/data         - Deleta dados de destino

POST    /api/v1/rendezvous/<integration id>/reconcile           - inicia rodada de reconciliação
GET     /api/v1/rendezvous/<integration id>/reconcile           - recupera status da reconciliação
GET     /api/v1/rendezvous/<integration id>/reconcile/log       - recupera log da reconciliação
POST    /api/v1/rendezvous/<integration id>/reconcile/abort     - aborta reconciliação
GET     /api/v1/rendezvous/<integration id>/reconcile/insert    - recupera registros para inserção do resultado da reconciliação
GET     /api/v1/rendezvous/<integration id>/reconcile/update    - recupera registros para atualização do resultado da reconciliação
GET     /api/v1/rendezvous/<integration id>/reconcile/delete    - recupera registros para exclusão do resultado da reconciliação
GET     /api/v1/rendezvous/<integration id>/reconcile/equalized - recupera registros equalizados do resultado da reconciliação

GET     /api/v1/workers                                         - Lista os processos de reconciliação
```