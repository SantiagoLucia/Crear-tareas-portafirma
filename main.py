import httpx
import zeep
import asyncio
from zeep.transports import AsyncTransport
from tqdm.asyncio import tqdm
import configparser


config = configparser.ConfigParser()
config.read('config.ini')

USER = config['GDEBA']['USER_PRE3']
PASSW = config['GDEBA']['PASSW_PRE3']
TOKEN_URL = config['GDEBA']['TOKEN_URL_PRE3']
WSDL_URL = config['GDEBA']['WSDL_URL_PRE3']
CANTIDAD_TAREAS = int(config['GDEBA']['CANTIDAD_TAREAS'])
LIMITE_CONCURRENCIA = int(config['GDEBA']['LIMITE_CONCURRENCIA'])


async def get_token(user: str, passw: str) -> str:
    '''Obtiene el token de autenticación'''
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(TOKEN_URL, auth=(user, passw))
            response.raise_for_status()
            return response.text
        except httpx.HTTPStatusError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except Exception as err:
            print(f'Other error occurred: {err}')


class BearerAuth(httpx.Auth):
    '''Clase de autenticación Bearer'''
    def __init__(self, token):
        self.token = token

    def auth_flow(self, request):
        '''Agrega el token de autenticación a la cabecera de la petición'''
        request.headers['authorization'] = f'Bearer {self.token}'
        yield request

    def update_token(self, new_token):
        '''Actualiza el token de autenticación'''
        self.token = new_token


async def generar_tarea_firma(client: zeep.AsyncClient, sem: asyncio.Semaphore, auth: BearerAuth):
    '''Genera una tarea de firma en GDEBA'''
    async with sem:
        request = {
            'request': {
                'acronimoTipoDocumento': 'TESTL',
                'data': bytes('Documento de prueba. Carece de motivacion administrativa.', 'utf-8'),
                'tarea': 'Firmar Documento',
                'usuarioEmisor': 'USERT',
                'usuarioFirmante': {
                    'entry': {
                        'key': 1,
                        'value': 'USERT',
                    }                
                },
                'usuarioReceptor': 'USERT',
                'referencia': 'Documento de prueba. Carece de motivacion administrativa.',
                'suscribirseAlDocumento': True,
                'enviarCorreoReceptor': False,
                'listaUsuariosDestinatariosExternos': {
                    'entry': {
                        'key': '',
                        'value': '',
                    }
                },
                'metaDatos': {
                    'entry': {
                        'key': '',
                        'value': '',
                    }                
                },
                'recibirAvisoFirma': False
            }
        }
        try:
            await client.service.generarTareaGEDO(**request)

        # renuevo el token y vuelvo a intentar
        except:
            new_token = await get_token(USER, PASSW)
            auth.update_token(new_token)
            await client.service.generarTareaGEDO(**request)


async def main() -> None:
    '''Función principal asincrónica'''
    token = await get_token(USER, PASSW)
    auth = BearerAuth(token)

    async with httpx.AsyncClient(auth=auth) as httpx_client:
        async with zeep.AsyncClient(wsdl=WSDL_URL, transport=AsyncTransport(client=httpx_client)) as async_client:
            semaforo = asyncio.Semaphore(LIMITE_CONCURRENCIA)
            tareas = [generar_tarea_firma(async_client, semaforo, auth) for _ in range(CANTIDAD_TAREAS)]

            for tarea in tqdm.as_completed(tareas, desc='Tareas'):
                await tarea

if __name__ == '__main__':
    asyncio.run(main())
